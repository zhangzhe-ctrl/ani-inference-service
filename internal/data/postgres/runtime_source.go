package postgres

// This adapter exposes the immutable desired generation to the Kubernetes
// executor. It intentionally reads only Inference-owned tables; model and
// artifact metadata remain external references in inference_specs.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	kube "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RuntimeSource reads desired state from PostgreSQL for one configured
// Kubernetes namespace. Namespace is deployment configuration, not a mutable
// business field supplied by a request.
type RuntimeSource struct {
	Pool      DBTX
	Namespace string
}

func NewRuntimeSource(pool DBTX, namespace string) *RuntimeSource {
	return &RuntimeSource{Pool: pool, Namespace: namespace}
}

var _ kube.DesiredRuntimeSource = (*RuntimeSource)(nil)

func (s *RuntimeSource) CurrentRuntime(ctx context.Context, tenantID, serviceID string, generation int64) (kube.DesiredRuntime, error) {
	if s == nil || s.Pool == nil {
		return kube.DesiredRuntime{}, errors.New("nil postgres runtime source")
	}
	if s.Namespace == "" {
		return kube.DesiredRuntime{}, errors.New("runtime source namespace is required")
	}
	if generation < 1 {
		return kube.DesiredRuntime{}, errors.New("runtime source generation must be positive")
	}
	tenant, err := tenantUUID(tenantID)
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	service, err := workUUID("service_id", serviceID)
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	q := New(s.Pool)
	managed, err := q.GetManagedGPUState(ctx, GetManagedGPUStateParams{TenantID: tenant, ResourceID: service})
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	aggregate, err := q.GetService(ctx, GetServiceParams{TenantID: tenant, ID: service})
	if errors.Is(err, pgx.ErrNoRows) {
		return kube.DesiredRuntime{}, fmt.Errorf("inference service %s/%s not found", tenantID, serviceID)
	}
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	if aggregate.DesiredGeneration != generation {
		return kube.DesiredRuntime{}, fmt.Errorf("desired generation %d does not match requested %d", aggregate.DesiredGeneration, generation)
	}
	row, err := q.GetSpec(ctx, GetSpecParams{TenantID: tenant, ServiceID: service, Generation: generation})
	if errors.Is(err, pgx.ErrNoRows) {
		if aggregate.DesiredState != "deleted" {
			return kube.DesiredRuntime{}, fmt.Errorf("inference spec generation %d not found", generation)
		}
		row, err = q.GetLatestSpec(ctx, GetLatestSpecParams{TenantID: tenant, ServiceID: service, Generation: generation})
		if errors.Is(err, pgx.ErrNoRows) {
			return kube.DesiredRuntime{}, fmt.Errorf("inference spec generation <= %d not found", generation)
		}
	}
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	var command []string
	if len(row.CommandArgv) != 0 {
		if err := json.Unmarshal(row.CommandArgv, &command); err != nil {
			return kube.DesiredRuntime{}, fmt.Errorf("decode command argv: %w", err)
		}
	}
	var resourceSpec resources.Spec
	if len(row.Resources) != 0 {
		if err := json.Unmarshal(row.Resources, &resourceSpec); err != nil {
			return kube.DesiredRuntime{}, fmt.Errorf("decode resources: %w", err)
		}
	}
	if resourceSpec.GPU == nil && len(row.GpuRequest) != 0 {
		if err := json.Unmarshal(row.GpuRequest, &resourceSpec.GPU); err != nil {
			return kube.DesiredRuntime{}, fmt.Errorf("decode gpu request: %w", err)
		}
	}
	normalized, err := resources.Normalize(resources.Spec{Requests: resourceSpec.Requests, Limits: resourceSpec.Limits, GPU: resourceSpec.GPU})
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	var gpuPlan *gpu.Plan
	if len(row.GpuPlan) > 0 {
		gpuPlan = new(gpu.Plan)
		if err := json.Unmarshal(row.GpuPlan, gpuPlan); err != nil {
			return kube.DesiredRuntime{}, fmt.Errorf("decode gpu plan: %w", err)
		}
		if row.GpuPlanDigest.Valid && gpuPlan.ResolutionDigest != row.GpuPlanDigest.String {
			return kube.DesiredRuntime{}, fmt.Errorf("GPU plan digest does not match persisted digest")
		}
		if resourceSpec.GPU == nil {
			return kube.DesiredRuntime{}, fmt.Errorf("GPU plan has no GPU request")
		}
		if err := gpu.ValidatePlan(gpuPlan, resourceSpec.GPU); err != nil {
			return kube.DesiredRuntime{}, err
		}
	}
	if managed.Managed {
		if err := gpu.ValidateManagedPlan(gpuPlan, resourceSpec.GPU); err != nil {
			return kube.DesiredRuntime{}, fmt.Errorf("invalid accepted Governance GPU snapshot: %w", err)
		}
	}
	// Resolution is a durable worker step before ApplyCR. Admission and
	// deletion still need to read a generation while that step is pending, so
	// an absent plan is not a read error here. The Kubernetes adapter remains
	// fail-closed and rejects applying a GPU generation without its plan.
	workers := row.WorkerReplicas
	if workers == 0 {
		workers = 1
	}
	mode := row.RuntimeMode
	if mode == "" {
		mode = "deployment"
	}
	provider := row.RuntimeProvider
	if provider == "" {
		provider = "deployment"
	}
	var endpoint *kube.EndpointSpec
	if row.EndpointContainerPort.Valid || row.EndpointServicePort.Valid || row.EndpointTargetPort.Valid || row.EndpointProtocol.Valid {
		target := intstr.FromString(row.EndpointTargetPort.String)
		if n, parseErr := strconv.Atoi(row.EndpointTargetPort.String); parseErr == nil {
			target = intstr.FromInt(n)
		}
		endpoint = &kube.EndpointSpec{ContainerPort: row.EndpointContainerPort.Int32, ServicePort: row.EndpointServicePort.Int32, TargetPort: target, Protocol: corev1.Protocol(row.EndpointProtocol.String)}
	}
	bindings, err := q.ListCurrentRuntimeBindings(ctx, ListCurrentRuntimeBindingsParams{TenantID: tenant, ServiceID: service, Generation: generation})
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	// A replacement operation deletes the previous generation before it can
	// apply the target generation. Until that apply records new bindings, the
	// target generation has no rows; retain the previous bindings as the fence
	// for the delete/absence steps. Once target bindings exist, they are the
	// authoritative set.
	if len(bindings) == 0 && generation > 1 {
		bindings, err = q.ListCurrentRuntimeBindings(ctx, ListCurrentRuntimeBindingsParams{TenantID: tenant, ServiceID: service, Generation: generation - 1})
		if err != nil {
			return kube.DesiredRuntime{}, err
		}
	}
	runtimeRow, err := q.GetRuntime(ctx, GetRuntimeParams{TenantID: tenant, ServiceID: service})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return kube.DesiredRuntime{}, err
	}
	currentRuntimePhase := "unknown"
	if err == nil {
		currentRuntimePhase = runtimeRow.RuntimePhase
	}
	quotaReserved := false
	if reservation, reservationErr := q.GetActiveQuotaReservation(ctx, GetActiveQuotaReservationParams{TenantID: tenant, ServiceID: service, Generation: generation}); reservationErr == nil {
		quotaReserved = reservation.State == "confirmed"
	} else if !errors.Is(reservationErr, pgx.ErrNoRows) {
		return kube.DesiredRuntime{}, reservationErr
	}
	publicationWithdrawn := true
	publicationPublished := false
	if publication, publicationErr := q.GetLatestPublication(ctx, GetLatestPublicationParams{TenantID: tenant, ServiceID: service, Generation: generation}); publicationErr == nil {
		publicationWithdrawn = publication.ObservedPhase == "withdrawn" && publication.DesiredPhase == "withdrawn"
		publicationPublished = publication.Generation == generation && publication.ObservedPhase == "published" && publication.DesiredPhase == "published"
	} else if !errors.Is(publicationErr, pgx.ErrNoRows) {
		return kube.DesiredRuntime{}, publicationErr
	}
	owned := make([]kube.RuntimeBinding, 0, len(bindings))
	for _, binding := range bindings {
		owned = append(owned, kube.RuntimeBinding{Kind: binding.ObjectKind, Namespace: binding.ObjectNamespace, Name: binding.ObjectName, UID: binding.ObjectUid, ResourceVersion: binding.ResourceVersion, Role: binding.Role, Generation: binding.Generation})
	}
	result := kube.DesiredRuntime{
		RuntimeSpec: kube.RuntimeSpec{
			TenantID: tenantID, ServiceID: serviceID, Name: aggregate.Name, RuntimeProvider: provider,
			Namespace: s.Namespace, Image: row.ImageRef, ModelVersionID: row.ModelVersionID.String(), ArtifactProvider: row.ArtifactProvider, ArtifactRef: row.ArtifactRef, ArtifactSHA256: row.ArtifactSha256, ServedModelName: row.ServedModelName, EngineRuntime: row.EngineRuntime, Generation: generation,
			CommandArgv: command, Resources: normalized, Replicas: row.Replicas,
			WorkerReplicas: workers, RuntimeMode: mode,
			GPUPlan: gpuPlan, ManagedGPU: managed.Managed,
			Endpoint: endpoint,
		},
		Bindings:        owned,
		DesiredState:    aggregate.DesiredState,
		RuntimePhase:    currentRuntimePhase,
		ModelReady:      runtimeRow.ModelReady,
		ModelReadyKnown: runtimeRow.ModelReadyKnown,
		QuotaReserved:   quotaReserved, PublicationWithdrawn: publicationWithdrawn,
		PublicationPublished: publicationPublished,
	}
	if row.ArtifactProvider == "model" {
		result.ModelClaim = kube.ModelClaimName(result.RuntimeSpec)
	}
	return result, nil
}
