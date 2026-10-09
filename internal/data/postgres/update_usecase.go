package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	gpubiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
)

type UpdateUseCase struct{ repo *Repository }

func endpointValues(e *inferencebiz.EndpointSpec) (pgtype.Int4, pgtype.Int4, pgtype.Text, pgtype.Text) {
	if e == nil {
		return pgtype.Int4{}, pgtype.Int4{}, pgtype.Text{}, pgtype.Text{}
	}
	return pgtype.Int4{Int32: e.ContainerPort, Valid: true}, pgtype.Int4{Int32: e.ServicePort, Valid: true}, pgtype.Text{String: e.TargetPort, Valid: true}, pgtype.Text{String: e.Protocol, Valid: true}
}

func NewUpdateUseCase(repo *Repository) *UpdateUseCase { return &UpdateUseCase{repo: repo} }

func (u *UpdateUseCase) Update(ctx context.Context, in inferencebiz.UpdateInput) (*inferencev1.OperationResponse, error) {
	if u == nil || u.repo == nil || u.repo.pool == nil {
		return nil, errors.New("nil postgres update repository")
	}
	tenant, err := parseUUID("tenant_id", in.TenantID, false)
	if err != nil {
		return nil, err
	}
	gpuPlanJSON, err := json.Marshal(in.GPUPlan)
	if in.GPUPlan == nil {
		gpuPlanJSON = nil
	}
	if err != nil {
		return nil, err
	}
	gpuPlanDigest := in.GPUPlanDigest
	if gpuPlanDigest == "" && in.GPUPlan != nil {
		gpuPlanDigest = in.GPUPlan.ResolutionDigest
	}
	commandJSON, err := json.Marshal(in.CommandArgv)
	if err != nil {
		return nil, err
	}
	serviceID, err := parseUUID("service_id", in.ServiceID, false)
	if err != nil {
		return nil, err
	}
	modelVersionID, err := parseUUID("model_version_id", in.ModelVersionID, true)
	if err != nil {
		return nil, err
	}
	if in.ExpectedGeneration < 1 {
		return nil, inferencebiz.ErrGenerationConflict
	}
	resourcesJSON, err := json.Marshal(struct {
		Requests map[string]string `json:"requests"`
		Limits   map[string]string `json:"limits"`
		GPU      *gpubiz.Request   `json:"gpu,omitempty"`
	}{in.Resources.Requests, in.Resources.Limits, in.Resources.GPU})
	if err != nil {
		return nil, err
	}
	gpuRequestJSON, err := json.Marshal(in.Resources.GPU)
	if in.Resources.GPU == nil {
		gpuRequestJSON = nil
	}
	if err != nil {
		return nil, err
	}
	opID := pgUUID(uuid.New())
	tx, err := u.repo.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	q := New(tx)
	method := "InferenceService/update"
	if in.RequestID != "" {
		if err := q.LockIdempotency(ctx, LockIdempotencyParams{TenantID: in.TenantID, Method: method, IdempotencyKey: in.RequestID}); err != nil {
			return nil, err
		}
		existing, e := q.GetIdempotency(ctx, GetIdempotencyParams{TenantID: tenant, Method: method, IdempotencyKey: in.RequestID})
		if e == nil {
			if existing.PayloadHash != in.RequestHash {
				return nil, inferencebiz.ErrIdempotencyConflict
			}
			op, e := q.GetOperation(ctx, GetOperationParams{TenantID: tenant, ID: existing.OperationID})
			if e != nil {
				return nil, e
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return commandResponseFromRow(op), nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return nil, e
		}
	}
	current, err := q.GetServiceForUpdate(ctx, GetServiceForUpdateParams{TenantID: tenant, ID: serviceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, inferencebiz.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if current.DesiredGeneration != in.ExpectedGeneration || current.DeletedAt.Valid {
		return nil, inferencebiz.ErrGenerationConflict
	}
	// A stopped service has no runtime operation to replace. Refusing this
	// mixed command keeps update from accidentally reserving quota and
	// publishing a service whose desired state is stopped; callers can update
	// after Start, or use a future stopped-spec-only operation explicitly.
	if current.DesiredState != "running" {
		return nil, inferencebiz.ErrInvalidState
	}
	managedResource, managedErr := q.GetManagedGPUResource(ctx, GetManagedGPUResourceParams{TenantID: tenant, ResourceID: serviceID})
	managed := managedErr == nil
	if managedErr != nil && !errors.Is(managedErr, pgx.ErrNoRows) {
		return nil, managedErr
	}
	if managed {
		if managedResource.Closing {
			return nil, fmt.Errorf("%w: managed GPU resource has a closing intent", inferencebiz.ErrInvalidState)
		}
		var original gpubiz.RefundContext
		if err := json.Unmarshal(managedResource.OriginalContext, &original); err != nil {
			return nil, err
		}
		if original.Plan == nil || in.Resources.GPU == nil || *original.Plan.Request != *in.Resources.GPU {
			return nil, fmt.Errorf("%w: managed GPU selection changes require Governance admission", inferencebiz.ErrInvalidState)
		}
		if err := gpubiz.ValidateTopology(original.Plan.Request, in.Replicas, in.RuntimeMode, in.WorkerReplicas); err != nil {
			return nil, fmt.Errorf("%w: %s", inferencebiz.ErrInvalidState, err)
		}
		currentSpec, err := q.GetSpec(ctx, GetSpecParams{TenantID: tenant, ServiceID: serviceID, Generation: current.DesiredGeneration})
		if err != nil {
			return nil, err
		}
		if currentSpec.Replicas != in.Replicas || currentSpec.RuntimeMode != in.RuntimeMode || currentSpec.WorkerReplicas != in.WorkerReplicas {
			return nil, fmt.Errorf("%w: managed GPU topology changes require Governance admission", inferencebiz.ErrInvalidState)
		}
		gpuPlanJSON, err = json.Marshal(original.Plan)
		if err != nil {
			return nil, err
		}
		gpuPlanDigest = original.Plan.ResolutionDigest
	}
	// The nested resource.gpu object is the complete accelerator intent for a
	// generation. An omitted object therefore means that this desired
	// generation has no GPU placement; never inherit a previous generation's
	// request or resolved plan. This also prevents a CPU-only update from
	// accidentally recreating stale GPU workloads.
	target := current.DesiredGeneration + 1
	workers := in.WorkerReplicas
	if workers < 1 {
		workers = 1
	}
	mode := in.RuntimeMode
	if mode == "" {
		mode = "deployment"
	}
	provider := in.RuntimeProvider
	ec, es, et, ep := endpointValues(in.Endpoint)
	rows, err := q.CloneSpecWithRuntimeUpdate(ctx, CloneSpecWithRuntimeUpdateParams{ID: pgUUID(uuid.New()), TargetGeneration: target, ModelVersionID: modelVersionID, Resources: resourcesJSON, Replicas: in.Replicas, RuntimeMode: mode, WorkerReplicas: workers, RuntimeProvider: provider, ArtifactProvider: in.ArtifactProvider, ArtifactRef: in.ArtifactRef, ArtifactSha256: in.ArtifactSHA256, ImageRef: in.ImageRef, ServedModelName: in.ServedModelName, EngineRuntime: in.EngineRuntime, CommandArgv: commandJSON, EndpointContainerPort: ec, EndpointServicePort: es, EndpointTargetPort: et, EndpointProtocol: ep, GpuRequest: gpuRequestJSON, GpuPlan: gpuPlanJSON, GpuPlanDigest: pgtype.Text{String: gpuPlanDigest, Valid: gpuPlanDigest != ""}, TenantID: tenant, ServiceID: serviceID, SourceGeneration: current.DesiredGeneration})
	if err != nil {
		return nil, err
	}
	if rows != 1 {
		return nil, fmt.Errorf("%w: missing current specification", inferencebiz.ErrInvalidState)
	}
	rows, err = q.TransitionService(ctx, TransitionServiceParams{DesiredState: current.DesiredState, TargetGeneration: target, OperationID: opID, TenantID: tenant, ServiceID: serviceID, ExpectedGeneration: current.DesiredGeneration})
	if err != nil {
		return nil, err
	}
	if rows != 1 {
		return nil, inferencebiz.ErrGenerationConflict
	}
	if err := q.InsertOperation(ctx, InsertOperationParams{TenantID: tenant, ID: opID, ServiceID: serviceID, Kind: "update", Phase: "pending", Step: "withdraw_publication", TargetGeneration: target, RequestHash: in.RequestHash}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, inferencebiz.ErrOperationActive
		}
		return nil, err
	}
	if !managed {
		if err := q.InsertQuotaReservation(ctx, InsertQuotaReservationParams{TenantID: tenant, ServiceID: serviceID, OperationID: opID, Generation: target, ReservationID: "pending-" + opID.String(), RequestedResources: resourcesJSON}); err != nil {
			return nil, err
		}
	}
	if err := q.UpsertResourceWork(ctx, UpsertResourceWorkParams{TenantID: tenant, ServiceID: serviceID, DirtyVersion: target}); err != nil {
		return nil, err
	}
	if in.RequestID != "" {
		if _, err := q.PutIdempotency(ctx, PutIdempotencyParams{TenantID: tenant, Method: method, IdempotencyKey: in.RequestID, PayloadHash: in.RequestHash, OperationID: opID}); err != nil {
			return nil, err
		}
	}
	payload, _ := json.Marshal(map[string]any{"kind": "update", "desired_state": current.DesiredState})
	if err := q.AppendAuditEvent(ctx, AppendAuditEventParams{TenantID: tenant, EventID: pgUUID(uuid.New()), ServiceID: serviceID, OperationID: opID, Generation: target, EventType: "inference.update.accepted", Actor: in.Actor, RequestID: in.RequestID, Payload: payload}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return commandResponse(serviceID.String(), opID.String(), target, "update", "withdraw_publication"), nil
}
