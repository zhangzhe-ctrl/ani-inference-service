package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	acceleratorv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	kube "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Only the external accelerator result is a fixture. Receiver admission,
// snapshots, ID/replay decisions, lifecycle guards and PostgreSQL are real.
func managedCreateFixture(t *testing.T, tenant, resource uuid.UUID) *inferencev1.CreateInferenceServiceRequest {
	t.Helper()
	const profileID = "10000000-0000-4000-8000-000000000001"
	request := &gpu.Request{ClusterID: profileID, PoolID: profileID, ProfileID: profileID, ProfileVersion: 1, Replicas: 1, DevicesPerReplica: 1, ContainerName: "kserve-container"}
	plan := &gpu.Plan{SchemaVersion: 1, Request: request,
		Profile:        &gpu.Profile{ProfileID: profileID, ProfileVersion: 1, DisplayName: "test", Spec: &gpu.ProfileSpec{GroupID: profileID, Mode: 2, ModelKey: "nvidia-test", SharedMemoryMiB: 6144, CoreLimitPercent: 25, MaxDevicesPerReplica: 1, IsolationClass: "SOFTWARE_COOPERATIVE"}, BaselineID: profileID, SpecDigest: "a10a605b164c85aec29cfa13c5e870759da9833825c8a9af089540fbc708add9", Published: true},
		Encoding:       &gpu.MemoryEncoding{MemoryBlockMiB: 1024, MemoryBlocksPerDevice: 6, SharedMemoryMiB: 6144, Policy: "EXACT"},
		Totals:         &gpu.ResourceTotals{LogicalDeviceCount: 1, SharedMemoryMiB: 6144},
		Runtime:        &gpu.RuntimeFragment{SchedulerName: "volcano", QueueName: "gpu", RecipeVersion: "volcano-hami-v1", NodeLabels: []gpu.KeyValue{{Key: "accelerator.ani.io/baseline-id", Value: profileID}, {Key: "accelerator.ani.io/model-key", Value: "nvidia-test"}, {Key: "accelerator.ani.io/supply-group", Value: profileID}}, PodAnnotations: []gpu.KeyValue{{Key: "volcano.sh/vgpu-mode", Value: "hami-core"}}, LimitsPerContainer: []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "25"}, {Key: "volcano.sh/vgpu-memory", Value: "6"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}},
		BaselineDigest: "52245607efe2656462e8aba6b9660b2e5eb9abb55bc381892952fd77ff71a70a",
	}
	digest, err := gpu.PlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.ResolutionDigest = digest
	var protoPlan acceleratorv1.ResolvedGpuPlan
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &protoPlan); err != nil {
		t.Fatal(err)
	}
	originalCharge := &integrationv1.GpuChargeRef{ChargeId: uuid.NewString(), QuotaCode: "gpu.shared_memory_mib", OriginalUnits: 6144}
	req := &inferencev1.CreateInferenceServiceRequest{RequestId: uuid.NewString(), Name: "managed-" + resource.String(), ModelVersionId: uuid.NewString(), Replicas: 1, Runtime: &inferencev1.RuntimeSpec{Mode: inferencev1.RuntimeMode_RUNTIME_MODE_DEPLOYMENT, Provider: "kserve"}, Engine: &inferencev1.EngineSpec{Type: "vllm", Image: "engine:v1", Command: []string{"engine"}, Args: []string{"serve", "/models"}}, Resource: &inferencev1.ResourceSpec{Gpu: gpuProto(request), Requests: map[string]string{"cpu": "1", "memory": "8Gi"}}, GpuOwnerAttachment: &integrationv1.GpuOwnerCreateAttachment{Ref: &acceleratorv1.GpuUsageRef{TenantId: tenant.String(), OwnerService: "ani-inference", ResourceId: resource.String(), CreateOperationId: uuid.NewString()}, Actor: &acceleratorv1.Actor{Type: "user", Id: "42"}, GpuPlan: &protoPlan, GpuCharges: []*integrationv1.GpuChargeRef{originalCharge}, MeteringVersion: "gpu-metering-v1"}, OriginalCharges: []*inferencev1.OriginalQuotaCharge{{ChargeId: uuid.NewString(), QuotaCode: "cpu.milli", OriginalUnits: 1000}, {ChargeId: originalCharge.ChargeId, QuotaCode: originalCharge.QuotaCode, OriginalUnits: originalCharge.OriginalUnits}}}
	refreshManagedFixtureDigests(t, req)
	return req
}

func refreshManagedFixtureDigests(t *testing.T, req *inferencev1.CreateInferenceServiceRequest) {
	t.Helper()
	var err error
	req.GpuOwnerAttachment.BusinessPayloadDigest, err = inferencev1.BusinessPayloadDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	req.GpuOwnerAttachment.RequestHash, err = inferencev1.ManagedCreateRequestHash(req, req.GpuOwnerAttachment.Ref.TenantId, req.GpuOwnerAttachment.Actor.Type, req.GpuOwnerAttachment.Actor.Id)
	if err != nil {
		t.Fatal(err)
	}
}

func managedDeleteFixture(t *testing.T, req *inferencev1.CreateInferenceServiceRequest) *inferencev1.DeleteInferenceServiceRequest {
	t.Helper()
	a := req.GetGpuOwnerAttachment()
	out := &inferencev1.DeleteInferenceServiceRequest{RequestId: uuid.NewString(), ResourceId: a.Ref.ResourceId, GpuOwnerAttachment: &integrationv1.GpuOwnerDeleteAttachment{DeleteOperationId: uuid.NewString(), Ref: proto.Clone(a.Ref).(*acceleratorv1.GpuUsageRef), Actor: proto.Clone(a.Actor).(*acceleratorv1.Actor), OriginalGpuCharges: a.GpuCharges, OriginalGpuPlan: a.GpuPlan, MeteringVersion: a.MeteringVersion}, OriginalCharges: req.OriginalCharges}
	hash, err := inferencev1.ManagedDeleteRequestHash(a.Ref.TenantId, a.Ref.ResourceId, a.Ref.CreateOperationId, a.Actor.Type, a.Actor.Id)
	if err != nil {
		t.Fatal(err)
	}
	out.GpuOwnerAttachment.RequestHash = hash
	return out
}

func TestPostgresManagedGPUCreateAtomicReplayAndOriginalContext(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedCreateFixture(t, tenant, resource)
	repo := NewRepository(pool)
	server := service.NewInferenceServerWithAll(NewCreateUseCase(repo), nil, NewCommandUseCase(repo), NewUpdateUseCase(repo))
	out, err := server.CreateInferenceService(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if out.GetResource().GetId() != resource.String() || out.GetOperation().GetId() != req.GpuOwnerAttachment.Ref.CreateOperationId || !out.GetDurableOwnerAck().GetAccepted() {
		t.Fatalf("Gov identities/ACK changed: %v", out)
	}
	// Reconstruct both server and repository to prove the response is from PG.
	reordered := proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
	reordered.RequestId = uuid.NewString()
	reordered.OriginalCharges[0], reordered.OriginalCharges[1] = reordered.OriginalCharges[1], reordered.OriginalCharges[0]
	restored := service.NewInferenceServer(NewCreateUseCase(NewRepository(pool)))
	replay, err := restored.CreateInferenceService(ctx, reordered)
	if err != nil || !proto.Equal(out, replay) {
		t.Fatalf("restart/reordered replay=%v error=%v", replay, err)
	}
	saved, err := repo.LoadRefundContext(ctx, tenant.String(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId)
	if err != nil || saved.Plan.ResolutionDigest != req.GpuOwnerAttachment.GpuPlan.ResolutionDigest || len(saved.Charges) != 2 || len(saved.GPUCharges) != 1 || saved.DeleteOperationID != "" {
		t.Fatalf("lost original context: %+v error=%v", saved, err)
	}
	row, err := New(pool).GetManagedGPUResource(ctx, GetManagedGPUResourceParams{TenantID: pgUUID(tenant), ResourceID: pgUUID(resource)})
	if err != nil || len(row.CreatePayload) == 0 || len(row.BusinessPayload) == 0 || row.BusinessPayloadDigest != req.GpuOwnerAttachment.BusinessPayloadDigest {
		t.Fatalf("incomplete command transaction: %+v error=%v", row, err)
	}
	var counts struct{ services, specs, operations, commands, work, reservations int }
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM inference_services WHERE tenant_id=$1),(SELECT count(*) FROM inference_specs WHERE tenant_id=$1),(SELECT count(*) FROM inference_operations WHERE tenant_id=$1),(SELECT count(*) FROM inference_managed_gpu_commands WHERE tenant_id=$1),(SELECT count(*) FROM inference_resource_work WHERE tenant_id=$1),(SELECT count(*) FROM inference_quota_reservations WHERE tenant_id=$1)`, tenant).Scan(&counts.services, &counts.specs, &counts.operations, &counts.commands, &counts.work, &counts.reservations); err != nil {
		t.Fatal(err)
	}
	if counts.services != 1 || counts.specs != 1 || counts.operations != 1 || counts.commands != 1 || counts.work != 1 || counts.reservations != 0 {
		t.Fatalf("atomic facts/legacy reservation wrong: %+v", counts)
	}
	changed := proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
	changed.Name = "different"
	refreshManagedFixtureDigests(t, changed)
	if _, err := server.CreateInferenceService(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("same command changed content accepted: %v", err)
	}
	// The immutable ref can never be replaced by a later CREATE operation.
	changed = proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
	changed.GpuOwnerAttachment.Ref.CreateOperationId = uuid.NewString()
	if _, err := server.CreateInferenceService(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("resource resurrected under new CREATE: %v", err)
	}
}

func TestPostgresManagedGPUConcurrentCommandHasOneACK(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedCreateFixture(t, tenant, resource)
	server := service.NewInferenceServer(NewCreateUseCase(NewRepository(pool)))
	var wait sync.WaitGroup
	results := make(chan *inferencev1.OperationResponse, 8)
	errors := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			out, err := server.CreateInferenceService(ctx, proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest))
			results <- out
			errors <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var original *inferencev1.OperationResponse
	for result := range results {
		if original == nil {
			original = result
		}
		if !proto.Equal(original, result) {
			t.Fatalf("concurrent ACK changed: %v versus %v", result, original)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inference_managed_gpu_commands WHERE tenant_id=$1`, tenant).Scan(&count); err != nil || count != 1 {
		t.Fatalf("command count=%d error=%v", count, err)
	}
}

func TestPostgresManagedGPUFrozenSnapshotProjectsSharedAndWholeReplicas(t *testing.T) {
	for _, whole := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared_two_replicas_12288MiB", true: "whole_two_devices"}[whole], func(t *testing.T) {
			ctx, pool, tenant, resource := lifecycleDatabase(t)
			ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
			req := managedCreateFixture(t, tenant, resource)
			var frozen gpu.Plan
			raw, _ := json.Marshal(req.GpuOwnerAttachment.GpuPlan)
			if err := json.Unmarshal(raw, &frozen); err != nil {
				t.Fatal(err)
			}
			req.Replicas, req.Resource.Gpu.Replicas, frozen.Request.Replicas = 2, 2, 2
			frozen.Totals.LogicalDeviceCount, frozen.Totals.SharedMemoryMiB = 2, 12288
			charge := req.GpuOwnerAttachment.GpuCharges[0]
			charge.OriginalUnits = 12288
			if whole {
				frozen.Profile.Spec.Mode, frozen.Profile.Spec.SharedMemoryMiB, frozen.Profile.Spec.CoreLimitPercent, frozen.Profile.Spec.IsolationClass = 1, 0, 100, "WHOLE_DEVICE_EXCLUSIVE"
				frozen.Profile.SpecDigest = fixtureSpecDigest(`{"baseline_id":"10000000-0000-4000-8000-000000000001","spec":{"core_limit_percent":"100","group_id":"10000000-0000-4000-8000-000000000001","isolation_class":"WHOLE_DEVICE_EXCLUSIVE","max_devices_per_replica":"1","mode":"1","model_key":"nvidia-test","shared_memory_mib":"0"}}`)
				frozen.Encoding.MemoryBlocksPerDevice, frozen.Encoding.SharedMemoryMiB, frozen.Encoding.MemoryPercentage = 0, 0, 100
				frozen.Totals = &gpu.ResourceTotals{ExclusiveDeviceCount: 2}
				frozen.Runtime.LimitsPerContainer = []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "100"}, {Key: "volcano.sh/vgpu-memory-percentage", Value: "100"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}
				charge.QuotaCode, charge.OriginalUnits = "gpu.physical.count", 2
			}
			req.OriginalCharges[1].QuotaCode, req.OriginalCharges[1].OriginalUnits = charge.QuotaCode, charge.OriginalUnits
			var err error
			frozen.ResolutionDigest, err = gpu.PlanDigest(&frozen)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ = json.Marshal(&frozen)
			if err := json.Unmarshal(raw, req.GpuOwnerAttachment.GpuPlan); err != nil {
				t.Fatal(err)
			}
			refreshManagedFixtureDigests(t, req)
			server := service.NewInferenceServer(NewCreateUseCase(NewRepository(pool)))
			if _, err := server.CreateInferenceService(ctx, req); err != nil {
				t.Fatal(err)
			}
			// Reconstruct the runtime from actual accepted PG rows. No GPU or
			// business fixture replaces this transformation/rendering chain.
			loaded, err := NewRuntimeSource(pool, "models").CurrentRuntime(ctx, tenant.String(), resource.String(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.ManagedGPU || loaded.GPUPlan.ResolutionDigest != frozen.ResolutionDigest || loaded.Resources.GPU.Replicas != 2 || loaded.Replicas != 2 {
				t.Fatalf("frozen runtime changed: %+v", loaded.RuntimeSpec)
			}
			obj, err := kube.RenderKServeRuntime(loaded.RuntimeSpec)
			if err != nil {
				t.Fatal(err)
			}
			predictor, _, _ := unstructured.NestedMap(obj.Object, "spec", "predictor")
			if predictor["schedulerName"] != "volcano" || predictor["minReplicas"] != int64(2) || predictor["maxReplicas"] != int64(2) {
				t.Fatalf("fixed replica/scheduler projection lost: %v", predictor)
			}
			annotations, _, _ := unstructured.NestedStringMap(predictor, "annotations")
			if annotations["scheduling.volcano.sh/queue-name"] != frozen.Runtime.QueueName || annotations["volcano.sh/vgpu-mode"] != "hami-core" {
				t.Fatalf("queue or HAMI annotation lost: %v", annotations)
			}
			containers, _, _ := unstructured.NestedSlice(predictor, "containers")
			container := containers[0].(map[string]interface{})
			if container["name"] != "kserve-container" {
				t.Fatalf("wrong frozen container: %v", container)
			}
			limits, _, _ := unstructured.NestedStringMap(container, "resources", "limits")
			for _, item := range frozen.Runtime.LimitsPerContainer {
				if limits[item.Key] != item.Value {
					t.Fatalf("runtime limits lost: %v", limits)
				}
			}
			argv, _, _ := unstructured.NestedStringSlice(container, "command")
			if strings.Join(argv, " ") != "engine serve /models" {
				t.Fatalf("caller engine argv changed: %v", argv)
			}
		})
	}
}

func TestPostgresManagedGPUDeleteFirstClosesLateCreate(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedCreateFixture(t, tenant, resource)
	repo := NewRepository(pool)
	server := service.NewInferenceServerWithCommands(NewCreateUseCase(repo), nil, NewCommandUseCase(repo))
	deletion := managedDeleteFixture(t, req)
	accepted, err := server.DeleteInferenceService(ctx, deletion)
	if err != nil || !accepted.GetDurableOwnerAck().GetAccepted() || accepted.GetDurableOwnerAck().GetOperationId() != deletion.GpuOwnerAttachment.DeleteOperationId {
		t.Fatalf("DELETE first=%v error=%v", accepted, err)
	}
	reordered := proto.Clone(deletion).(*inferencev1.DeleteInferenceServiceRequest)
	reordered.RequestId, reordered.ExpectedGeneration = uuid.NewString(), 99
	reordered.OriginalCharges[0], reordered.OriginalCharges[1] = reordered.OriginalCharges[1], reordered.OriginalCharges[0]
	replay, err := server.DeleteInferenceService(ctx, reordered)
	if err != nil || !proto.Equal(accepted, replay) {
		t.Fatalf("DELETE replay=%v error=%v", replay, err)
	}
	changedDelete := proto.Clone(deletion).(*inferencev1.DeleteInferenceServiceRequest)
	changedDelete.GpuOwnerAttachment.DeleteOperationId = uuid.NewString()
	if _, err := server.DeleteInferenceService(ctx, changedDelete); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("original DELETE association changed: %v", err)
	}
	late, err := server.CreateInferenceService(ctx, req)
	if err != nil || !late.GetDurableOwnerAck().GetAccepted() || late.GetOperation().GetStep() != "blocked_by_delete" {
		t.Fatalf("late create=%v error=%v", late, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM inference_services WHERE tenant_id=$1)+(SELECT count(*) FROM inference_resource_work WHERE tenant_id=$1)+(SELECT count(*) FROM inference_operations WHERE tenant_id=$1)`, tenant).Scan(&count); err != nil || count != 0 {
		t.Fatalf("late CREATE executed after DELETE intent: count=%d error=%v", count, err)
	}
	context, err := repo.LoadRefundContext(ctx, tenant.String(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId)
	if err != nil || context.DeleteOperationID != deletion.GpuOwnerAttachment.DeleteOperationId || len(context.Charges) != 2 {
		t.Fatalf("DELETE first lost original context: %+v error=%v", context, err)
	}
}

func TestPostgresManagedGPURejectsForgedIdentityDigestAndCharge(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	req := managedCreateFixture(t, tenant, resource)
	server := service.NewInferenceServer(NewCreateUseCase(NewRepository(pool)))
	scoped := service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	tests := []struct {
		name    string
		context context.Context
		mutate  func(*inferencev1.CreateInferenceServiceRequest)
	}{
		{"untrusted caller", service.WithTenantID(ctx, tenant.String()), func(*inferencev1.CreateInferenceServiceRequest) {}},
		{"cross tenant", service.WithTenantID(gpu.WithGovernanceIdentity(ctx), uuid.NewString()), func(*inferencev1.CreateInferenceServiceRequest) {}},
		{"wrong owner", scoped, func(r *inferencev1.CreateInferenceServiceRequest) { r.GpuOwnerAttachment.Ref.OwnerService = "other" }},
		{"business digest", scoped, func(r *inferencev1.CreateInferenceServiceRequest) { r.Engine.Args = []string{"tampered"} }},
		{"request digest", scoped, func(r *inferencev1.CreateInferenceServiceRequest) {
			r.GpuOwnerAttachment.RequestHash = strings.Repeat("0", 64)
		}},
		{"wrong original units", scoped, func(r *inferencev1.CreateInferenceServiceRequest) {
			r.GpuOwnerAttachment.GpuCharges[0].OriginalUnits = 6
		}},
		{"incomplete full charges", scoped, func(r *inferencev1.CreateInferenceServiceRequest) { r.OriginalCharges = r.OriginalCharges[:1] }},
		{"missing plan", scoped, func(r *inferencev1.CreateInferenceServiceRequest) { r.GpuOwnerAttachment.GpuPlan = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
			test.mutate(bad)
			if _, err := server.CreateInferenceService(test.context, bad); err == nil {
				t.Fatal("forged command accepted")
			}
		})
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inference_managed_gpu_commands WHERE tenant_id=$1`, tenant).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected command persisted: count=%d error=%v", count, err)
	}
	if _, err := NewRepository(pool).LoadRefundContext(ctx, uuid.NewString(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId); err != pgx.ErrNoRows {
		t.Fatalf("cross tenant lookup=%v", err)
	}
}

// Freeze-digest fixture helper for future whole/LWS parameter cases. The
// canonical fixed spec body is deliberately independent of business logic.
func fixtureSpecDigest(canonical string) string {
	sum := sha256.Sum256(append([]byte("acc-c14n-v1\n"), []byte(canonical)...))
	return hex.EncodeToString(sum[:])
}
