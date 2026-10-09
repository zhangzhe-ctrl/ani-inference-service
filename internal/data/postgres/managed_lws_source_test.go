package postgres

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	kube "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
)

func managedLWSCreateFixture(t *testing.T, tenant, resource uuid.UUID) *inferencev1.CreateInferenceServiceRequest {
	t.Helper()
	req := managedCreateFixture(t, tenant, resource)
	req.Name = "lws-" + resource.String()[:8]
	// The external model provider is a fixture; runtime/claim conversion remains real.
	req.ModelArtifact = &inferencev1.ModelArtifact{Provider: "model", Reference: "external-model-artifact-fixture"}
	var plan gpu.Plan
	raw, _ := json.Marshal(req.GpuOwnerAttachment.GpuPlan)
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	req.Replicas, req.Runtime.Mode, req.Runtime.WorkerReplicas = 2, inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET, 1
	req.Resource.Gpu.Replicas, req.Resource.Gpu.ContainerName = 4, "main"
	plan.Request.Replicas, plan.Request.ContainerName = 4, "main"
	plan.Totals.LogicalDeviceCount, plan.Totals.SharedMemoryMiB = 4, 24576
	req.GpuOwnerAttachment.GpuCharges[0].OriginalUnits, req.OriginalCharges[1].OriginalUnits = 24576, 24576
	var err error
	plan.ResolutionDigest, err = gpu.PlanDigest(&plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(&plan)
	if err := json.Unmarshal(raw, req.GpuOwnerAttachment.GpuPlan); err != nil {
		t.Fatal(err)
	}
	refreshManagedFixtureDigests(t, req)
	return req
}

func TestPostgresManagedLWSAdmissionUsesOriginalContextAndObservedOwnerUID(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedLWSCreateFixture(t, tenant, resource)
	repo := NewRepository(pool)
	owner := service.NewInferenceServerWithCommands(NewCreateUseCase(repo), nil, NewCommandUseCase(repo))
	if _, err := owner.CreateInferenceService(ctx, req); err != nil {
		t.Fatal(err)
	}
	source := NewManagedLWSAdmissionSource(pool, "models")
	if _, err := source.ManagedLWSRuntime(ctx, "models", req.Name, "observed-llmi"); !errors.Is(err, kube.ErrManagedLWSBindingPending) {
		t.Fatalf("unbound first CREATE allowed: %v", err)
	}
	if err := repo.UpsertRuntimeBindingCAS(ctx, RuntimeBindingInput{TenantID: tenant.String(), ServiceID: resource.String(), Generation: 1, ObjectKind: kube.KServeLLMInferenceServiceKind, ObjectNamespace: "models", ObjectName: req.Name, ObjectUID: "observed-llmi", ResourceVersion: "1", Role: "runtime"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := source.ManagedLWSRuntime(ctx, "models", req.Name, "observed-llmi")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TenantID != tenant.String() || loaded.ServiceID != resource.String() || loaded.GPUPlan.ResolutionDigest != req.GpuOwnerAttachment.GpuPlan.ResolutionDigest || loaded.Resources.GPU.ContainerName != "main" || loaded.Resources.GPU.Replicas != 4 {
		t.Fatalf("original runtime context lost: %+v", loaded.RuntimeSpec)
	}
	obj, err := kube.RenderKServeRuntime(loaded.RuntimeSpec)
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetKind() != "LLMInferenceService" {
		t.Fatalf("wrong real renderer object: %v", obj)
	}
	saved, err := New(pool).GetManagedGPUResource(ctx, GetManagedGPUResourceParams{TenantID: pgUUID(tenant), ResourceID: pgUUID(resource)})
	if err != nil {
		t.Fatal(err)
	}
	var corrupted gpu.RefundContext
	if err := json.Unmarshal(saved.OriginalContext, &corrupted); err != nil {
		t.Fatal(err)
	}
	corrupted.Plan = nil
	bad, _ := json.Marshal(corrupted)
	if _, err := pool.Exec(ctx, "UPDATE inference_managed_gpu_resources SET original_context=$3 WHERE tenant_id=$1 AND resource_id=$2", tenant, resource, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ManagedLWSRuntime(ctx, "models", req.Name, "observed-llmi"); err == nil {
		t.Fatal("nil original plan accepted")
	}
	if _, err := pool.Exec(ctx, "UPDATE inference_managed_gpu_resources SET original_context=$3 WHERE tenant_id=$1 AND resource_id=$2", tenant, resource, saved.OriginalContext); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ManagedLWSRuntime(ctx, "models", req.Name, "forged-uid"); !errors.Is(err, kube.ErrManagedLWSBindingPending) {
		t.Fatalf("forged owner UID selected context: %v", err)
	}
	if _, err := source.ManagedLWSRuntime(ctx, "other", req.Name, "observed-llmi"); err == nil {
		t.Fatal("cross namespace owner selected context")
	}
	if _, err := source.ManagedLWSRuntime(ctx, "models", "unrelated", "other"); !errors.Is(err, kube.ErrUnmanagedLWSOwner) {
		t.Fatalf("unrelated owner classified managed: %v", err)
	}
	if _, err := owner.DeleteInferenceService(ctx, managedDeleteFixture(t, req)); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ManagedLWSRuntime(ctx, "models", req.Name, "observed-llmi"); err == nil {
		t.Fatal("closing intent allowed new projection")
	}
}

func TestPostgresManagedLWSAdmissionRejectsAmbiguousCrossTenantBinding(t *testing.T) {
	ctx, pool, firstTenant, firstResource := lifecycleDatabase(t)
	secondCtx, secondPool, secondTenant, secondResource := lifecycleDatabase(t)
	for i, item := range []struct{ tenant, resource uuid.UUID }{{firstTenant, firstResource}, {secondTenant, secondResource}} {
		req := managedLWSCreateFixture(t, item.tenant, item.resource)
		req.Name = "shared-observed-name"
		refreshManagedFixtureDigests(t, req)
		activeCtx, activePool := ctx, pool
		if i == 1 {
			activeCtx, activePool = secondCtx, secondPool
		}
		activeCtx = service.WithTenantID(gpu.WithGovernanceIdentity(activeCtx), item.tenant.String())
		repo := NewRepository(activePool)
		if _, err := service.NewInferenceServer(NewCreateUseCase(repo)).CreateInferenceService(activeCtx, req); err != nil {
			t.Fatal(err)
		}
		if err := repo.UpsertRuntimeBindingCAS(activeCtx, RuntimeBindingInput{TenantID: item.tenant.String(), ServiceID: item.resource.String(), Generation: 1, ObjectKind: kube.KServeLLMInferenceServiceKind, ObjectNamespace: "models", ObjectName: req.Name, ObjectUID: "ambiguous-llmi", ResourceVersion: "1", Role: "runtime"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewManagedLWSAdmissionSource(pool, "models").ManagedLWSRuntime(ctx, "models", "shared-observed-name", "ambiguous-llmi"); err == nil {
		t.Fatal("one observed owner selected two tenants")
	}
}
