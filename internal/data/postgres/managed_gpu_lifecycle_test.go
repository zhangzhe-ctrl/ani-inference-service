package postgres

import (
	"testing"

	"github.com/google/uuid"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestPostgresManagedGPUDeletePreservesAcceptedCreateAndRejectsTopologyMutation(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedCreateFixture(t, tenant, resource)
	repo := NewRepository(pool)
	server := service.NewInferenceServerWithAll(NewCreateUseCase(repo), nil, NewCommandUseCase(repo), NewUpdateUseCase(repo))
	if _, err := server.CreateInferenceService(ctx, req); err != nil {
		t.Fatal(err)
	}
	// This fixture only marks the accepted operation terminal so the existing
	// update/start/stop intake can be exercised without executing any workload.
	if _, err := pool.Exec(ctx, `UPDATE inference_operations SET phase='succeeded',step='complete' WHERE tenant_id=$1 AND id=$2`, tenant, req.GpuOwnerAttachment.Ref.CreateOperationId); err != nil {
		t.Fatal(err)
	}
	update := &inferencev1.UpdateInferenceServiceRequest{RequestId: uuid.NewString(), ResourceId: resource.String(), ExpectedGeneration: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"resource", "replicas", "runtime"}}, Resource: proto.Clone(req.Resource).(*inferencev1.ResourceSpec), Replicas: 1, Runtime: proto.Clone(req.Runtime).(*inferencev1.RuntimeSpec)}
	update.Resource.Gpu.ProfileId = uuid.NewString()
	if _, err := server.UpdateInferenceService(ctx, update); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ungoverned GPU selection change accepted: %v", err)
	}
	update.Resource = proto.Clone(req.Resource).(*inferencev1.ResourceSpec)
	update.Replicas, update.Resource.Gpu.Replicas = 2, 2
	if _, err := server.UpdateInferenceService(ctx, update); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ungoverned replica change accepted: %v", err)
	}
	// An unchanged selection is allowed and copies the original plan.
	update.Replicas, update.Resource.Gpu.Replicas = 1, 1
	updated, err := server.UpdateInferenceService(ctx, update)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := New(pool).GetSpec(ctx, GetSpecParams{TenantID: pgUUID(tenant), ServiceID: pgUUID(resource), Generation: 2})
	if err != nil || spec.GpuPlanDigest.String != req.GpuOwnerAttachment.GpuPlan.ResolutionDigest {
		t.Fatalf("unchanged update lost frozen plan: %+v error=%v", spec, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE inference_operations SET phase='succeeded',step='complete' WHERE tenant_id=$1 AND id=$2`, tenant, updated.GetOperation().GetId()); err != nil {
		t.Fatal(err)
	}
	stopped, err := server.StopInferenceService(ctx, &inferencev1.ServiceCommandRequest{RequestId: uuid.NewString(), ResourceId: resource.String(), ExpectedGeneration: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE inference_operations SET phase='succeeded',step='complete' WHERE tenant_id=$1 AND id=$2`, tenant, stopped.GetOperation().GetId()); err != nil {
		t.Fatal(err)
	}
	if _, err := server.StartInferenceService(ctx, &inferencev1.ServiceCommandRequest{RequestId: uuid.NewString(), ResourceId: resource.String(), ExpectedGeneration: 3}); err != nil {
		t.Fatal(err)
	}
	var reservations int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inference_quota_reservations WHERE tenant_id=$1`, tenant).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("lifecycle reoccupied Governance charge: count=%d error=%v", reservations, err)
	}
	original, err := repo.LoadRefundContext(ctx, tenant.String(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId)
	if err != nil || original.DeleteOperationID != "" {
		t.Fatalf("ordinary lifecycle created refund intent: %+v error=%v", original, err)
	}
	deletion := managedDeleteFixture(t, req)
	out, err := server.DeleteInferenceService(ctx, deletion)
	if err != nil || !out.GetDurableOwnerAck().GetAccepted() {
		t.Fatalf("DELETE with active local operation=%v error=%v", out, err)
	}
	original, err = repo.LoadRefundContext(ctx, tenant.String(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId)
	if err != nil || original.DeleteOperationID != deletion.GpuOwnerAttachment.DeleteOperationId || original.Plan.ResolutionDigest != req.GpuOwnerAttachment.GpuPlan.ResolutionDigest {
		t.Fatalf("DELETE altered original context: %+v error=%v", original, err)
	}
}

func TestPostgresManagedGPUAcceptanceRollbackLeavesNoACK(t *testing.T) {
	ctx, pool, tenant, resource := lifecycleDatabase(t)
	ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
	req := managedCreateFixture(t, tenant, resource)
	repo := NewRepository(pool)
	if _, err := repo.CreateService(ctx, CreateAggregateInput{TenantID: tenant.String(), Name: req.Name, ModelVersionID: uuid.NewString(), RequestHash: "direct-seed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.NewInferenceServer(NewCreateUseCase(repo)).CreateInferenceService(ctx, req); err == nil {
		t.Fatal("duplicate business name accepted")
	}
	var commands, resources int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM inference_managed_gpu_commands WHERE tenant_id=$1),(SELECT count(*) FROM inference_managed_gpu_resources WHERE tenant_id=$1)`, tenant).Scan(&commands, &resources); err != nil || commands != 0 || resources != 0 {
		t.Fatalf("rollback retained command/ACK/context: commands=%d resources=%d error=%v", commands, resources, err)
	}
}
