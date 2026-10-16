package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	inferencev1 "github.com/liangzai006/ani-inference-service/api/inference/v1"
	"github.com/liangzai006/ani-inference-service/internal/biz/gpu"
	kube "github.com/liangzai006/ani-inference-service/internal/data/kubernetes"
	"github.com/liangzai006/ani-inference-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func quantityManagedCreate(t *testing.T, tenant, resource uuid.UUID, name string) (*inferencev1.CreateInferenceServiceRequest, *gpu.Plan) {
	t.Helper()
	raw, err := os.ReadFile("../../../testdata/accelerator-quantity-plans.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	var plan gpu.Plan
	if err := json.Unmarshal(vectors[name], &plan); err != nil {
		t.Fatal(err)
	}
	req := managedCreateFixture(t, tenant, resource)
	if err := json.Unmarshal(vectors[name], req.GpuOwnerAttachment.GpuPlan); err != nil {
		t.Fatal(err)
	}
	req.Resource.Gpu, req.Replicas = gpuProto(plan.Request), int32(plan.Request.Replicas)
	charge := req.GpuOwnerAttachment.GpuCharges[0]
	if plan.Profile.Spec.Mode == 1 {
		charge.QuotaCode, charge.OriginalUnits = "gpu.physical.count", plan.Totals.ExclusiveDeviceCount
	} else {
		charge.QuotaCode, charge.OriginalUnits = "gpu.shared_memory_mib", plan.Totals.SharedMemoryMiB
	}
	req.OriginalCharges[1].QuotaCode, req.OriginalCharges[1].OriginalUnits = charge.QuotaCode, charge.OriginalUnits
	refreshManagedFixtureDigests(t, req)
	return req, &plan
}

func TestPostgresManagedGPUQuantityPlanRoundtrip(t *testing.T) {
	for _, name := range []string{"Whole1", "Whole2", "Shared1"} {
		t.Run(name, func(t *testing.T) {
			ctx, pool, tenant, resource := lifecycleDatabase(t)
			ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
			req, expected := quantityManagedCreate(t, tenant, resource, name)
			repo := NewRepository(pool)
			server := service.NewInferenceServerWithAll(NewCreateUseCase(repo), nil, NewCommandUseCase(repo), NewUpdateUseCase(repo))
			accepted, err := server.CreateInferenceService(ctx, req)
			if err != nil || !accepted.GetDurableOwnerAck().GetAccepted() {
				t.Fatalf("managed quantity acceptance: result=%v error=%v", accepted, err)
			}
			replayed, err := server.CreateInferenceService(ctx, req)
			if err != nil || replayed.GetOperation().GetId() != accepted.GetOperation().GetId() {
				t.Fatalf("quantity replay lost original operation: %v %v", replayed, err)
			}
			row, err := New(pool).GetManagedGPUResource(ctx, GetManagedGPUResourceParams{TenantID: pgUUID(tenant), ResourceID: pgUUID(resource)})
			if err != nil {
				t.Fatal(err)
			}
			// JSONB preserves the object, not wire whitespace/key order. Restore
			// canonical JSON before invoking the strict public wire decoder.
			var storedBusiness map[string]any
			if err := json.Unmarshal(row.BusinessPayload, &storedBusiness); err != nil {
				t.Fatal(err)
			}
			var canonicalBusiness bytes.Buffer
			encoder := json.NewEncoder(&canonicalBusiness)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(storedBusiness); err != nil {
				t.Fatal(err)
			}
			business, err := inferencev1.DecodeBusinessPayload(bytes.TrimSuffix(canonicalBusiness.Bytes(), []byte{'\n'}))
			if err != nil {
				t.Fatal(err)
			}
			hash, err := inferencev1.ManagedCreateRequestHash(business, tenant.String(), req.GpuOwnerAttachment.Actor.Type, req.GpuOwnerAttachment.Actor.Id)
			if err != nil || hash != row.CreateRequestHash || row.BusinessPayloadDigest != req.GpuOwnerAttachment.BusinessPayloadDigest {
				t.Fatalf("stored business/hash changed: hash=%s error=%v", hash, err)
			}
			loaded, err := NewRuntimeSource(pool, "models").CurrentRuntime(ctx, tenant.String(), resource.String(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.ManagedGPU || !reflect.DeepEqual(loaded.GPUPlan, expected) || !reflect.DeepEqual(loaded.Resources.GPU, expected.Request) || loaded.Replicas != 1 {
				t.Fatalf("complete quantity plan did not survive PG/CurrentRuntime: %+v", loaded.RuntimeSpec)
			}
			if err := gpu.ValidateManagedPlan(loaded.GPUPlan, loaded.Resources.GPU); err != nil {
				t.Fatal(err)
			}
			obj, err := kube.RenderKServeRuntime(loaded.RuntimeSpec)
			if err != nil {
				t.Fatal(err)
			}
			predictor, _, _ := unstructured.NestedMap(obj.Object, "spec", "predictor")
			if predictor["schedulerName"] != expected.Runtime.SchedulerName || predictor["minReplicas"] != int64(1) || predictor["maxReplicas"] != int64(1) {
				t.Fatalf("quantity changed pod topology: %v", predictor)
			}
			selectors, _, _ := unstructured.NestedStringMap(predictor, "nodeSelector")
			for _, field := range expected.Runtime.NodeLabels {
				if selectors[field.Key] != field.Value {
					t.Fatalf("frozen selector lost: %s", field.Key)
				}
			}
			annotations, _, _ := unstructured.NestedStringMap(predictor, "annotations")
			if annotations["scheduling.volcano.sh/queue-name"] != expected.Runtime.QueueName || annotations["volcano.sh/vgpu-mode"] != "hami-core" {
				t.Fatalf("queue or mode lost: %v", annotations)
			}
			containers, _, _ := unstructured.NestedSlice(predictor, "containers")
			container := containers[0].(map[string]interface{})
			limits, _, _ := unstructured.NestedStringMap(container, "resources", "limits")
			if container["name"] != expected.Request.ContainerName {
				t.Fatalf("wrong GPU container: %v", container)
			}
			for _, field := range expected.Runtime.LimitsPerContainer {
				if limits[field.Key] != field.Value {
					t.Fatalf("frozen runtime limit lost: %s=%s", field.Key, limits[field.Key])
				}
			}
			command, _, _ := unstructured.NestedStringSlice(container, "command")
			args, _, _ := unstructured.NestedStringSlice(container, "args")
			if !reflect.DeepEqual(command, []string{"/bin/sh", "-c"}) || !reflect.DeepEqual(args, []string{req.Engine.Command}) {
				t.Fatalf("engine command changed: %v %v", command, args)
			}
			deletion := managedDeleteFixture(t, req)
			deleted, err := server.DeleteInferenceService(ctx, deletion)
			if err != nil || !deleted.GetDurableOwnerAck().GetAccepted() {
				t.Fatalf("original quantity delete context rejected: %v %v", deleted, err)
			}
			original, err := repo.LoadRefundContext(ctx, tenant.String(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId)
			if err != nil || original.DeleteOperationID != deletion.GpuOwnerAttachment.DeleteOperationId || !reflect.DeepEqual(original.Plan, expected) || len(original.Charges) != len(req.OriginalCharges) {
				t.Fatalf("DELETE lost full original context: %+v error=%v", original, err)
			}
			for i, charge := range original.Charges {
				want := req.OriginalCharges[i]
				if charge.ChargeID != want.ChargeId || charge.QuotaCode != want.QuotaCode || charge.OriginalUnits != want.OriginalUnits {
					t.Fatalf("original charge changed: %+v", charge)
				}
			}
			gpuCharge := req.GpuOwnerAttachment.GpuCharges[0]
			notice := gpu.ReleaseNotification{TenantID: tenant.String(), ResourceID: resource.String(), OriginalCreateOperationID: req.GpuOwnerAttachment.Ref.CreateOperationId, ReleaseEventID: uuid.NewString(), Reason: gpu.ResourceReleased, Items: []gpu.ReleaseItem{{ChargeID: gpuCharge.ChargeId, QuotaCode: gpuCharge.QuotaCode, ReleasedTotal: gpuCharge.OriginalUnits}}}
			if err := gpu.ValidateRefund(notice, original); err != nil {
				t.Fatal("original quantity refund contract did not decode", err)
			}
			if _, err := repo.LoadRefundContext(ctx, uuid.NewString(), resource.String(), req.GpuOwnerAttachment.Ref.CreateOperationId); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("cross-tenant original context visible: %v", err)
			}
		})
	}
}

func TestPostgresManagedGPUQuantityRejectsWrongOriginalCharge(t *testing.T) {
	for _, name := range []string{"Whole2", "Shared1"} {
		t.Run(name, func(t *testing.T) {
			ctx, pool, tenant, resource := lifecycleDatabase(t)
			ctx = service.WithTenantID(gpu.WithGovernanceIdentity(ctx), tenant.String())
			req, _ := quantityManagedCreate(t, tenant, resource, name)
			req.GpuOwnerAttachment.GpuCharges[0].OriginalUnits, req.OriginalCharges[1].OriginalUnits = 1, 1
			refreshManagedFixtureDigests(t, req)
			_, err := service.NewInferenceServer(NewCreateUseCase(NewRepository(pool))).CreateInferenceService(ctx, req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("wrong original quantity accepted: %v", err)
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM inference_managed_gpu_commands WHERE tenant_id=$1`, tenant).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected quantity left a command/ACK: count=%d error=%v", count, err)
			}
		})
	}
}
