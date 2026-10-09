package inferencev1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestBusinessCanonicalFixedVectorAndRoundtrip(t *testing.T) {
	req := &CreateInferenceServiceRequest{RequestId: "dynamic", Name: "<&中文>", ModelVersionId: "11111111-1111-4111-8111-111111111111", Replicas: 1, Resource: &ResourceSpec{Limits: map[string]string{"memory": "8Gi", "cpu": "2"}}, Engine: &EngineSpec{Type: "vllm", Image: "engine:v1", Command: []string{"serve"}}, Runtime: &RuntimeSpec{Mode: RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET, WorkerReplicas: 1}}
	const expected = `{"engine":{"args":[],"command":["serve"],"image":"engine:v1","type":"vllm"},"model_artifact":null,"model_version_id":"11111111-1111-4111-8111-111111111111","name":"<&中文>","replicas":"1","resource":{"gpu":null,"limits":{"cpu":"2","memory":"8Gi"},"requests":{}},"runtime":{"endpoint":null,"mode":"2","provider":"","worker_replicas":"1"},"served_model_name":""}`
	raw, err := CanonicalBusinessPayload(req)
	if err != nil || string(raw) != expected {
		t.Fatalf("canonical=%s error=%v", raw, err)
	}
	digest, err := BusinessPayloadDigest(req)
	sum := sha256.Sum256([]byte(expected))
	if err != nil || digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest=%s error=%v", digest, err)
	}
	t.Logf("fixed business digest=%s", digest)
	decoded, err := DecodeBusinessPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestId = ""
	if !proto.Equal(req, decoded) {
		t.Fatalf("decoded business differs: %+v", decoded)
	}
	req.RequestId = "another"
	req.GpuOwnerAttachment = &integrationv1.GpuOwnerCreateAttachment{RequestHash: "irrelevant"}
	req.OriginalCharges = []*OriginalQuotaCharge{{ChargeId: "dynamic", QuotaCode: "cpu", OriginalUnits: 5}}
	excluded, err := CanonicalBusinessPayload(req)
	if err != nil || !bytes.Equal(raw, excluded) {
		t.Fatalf("trusted context contaminated business digest: %s error=%v", excluded, err)
	}
	req.Name = "changed"
	changed, _ := BusinessPayloadDigest(req)
	if changed == digest {
		t.Fatal("business changes did not alter digest")
	}
}

func TestBusinessRejectsUnknownAndNoncanonicalPayload(t *testing.T) {
	req := &CreateInferenceServiceRequest{}
	req.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	if _, err := CanonicalBusinessPayload(req); err == nil {
		t.Fatal("unknown business fields accepted")
	}
	if _, err := DecodeBusinessPayload([]byte(`{"name":"partial"}`)); err == nil {
		t.Fatal("noncanonical partial payload accepted")
	}
}

func TestBusinessValidationRejectsResourcesBeforeQuota(t *testing.T) {
	req := &CreateInferenceServiceRequest{Name: "valid", ModelVersionId: "11111111-1111-4111-8111-111111111111", Replicas: 1, Engine: &EngineSpec{Type: "vllm", Image: "engine:v1", Command: []string{"serve"}}, Resource: &ResourceSpec{Requests: map[string]string{"cpu": "3"}, Limits: map[string]string{"cpu": "2"}}}
	if err := ValidateBusinessPayload(req); err == nil {
		t.Fatal("request above limit accepted")
	}
	req.Resource.Requests["cpu"] = "invalid"
	if err := ValidateBusinessPayload(req); err == nil {
		t.Fatal("invalid quantity accepted")
	}
	req.Resource.Requests["cpu"] = "1"
	if err := ValidateBusinessPayload(req); err != nil {
		t.Fatal(err)
	}
}

func TestBusinessValidationFailsClosedForManagedLWSOnly(t *testing.T) {
	req := &CreateInferenceServiceRequest{Name: "distributed", ModelVersionId: "11111111-1111-4111-8111-111111111111", Replicas: 1, Engine: &EngineSpec{Type: "vllm", Image: "engine:v1", Command: []string{"serve"}}, Runtime: &RuntimeSpec{Mode: RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET, WorkerReplicas: 1}, Resource: &ResourceSpec{}}
	if err := ValidateBusinessPayload(req); err != nil {
		t.Fatalf("CPU LWS changed: %v", err)
	}
	req.Resource.Gpu = &GpuRequest{ClusterId: req.ModelVersionId, PoolId: req.ModelVersionId, ProfileId: req.ModelVersionId, ProfileVersion: 1, Replicas: 2, DevicesPerReplica: 1, ContainerName: "main"}
	if err := ValidateBusinessPayload(req); err == nil {
		t.Fatal("GPU LWS accepted before a supported synchronous projection exists")
	}
}
