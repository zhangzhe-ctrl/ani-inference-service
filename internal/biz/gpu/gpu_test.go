package gpu

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestCanonicalMatchesAcceleratorContractExample(t *testing.T) {
	got, err := canonical(map[string]any{"z": int64(9223372036854775807), "a": "<&中文>", "b": true})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"a":"<&中文>","b":true,"z":"9223372036854775807"}`
	if string(got) != want {
		t.Fatalf("canonical = %s, want %s", got, want)
	}
	digestBytes := sha256.Sum256(append([]byte("acc-c14n-v1\n"), got...))
	digest := hex.EncodeToString(digestBytes[:])
	if digest != "c8151fcfc0f56c9a06c7a35c22ad29a17501bceacabde76c420bff9ffc4a43cb" {
		t.Fatalf("digest = %s", digest)
	}
}

func validRequest() *Request {
	return &Request{ClusterID: "10000000-0000-4000-8000-000000000001", PoolID: "10000000-0000-4000-8000-000000000002", ProfileID: "10000000-0000-4000-8000-000000000003", ProfileVersion: 1, Replicas: 1, DevicesPerReplica: 1, ContainerName: "kserve-container"}
}

func TestValidateRequestAcceptsPhaseOneShape(t *testing.T) {
	for _, devices := range []uint32{1, 2, 4} {
		r := validRequest()
		r.DevicesPerReplica = devices
		if err := ValidateRequest(r); err != nil {
			t.Fatalf("ValidateRequest(%d) error = %v", devices, err)
		}
	}
}

func TestValidateRequestRejectsWrongContainerShape(t *testing.T) {
	r := validRequest()
	r.DevicesPerReplica = 0
	if err := ValidateRequest(r); err == nil {
		t.Fatal("ValidateRequest() accepted devices_per_replica=0")
	}
}

func TestValidateTopologyUsesRuntimeContainerName(t *testing.T) {
	r := validRequest()
	if err := ValidateTopology(r, 1, "deployment", 1); err != nil {
		t.Fatalf("deployment: %v", err)
	}
	r.ContainerName = "main"
	r.Replicas = 2 // one leader plus one worker in the single LWS group
	if err := ValidateTopology(r, 1, "leader_worker_set", 1); err != nil {
		t.Fatalf("LWS: %v", err)
	}
	r.DevicesPerReplica = 2
	if err := ValidateTopology(r, 2, "leader_worker_set", 3); err == nil {
		t.Fatal("accepted GPU replica count that differs from the LWS pod topology")
	}
}

func TestTotalPodsForLeaderWorkerSet(t *testing.T) {
	got, err := TotalPods(2, "leader_worker_set", 3)
	if err != nil || got != 8 {
		t.Fatalf("TotalPods() = %d, error=%v; want 8", got, err)
	}
}

func TestValidateTopologyRequiresMatchingReplicaCount(t *testing.T) {
	if err := ValidateTopology(validRequest(), 2, "deployment", 1); err == nil {
		t.Fatal("accepted mismatched GPU/runtime replicas")
	}
}

func TestPlanDigestChangesWhenRuntimeChanges(t *testing.T) {
	p := &Plan{SchemaVersion: 1, Request: validRequest(), Profile: &Profile{ProfileID: validRequest().ProfileID, ProfileVersion: 1}, Runtime: &RuntimeFragment{SchedulerName: "volcano"}}
	a, err := PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Runtime.SchedulerName = "other"
	b, err := PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("plan digest did not commit runtime")
	}
}

func TestPlanDigestSortsManagedRuntimeFields(t *testing.T) {
	p := &Plan{SchemaVersion: 1, Request: validRequest(), Profile: &Profile{ProfileID: validRequest().ProfileID, ProfileVersion: 1}, Runtime: &RuntimeFragment{NodeLabels: []KeyValue{{Key: "z", Value: "1"}, {Key: "a", Value: "2"}}}}
	a, err := PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Runtime.NodeLabels = []KeyValue{{Key: "a", Value: "2"}, {Key: "z", Value: "1"}}
	b, err := PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("digest depends on managed field ordering: %s != %s", a, b)
	}
}
