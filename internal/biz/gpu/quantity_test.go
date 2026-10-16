package gpu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func quantityPlan(t *testing.T, name string) *Plan {
	t.Helper()
	raw, err := os.ReadFile("../../../testdata/accelerator-quantity-plans.json")
	if err != nil {
		t.Fatal(err)
	}
	var plans map[string]*Plan
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatal(err)
	}
	if plans[name] == nil {
		t.Fatalf("missing real Accelerator builder vector %s", name)
	}
	return plans[name]
}

func refreshQuantityDigests(t *testing.T, plan *Plan) {
	t.Helper()
	raw, err := canonical(map[string]any{"spec": plan.Profile.Spec, "baseline_id": plan.Profile.BaselineID})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(append([]byte("acc-c14n-v1\n"), raw...))
	plan.Profile.SpecDigest = hex.EncodeToString(digest[:])
	plan.ResolutionDigest, err = PlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQuantityManagedPlanAcceptsRealBuilderVectors(t *testing.T) {
	whole1, whole2 := quantityPlan(t, "Whole1"), quantityPlan(t, "Whole2")
	if !reflect.DeepEqual(whole1.Profile, whole2.Profile) || whole1.Profile.Spec.MaxDevicesPerReplica != 2 {
		t.Fatal("one/two-device vectors do not use the same max=2 profile")
	}
	for _, name := range []string{"Whole1", "Whole2", "Shared1", "WholeProfileMax"} {
		t.Run(name, func(t *testing.T) {
			plan := quantityPlan(t, name)
			if err := ValidateManagedPlan(plan, plan.Request); err != nil {
				t.Fatal(err)
			}
			if err := ValidateTopology(plan.Request, 1, "deployment", 1); err != nil {
				t.Fatal(err)
			}
			digest, err := PlanDigest(plan)
			if err != nil || digest != plan.ResolutionDigest {
				t.Fatalf("Accelerator/Inference digest mismatch: %s %v", digest, err)
			}
		})
	}
}

func TestQuantityManagedPlanRejectsRehashedInconsistency(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		mutate       func(*Plan)
	}{
		{"whole-max1-request2", "Whole2", func(p *Plan) { p.Profile.Spec.MaxDevicesPerReplica = 1 }},
		{"whole-over-profile-maximum", "WholeProfileMax", func(p *Plan) { p.Request.DevicesPerReplica = p.Profile.Spec.MaxDevicesPerReplica + 1 }},
		{"shared-max2", "Shared1", func(p *Plan) { p.Profile.Spec.MaxDevicesPerReplica = 2 }},
		{"shared-request2", "Shared1", func(p *Plan) { p.Request.DevicesPerReplica, p.Profile.Spec.MaxDevicesPerReplica = 2, 2 }},
		{"request0", "Whole2", func(p *Plan) { p.Request.DevicesPerReplica = 0 }},
		{"request3", "Whole2", func(p *Plan) { p.Request.DevicesPerReplica = 3 }},
		{"wrong-total", "Whole2", func(p *Plan) { p.Totals.ExclusiveDeviceCount = 1 }},
		{"nonhex-baseline-digest", "Whole1", func(p *Plan) { p.BaselineDigest = strings.Repeat("z", 64) }},
		{"wrong-number", "Whole2", func(p *Plan) {
			for i := range p.Runtime.LimitsPerContainer {
				if p.Runtime.LimitsPerContainer[i].Key == "volcano.sh/vgpu-number" {
					p.Runtime.LimitsPerContainer[i].Value = "1"
				}
			}
		}},
		{"shared-overflow", "Shared1", func(p *Plan) { p.Request.Replicas, p.Profile.Spec.SharedMemoryMiB = 16, math.MaxInt64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := quantityPlan(t, tc.source)
			tc.mutate(plan)
			refreshQuantityDigests(t, plan)
			if err := ValidateManagedPlan(plan, plan.Request); err == nil {
				t.Fatal("inconsistent quantities accepted despite a recomputed digest")
			}
		})
	}
}
