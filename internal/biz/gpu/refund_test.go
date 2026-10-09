package gpu

import (
	"errors"
	"testing"
)

func refundFixture(t *testing.T) (ReleaseNotification, RefundContext) {
	t.Helper()
	plan := &Plan{SchemaVersion: 1, Request: validRequest(), Profile: &Profile{ProfileID: validRequest().ProfileID, ProfileVersion: 1, Spec: &ProfileSpec{Mode: 2, SharedMemoryMiB: 1024}}, Encoding: &MemoryEncoding{MemoryBlockMiB: 256, MemoryBlocksPerDevice: 4, SharedMemoryMiB: 1024}, Totals: &ResourceTotals{LogicalDeviceCount: 1, SharedMemoryMiB: 1024}, Runtime: &RuntimeFragment{SchedulerName: "volcano"}}
	digest, err := PlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.ResolutionDigest = digest
	gpuCharge := OriginalCharge{ChargeID: "10000000-0000-4000-8000-000000000011", QuotaCode: "gpu.shared_memory_mib", OriginalUnits: 1024}
	otherCharge := OriginalCharge{ChargeID: "10000000-0000-4000-8000-000000000012", QuotaCode: "inference.count", OriginalUnits: 1}
	original := RefundContext{TenantID: "10000000-0000-4000-8000-000000000013", ResourceID: "10000000-0000-4000-8000-000000000014", OriginalCreateOperationID: "10000000-0000-4000-8000-000000000015", DeleteOperationID: "10000000-0000-4000-8000-000000000016", OwnerService: "ani-inference", MeteringVersion: "gpu-metering-v1", Plan: plan, GPUCharges: []OriginalCharge{gpuCharge}, Charges: []OriginalCharge{gpuCharge, otherCharge}}
	n := ReleaseNotification{TenantID: original.TenantID, ResourceID: original.ResourceID, OriginalCreateOperationID: original.OriginalCreateOperationID, ReleaseEventID: "10000000-0000-4000-8000-000000000017", Reason: ResourceReleased, Items: []ReleaseItem{{ChargeID: gpuCharge.ChargeID, QuotaCode: gpuCharge.QuotaCode, ReleasedTotal: gpuCharge.OriginalUnits}}, ResourceRefs: []string{original.ResourceID}}
	return n, original
}

func TestRefundUsesFullOriginalMiBAndAllowsOriginalNonGPUItems(t *testing.T) {
	n, original := refundFixture(t)
	n.Items = append(n.Items, ReleaseItem{ChargeID: original.Charges[1].ChargeID, QuotaCode: original.Charges[1].QuotaCode, ReleasedTotal: 1})
	if err := ValidateRefund(n, original); err != nil {
		t.Fatal(err)
	}
	n.Items[0].ReleasedTotal = original.Plan.Encoding.MemoryBlocksPerDevice
	if !errors.Is(ValidateRefund(n, original), ErrInvalidRefund) {
		t.Fatal("provider memory blocks were accepted instead of original MiB")
	}
}

func TestRefundRejectsWrongOriginalContextAndIncompleteGPU(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(*ReleaseNotification, *RefundContext)
	}{
		{"wrong-create", func(n *ReleaseNotification, original *RefundContext) {
			n.OriginalCreateOperationID = original.DeleteOperationID
		}},
		{"wrong-tenant", func(n *ReleaseNotification, original *RefundContext) { n.TenantID = original.ResourceID }},
		{"wrong-owner", func(_ *ReleaseNotification, original *RefundContext) { original.OwnerService = "ani-model" }},
		{"no-delete", func(_ *ReleaseNotification, original *RefundContext) { original.DeleteOperationID = "" }},
		{"partial", func(n *ReleaseNotification, _ *RefundContext) { n.Items[0].ReleasedTotal = 512 }},
		{"over-total", func(n *ReleaseNotification, _ *RefundContext) { n.Items[0].ReleasedTotal = 2048 }},
		{"missing-gpu", func(n *ReleaseNotification, original *RefundContext) {
			n.Items = []ReleaseItem{{ChargeID: original.Charges[1].ChargeID, QuotaCode: original.Charges[1].QuotaCode, ReleasedTotal: 1}}
		}},
		{"wrong-charge", func(n *ReleaseNotification, _ *RefundContext) { n.Items[0].ChargeID = n.ResourceID }},
		{"wrong-meter", func(_ *ReleaseNotification, original *RefundContext) { original.MeteringVersion = "provider-blocks" }},
		{"wrong-digest", func(_ *ReleaseNotification, original *RefundContext) { original.Plan.Runtime.SchedulerName = "changed" }},
		{"ordinary-exit", func(n *ReleaseNotification, _ *RefundContext) { n.Reason = "EXITED" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, original := refundFixture(t)
			test.alter(&n, &original)
			if !errors.Is(ValidateRefund(n, original), ErrInvalidRefund) {
				t.Fatal("invalid notification accepted")
			}
		})
	}
}

func TestRefundReceiptValidatesEntireAuthoritativeResponse(t *testing.T) {
	n, original := refundFixture(t)
	if err := ValidateReleaseReceipt(n, original, ReleaseReceipt{Items: []ReleaseResult{{ChargeID: n.Items[0].ChargeID, ReleasedTotal: 1024, AppliedDelta: 0}}}); err != nil {
		t.Fatal(err)
	}
	for _, receipt := range []ReleaseReceipt{
		{},
		{Items: []ReleaseResult{{ChargeID: n.ResourceID, ReleasedTotal: 1024, AppliedDelta: 1024}}},
		{Items: []ReleaseResult{{ChargeID: n.Items[0].ChargeID, ReleasedTotal: 512, AppliedDelta: 512}}},
		{Items: []ReleaseResult{{ChargeID: n.Items[0].ChargeID, ReleasedTotal: 2048, AppliedDelta: 1024}}},
		{Items: []ReleaseResult{{ChargeID: n.Items[0].ChargeID, ReleasedTotal: 1024, AppliedDelta: -1}}},
	} {
		if !errors.Is(ValidateReleaseReceipt(n, original, receipt), ErrInvalidRefund) {
			t.Fatal("inconsistent receipt accepted")
		}
	}
}
