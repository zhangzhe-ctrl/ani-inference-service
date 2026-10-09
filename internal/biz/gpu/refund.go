package gpu

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidRefund = errors.New("invalid managed GPU refund")

// OriginalCharge is an immutable Governance ledger entry saved at command
// acceptance. Units are the original quota units, never provider memory blocks.
type OriginalCharge struct {
	ChargeID      string `json:"charge_id"`
	QuotaCode     string `json:"quota_code"`
	OriginalUnits int64  `json:"original_units"`
}

// RefundContext is loaded from the accepted owner command snapshots. A DELETE
// ID proves only durable intent; the lifecycle owner supplies completion facts.
type RefundContext struct {
	TenantID, ResourceID, OriginalCreateOperationID  string
	DeleteOperationID, OwnerService, MeteringVersion string
	Plan                                             *Plan
	GPUCharges, Charges                              []OriginalCharge
}

type RefundContextStore interface {
	LoadRefundContext(context.Context, string, string, string) (RefundContext, error)
}

type ReleaseReason string

const (
	ResourceReleased ReleaseReason = "RESOURCE_RELEASED"
	AbortedCleaned   ReleaseReason = "ABORTED_CLEANED"
)

type ReleaseItem struct {
	ChargeID, QuotaCode string
	ReleasedTotal       int64
}

// ReleaseNotification must already be durable before calling the reporter.
// Retry this same event and content after an error or an uncertain response.
// Reporting never creates an event ID or decides whether cleanup is complete.
type ReleaseNotification struct {
	TenantID, ResourceID, OriginalCreateOperationID, ReleaseEventID string
	Reason                                                          ReleaseReason
	Items                                                           []ReleaseItem
	ResourceRefs                                                    []string
}

type ReleaseResult struct {
	ChargeID                    string
	AppliedDelta, ReleasedTotal int64
}

type ReleaseReceipt struct{ Items []ReleaseResult }

type RefundReporter interface {
	ReportQuotaRelease(context.Context, ReleaseNotification) (ReleaseReceipt, error)
}

// ValidateRefund checks the caller's persisted completion notice against the
// original accepted context. It does not infer cleanup from DELETE or a plan.
func ValidateRefund(n ReleaseNotification, original RefundContext) error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", ErrInvalidRefund, message) }
	for _, id := range []string{n.TenantID, n.ResourceID, n.OriginalCreateOperationID, n.ReleaseEventID, original.DeleteOperationID} {
		if !uuidPattern.MatchString(id) {
			return invalid("UUID identity and persisted DELETE are required")
		}
	}
	if n.TenantID != original.TenantID || n.ResourceID != original.ResourceID || n.OriginalCreateOperationID != original.OriginalCreateOperationID || original.DeleteOperationID == original.OriginalCreateOperationID || original.OwnerService != "ani-inference" || original.MeteringVersion != "gpu-metering-v1" {
		return invalid("original owner context mismatch")
	}
	if n.Reason != ResourceReleased && n.Reason != AbortedCleaned {
		return invalid("completion reason is required")
	}
	if original.Plan == nil || original.Plan.Profile == nil || original.Plan.Profile.Spec == nil || original.Plan.Totals == nil {
		return invalid("original plan is required")
	}
	if err := ValidatePlan(original.Plan, original.Plan.Request); err != nil {
		return invalid("original plan digest mismatch")
	}
	if len(n.Items) == 0 || len(n.Items) > 16 || len(n.ResourceRefs) > 64 || len(original.GPUCharges) == 0 || len(original.Charges) == 0 || len(original.Charges) > 64 {
		return invalid("invalid charge vector or audit references")
	}
	charges := make(map[string]OriginalCharge, len(original.Charges))
	codes := make(map[string]bool, len(original.Charges))
	for _, c := range original.Charges {
		if !uuidPattern.MatchString(c.ChargeID) || c.QuotaCode == "" || c.OriginalUnits <= 0 || codes[c.QuotaCode] || (strings.HasPrefix(c.QuotaCode, "gpu.") && !isGPUQuota(c.QuotaCode)) {
			return invalid("invalid original charge")
		}
		if _, duplicate := charges[c.ChargeID]; duplicate {
			return invalid("duplicate original charge")
		}
		charges[c.ChargeID], codes[c.QuotaCode] = c, true
	}
	gpuCharges := make(map[string]OriginalCharge, len(original.GPUCharges))
	for _, c := range original.GPUCharges {
		if saved, ok := charges[c.ChargeID]; !ok || saved != c || !isGPUQuota(c.QuotaCode) {
			return invalid("GPU subset differs from original charges")
		}
		if _, duplicate := gpuCharges[c.ChargeID]; duplicate {
			return invalid("duplicate original GPU charge")
		}
		gpuCharges[c.ChargeID] = c
	}
	for _, c := range original.Charges {
		if isGPUQuota(c.QuotaCode) {
			if _, ok := gpuCharges[c.ChargeID]; !ok {
				return invalid("incomplete saved GPU subset")
			}
		}
	}
	// v1.2 has one metering code per fixed GPU profile. This check protects MiB
	// from accidentally being replaced with the provider's q block encoding.
	if len(gpuCharges) != 1 {
		return invalid("unsupported GPU metering vector")
	}
	for _, c := range gpuCharges {
		switch original.Plan.Profile.Spec.Mode {
		case 1: // WHOLE_EXCLUSIVE
			if c.QuotaCode != "gpu.physical.count" || c.OriginalUnits != original.Plan.Totals.ExclusiveDeviceCount {
				return invalid("whole GPU charge differs from original plan total")
			}
		case 2: // SHARED_FIXED
			if c.QuotaCode != "gpu.shared_memory_mib" || c.OriginalUnits != original.Plan.Totals.SharedMemoryMiB {
				return invalid("shared GPU charge differs from original MiB total")
			}
		default:
			return invalid("unsupported original GPU mode")
		}
	}
	seen := make(map[string]bool, len(n.Items))
	for _, it := range n.Items {
		c, ok := charges[it.ChargeID]
		if !ok || seen[it.ChargeID] || c.QuotaCode != it.QuotaCode || it.ReleasedTotal < 0 || it.ReleasedTotal > c.OriginalUnits {
			return invalid("unknown, duplicate or inconsistent release item")
		}
		if isGPUQuota(c.QuotaCode) && it.ReleasedTotal != c.OriginalUnits {
			return invalid("GPU release must use full original cumulative units")
		}
		seen[it.ChargeID] = true
	}
	for id := range gpuCharges {
		if !seen[id] {
			return invalid("GPU release requires the complete original GPU subset")
		}
	}
	for _, ref := range n.ResourceRefs {
		if !uuidPattern.MatchString(ref) {
			return invalid("invalid audit resource reference")
		}
	}
	return nil
}

func isGPUQuota(code string) bool {
	return code == "gpu.shared_memory_mib" || code == "gpu.physical.count"
}

// ValidateReleaseReceipt requires one authoritative result for every requested
// item. A lower cumulative total may not erase a later successful release.
func ValidateReleaseReceipt(n ReleaseNotification, original RefundContext, receipt ReleaseReceipt) error {
	if len(receipt.Items) != len(n.Items) {
		return fmt.Errorf("%w: incomplete Governance receipt", ErrInvalidRefund)
	}
	wanted := make(map[string]ReleaseItem, len(n.Items))
	units := make(map[string]int64, len(original.Charges))
	for _, it := range n.Items {
		wanted[it.ChargeID] = it
	}
	for _, c := range original.Charges {
		units[c.ChargeID] = c.OriginalUnits
	}
	seen := make(map[string]bool, len(receipt.Items))
	for _, result := range receipt.Items {
		it, ok := wanted[result.ChargeID]
		if !ok || seen[result.ChargeID] || result.AppliedDelta < 0 || result.AppliedDelta > it.ReleasedTotal || result.ReleasedTotal < it.ReleasedTotal || result.ReleasedTotal > units[result.ChargeID] || result.AppliedDelta > result.ReleasedTotal {
			return fmt.Errorf("%w: inconsistent Governance receipt", ErrInvalidRefund)
		}
		seen[result.ChargeID] = true
	}
	return nil
}
