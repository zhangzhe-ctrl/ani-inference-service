package inference

import (
	"context"
	"testing"
)

// Old quota and resolver providers must never receive Governance-managed
// charges from ordinary start/stop/restart/update or the resolve gate.
func TestManagedGPUIsolationFromLegacyQuotaAndResolve(t *testing.T) {
	for _, tc := range []struct{ kind, step string }{
		{"create", string(StepReserveQuota)}, {"start", string(StepReserveQuota)},
		{"restart", string(StepReleasePreviousQuota)}, {"update", string(StepReleasePreviousQuota)},
		{"stop", string(StepReleaseQuota)}, {"delete", string(StepReleaseQuota)},
	} {
		t.Run(tc.kind+"/"+tc.step, func(t *testing.T) {
			store := &runnerStore{op: operation(tc.step)}
			store.op.Kind, store.op.ManagedGPU = tc.kind, true
			quotaProvider := &runnerQuota{}
			runner := &Runner{Store: store, Quota: quotaProvider, Audit: &runnerAudit{}}
			if _, err := runner.Execute(context.Background(), item()); err != nil {
				t.Fatal(err)
			}
			if quotaProvider.reserved || quotaProvider.confirmed || quotaProvider.released {
				t.Fatal("managed GPU touched legacy reservation port")
			}
		})
	}
	store := &runnerStore{op: operation(string(StepResolveGPU))}
	store.op.ManagedGPU, store.op.GPURequest, store.op.Replicas, store.op.RuntimeMode = true, validGPURequest(), 1, "deployment"
	resolver := &runnerGPU{}
	runner := &Runner{Store: store, GPU: resolver, Audit: &runnerAudit{}}
	if _, err := runner.Execute(context.Background(), item()); err == nil || resolver.calls != 0 {
		t.Fatalf("missing managed plan downgraded/resolved: calls=%d error=%v", resolver.calls, err)
	}
	store.op.GPURequest = nil
	if _, err := runner.Execute(context.Background(), item()); err == nil || resolver.calls != 0 {
		t.Fatalf("missing managed request downgraded/resolved: calls=%d error=%v", resolver.calls, err)
	}
}
