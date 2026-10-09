package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	kube "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
)

type ManagedLWSAdmissionSource struct {
	pool      DBTX
	namespace string
}

func NewManagedLWSAdmissionSource(pool DBTX, namespace string) *ManagedLWSAdmissionSource {
	return &ManagedLWSAdmissionSource{pool: pool, namespace: namespace}
}

var _ kube.ManagedLWSAdmissionSource = (*ManagedLWSAdmissionSource)(nil)

func (s *ManagedLWSAdmissionSource) ManagedLWSRuntime(ctx context.Context, namespace, ownerName, ownerUID string) (kube.DesiredRuntime, error) {
	if s == nil || s.pool == nil || s.namespace == "" || namespace != s.namespace || ownerName == "" || ownerUID == "" {
		return kube.DesiredRuntime{}, fmt.Errorf("managed LWS owner lookup scope is incomplete")
	}
	q := New(s.pool)
	bindings, err := q.ListManagedLWSOwnerBindings(ctx, ListManagedLWSOwnerBindingsParams{ObjectNamespace: namespace, ObjectName: ownerName, ObjectUid: ownerUID})
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	if len(bindings) == 0 {
		candidate, err := q.HasManagedLWSOwnerCandidate(ctx, ownerName)
		if err != nil {
			return kube.DesiredRuntime{}, err
		}
		if candidate {
			return kube.DesiredRuntime{}, kube.ErrManagedLWSBindingPending
		}
		return kube.DesiredRuntime{}, kube.ErrUnmanagedLWSOwner
	}
	if len(bindings) != 1 {
		return kube.DesiredRuntime{}, fmt.Errorf("LLMI identity matches more than one managed tenant/resource")
	}
	bound := bindings[0]
	if bound.Closing {
		return kube.DesiredRuntime{}, fmt.Errorf("managed resource has a persisted closing intent")
	}
	var original gpu.RefundContext
	if err := json.Unmarshal(bound.OriginalContext, &original); err != nil {
		return kube.DesiredRuntime{}, fmt.Errorf("decode original managed GPU context: %w", err)
	}
	tenantID, resourceID := bound.TenantID.String(), bound.ServiceID.String()
	if original.TenantID != tenantID || original.ResourceID != resourceID || original.OwnerService != "ani-inference" || original.MeteringVersion != "gpu-metering-v1" || original.OriginalCreateOperationID == "" {
		return kube.DesiredRuntime{}, fmt.Errorf("observed LLMI binding differs from its original managed owner context")
	}
	if original.Plan == nil {
		return kube.DesiredRuntime{}, fmt.Errorf("original managed GPU plan is missing")
	}
	if err := gpu.ValidateManagedPlan(original.Plan, original.Plan.Request); err != nil {
		return kube.DesiredRuntime{}, err
	}
	runtime, err := NewRuntimeSource(s.pool, s.namespace).CurrentRuntime(ctx, tenantID, resourceID, bound.DesiredGeneration)
	if err != nil {
		return kube.DesiredRuntime{}, err
	}
	if !runtime.ManagedGPU || runtime.RuntimeMode != "leader_worker_set" || runtime.RuntimeProvider != "kserve" || runtime.Name != ownerName || runtime.GPUPlan == nil || runtime.GPUPlan.ResolutionDigest != original.Plan.ResolutionDigest {
		return kube.DesiredRuntime{}, fmt.Errorf("current LWS runtime differs from the original frozen Governance plan")
	}
	return runtime, nil
}
