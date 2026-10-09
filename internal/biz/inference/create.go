package inference

import (
	"context"

	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
)

// CreateInput is the transport-independent command accepted after admission.
type CreateInput struct {
	TenantID, RequestID, Actor, Name, ModelVersionID string
	ArtifactProvider, ArtifactRef, ArtifactSHA256    string
	ImageRef, ServedModelName, EngineRuntime         string
	RuntimeProvider                                  string
	CommandArgv                                      []string
	Resources                                        resources.Normalized
	GPUPlan                                          *gpu.Plan
	GPUPlanDigest                                    string
	Replicas, WorkerReplicas                         int32
	RuntimeMode                                      string
	Endpoint                                         *EndpointSpec
	RequestHash                                      string
	ManagedGPU                                       *ManagedGPUCommand
}

// ManagedGPUCommand preserves the trusted command independently of later
// local generations. Payload is the complete transport command, Business is
// the canonical body without attachment/IDs, and Context is the original
// immutable ledger/plan reference used by the refund client.
type ManagedGPUCommand struct {
	Context                     gpu.RefundContext
	Payload, Business           []byte
	BusinessDigest, RequestHash string
}

type EndpointSpec struct {
	ContainerPort int32
	ServicePort   int32
	TargetPort    string
	Protocol      string
}

type CreateUseCase interface {
	Create(context.Context, CreateInput) (*inferencev1.OperationResponse, error)
}
