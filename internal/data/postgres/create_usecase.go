package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	gpubiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"google.golang.org/protobuf/encoding/protojson"
)

// CreateUseCase adapts the admitted command to the durable PostgreSQL
// aggregate transaction. Kubernetes and remote quota calls happen later in
// worker steps, after this transaction commits.
type CreateUseCase struct{ repo createRepository }

type createRepository interface {
	CreateService(context.Context, CreateAggregateInput) (CreateAggregateResult, error)
}

func NewCreateUseCase(repo createRepository) *CreateUseCase { return &CreateUseCase{repo: repo} }

func (u *CreateUseCase) Create(ctx context.Context, in inferencebiz.CreateInput) (*inferencev1.OperationResponse, error) {
	if u == nil || u.repo == nil {
		return nil, errors.New("nil postgres create repository")
	}
	resourcesJSON, err := json.Marshal(struct {
		Requests map[string]string `json:"requests"`
		Limits   map[string]string `json:"limits"`
		GPU      *gpubiz.Request   `json:"gpu,omitempty"`
	}{Requests: in.Resources.Requests, Limits: in.Resources.Limits, GPU: in.Resources.GPU})
	if err != nil {
		return nil, err
	}
	gpuRequestJSON, err := json.Marshal(in.Resources.GPU)
	if in.Resources.GPU == nil {
		gpuRequestJSON = nil
	}
	if err != nil {
		return nil, err
	}
	gpuPlanJSON, err := json.Marshal(in.GPUPlan)
	if in.GPUPlan == nil {
		gpuPlanJSON = nil
	}
	if err != nil {
		return nil, err
	}
	gpuPlanDigest := in.GPUPlanDigest
	if gpuPlanDigest == "" && in.GPUPlan != nil {
		gpuPlanDigest = in.GPUPlan.ResolutionDigest
	}
	commandJSON, err := json.Marshal(in.CommandArgv)
	if err != nil {
		return nil, err
	}
	serviceID, operationID := uuid.New(), uuid.New()
	idempotencyKey := in.RequestID
	var responseSnapshot []byte
	if in.ManagedGPU != nil {
		idempotencyKey = "" // immutable Gov command ID owns replay
		serviceID, err = uuid.Parse(in.ManagedGPU.Context.ResourceID)
		if err != nil {
			return nil, err
		}
		operationID, err = uuid.Parse(in.ManagedGPU.Context.OriginalCreateOperationID)
		if err != nil {
			return nil, err
		}
		responseSnapshot, err = protojson.Marshal(createResponse(in, CreateAggregateResult{ServiceID: serviceID.String(), OperationID: operationID.String()}))
		if err != nil {
			return nil, err
		}
	}
	result, err := u.repo.CreateService(ctx, CreateAggregateInput{
		TenantID: in.TenantID, Actor: in.Actor, ServiceID: serviceID.String(), SpecID: uuid.NewString(), OperationID: operationID.String(),
		Name: in.Name, ModelVersionID: in.ModelVersionID, Resources: resourcesJSON,
		ArtifactProvider: in.ArtifactProvider, ArtifactRef: in.ArtifactRef, ArtifactSHA256: in.ArtifactSHA256,
		ImageRef: in.ImageRef, ServedModelName: in.ServedModelName, EngineRuntime: in.EngineRuntime, CommandArgv: commandJSON,
		Replicas: in.Replicas, RequestHash: in.RequestHash, Method: "CreateInferenceService",
		RuntimeMode: in.RuntimeMode, WorkerReplicas: in.WorkerReplicas, RuntimeProvider: in.RuntimeProvider,
		GPURequest: gpuRequestJSON,
		GPUPlan:    gpuPlanJSON, GPUPlanDigest: gpuPlanDigest,
		ManagedGPU: in.ManagedGPU, ResponseSnapshot: responseSnapshot,
		EndpointEnabled:       in.Endpoint != nil,
		EndpointContainerPort: endpointContainerPort(in.Endpoint), EndpointServicePort: endpointServicePort(in.Endpoint),
		EndpointTargetPort: endpointTargetPort(in.Endpoint), EndpointProtocol: endpointProtocol(in.Endpoint),
		IdempotencyKey: idempotencyKey, RequestID: in.RequestID, Generation: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(result.ResponseSnapshot) != 0 {
		return decodeManagedResponse(result.ResponseSnapshot)
	}
	return createResponse(in, result), nil
}

func createResponse(in inferencebiz.CreateInput, result CreateAggregateResult) *inferencev1.OperationResponse {
	engine := &inferencev1.EngineSpec{Type: in.EngineRuntime, Image: in.ImageRef, Command: append([]string(nil), in.CommandArgv...)}
	artifact := &inferencev1.ModelArtifact{Provider: in.ArtifactProvider, Reference: in.ArtifactRef, Sha256: in.ArtifactSHA256}
	runtime := &inferencev1.RuntimeSpec{Mode: runtimeModeEnum(in.RuntimeMode), WorkerReplicas: in.WorkerReplicas, Provider: runtimeProviderEnum(in.RuntimeProvider)}
	if in.Endpoint != nil {
		runtime.Endpoint = &inferencev1.EndpointSpec{ContainerPort: in.Endpoint.ContainerPort, ServicePort: in.Endpoint.ServicePort, TargetPort: in.Endpoint.TargetPort, Protocol: in.Endpoint.Protocol}
	}
	out := &inferencev1.OperationResponse{
		Resource: &inferencev1.InferenceService{
			Id: result.ServiceID, Name: in.Name, DesiredState: "running", Generation: 1,
			ModelVersionId: in.ModelVersionID, Replicas: in.Replicas,
			Resource:      &inferencev1.ResourceSpec{Requests: in.Resources.Requests, Limits: in.Resources.Limits, Gpu: gpuProto(in.Resources.GPU)},
			Runtime:       runtime,
			ModelArtifact: artifact, Engine: engine,
		},
		Operation: &inferencev1.Operation{Id: result.OperationID, ServiceId: result.ServiceID, Kind: "create", Phase: "pending", Step: "admission", TargetGeneration: 1},
	}
	if in.ManagedGPU != nil {
		out.DurableOwnerAck = &integrationv1.DurableOwnerAck{OperationId: result.OperationID, ResourceId: result.ServiceID, Accepted: true}
	}
	return out
}

func gpuProto(in *gpubiz.Request) *inferencev1.GpuRequest {
	if in == nil {
		return nil
	}
	return &inferencev1.GpuRequest{ClusterId: in.ClusterID, PoolId: in.PoolID, ProfileId: in.ProfileID, ProfileVersion: in.ProfileVersion, Replicas: in.Replicas, DevicesPerReplica: in.DevicesPerReplica, ContainerName: in.ContainerName}
}

func endpointContainerPort(e *inferencebiz.EndpointSpec) int32 {
	if e == nil {
		return 0
	}
	return e.ContainerPort
}
func endpointServicePort(e *inferencebiz.EndpointSpec) int32 {
	if e == nil {
		return 0
	}
	return e.ServicePort
}
func endpointTargetPort(e *inferencebiz.EndpointSpec) string {
	if e == nil {
		return ""
	}
	return e.TargetPort
}
func endpointProtocol(e *inferencebiz.EndpointSpec) string {
	if e == nil {
		return ""
	}
	return e.Protocol
}

func runtimeModeEnum(mode string) inferencev1.RuntimeMode {
	if mode == "leader_worker_set" {
		return inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET
	}
	return inferencev1.RuntimeMode_RUNTIME_MODE_DEPLOYMENT
}
