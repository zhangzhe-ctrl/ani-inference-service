package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/admission"
	gpubiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"
)

// tenantContextKey carries the tenant scope selected at the transport
// boundary. Trusted middleware may set it directly; the isolated validation
// middleware may derive it from the explicit direct-request metadata fallback.
type tenantContextKey struct{}
type actorContextKey struct{}

var (
	ErrMissingTenant  = errors.New("tenant identity is required")
	ErrTenantMismatch = errors.New("request tenant does not match trusted tenant")
)

func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

func TenantID(ctx context.Context) (string, bool) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	return tenantID, ok && tenantID != ""
}

// RequireTenant prefers the tenant selected by trusted transport context. The
// direct tenant is an isolated validation fallback and, when both are set,
// must match the trusted scope.
func RequireTenant(ctx context.Context, directTenant string) (string, error) {
	directTenant = strings.TrimSpace(directTenant)
	trustedTenant, ok := TenantID(ctx)
	if !ok {
		if directTenant == "" {
			return "", ErrMissingTenant
		}
		return directTenant, nil
	}
	if directTenant != "" && directTenant != trustedTenant {
		return "", ErrTenantMismatch
	}
	return trustedTenant, nil
}

// WithActor supplies the authenticated subject for durable audit attribution.
// IAM middleware owns the value; an absent actor remains an explicitly empty
// value until the IAM contract is wired.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

func Actor(ctx context.Context) string {
	actor, _ := ctx.Value(actorContextKey{}).(string)
	return actor
}

// CreateInput is the durable command handed to the application layer. The
// service only validates and normalizes transport input; the use case persists
// it and returns immediately with an asynchronous operation.
type CreateInput = inferencebiz.CreateInput
type CreateUseCase = inferencebiz.CreateUseCase

func runtimeMode(req *inferencev1.RuntimeSpec) string {
	if req == nil || req.GetMode() == inferencev1.RuntimeMode_RUNTIME_MODE_UNSPECIFIED || req.GetMode() == inferencev1.RuntimeMode_RUNTIME_MODE_DEPLOYMENT {
		return "deployment"
	}
	if req.GetMode() == inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET {
		return "leader_worker_set"
	}
	return "unsupported"
}

func runtimeProvider(req *inferencev1.RuntimeSpec) string {
	// New inference services are reconciled through KServe. Persisted services
	// still carry their provider explicitly, so this default only affects the
	// create request path and does not rewrite legacy resources on update.
	provider := "kserve"
	if req != nil && req.GetProvider() != "" {
		provider = strings.ToLower(strings.TrimSpace(req.GetProvider()))
	}
	if provider != "kserve" {
		return "unsupported"
	}
	return provider
}

func gpuInput(in *inferencev1.GpuRequest) *gpubiz.Request {
	if in == nil {
		return nil
	}
	return &gpubiz.Request{ClusterID: in.GetClusterId(), PoolID: in.GetPoolId(), ProfileID: in.GetProfileId(), ProfileVersion: in.GetProfileVersion(), Replicas: in.GetReplicas(), DevicesPerReplica: in.GetDevicesPerReplica(), ContainerName: in.GetContainerName()}
}

func gpuProto(in *gpubiz.Request) *inferencev1.GpuRequest {
	if in == nil {
		return nil
	}
	return &inferencev1.GpuRequest{ClusterId: in.ClusterID, PoolId: in.PoolID, ProfileId: in.ProfileID, ProfileVersion: in.ProfileVersion, Replicas: in.Replicas, DevicesPerReplica: in.DevicesPerReplica, ContainerName: in.ContainerName}
}

func engineInput(engine *inferencev1.EngineSpec) (image, runtime string, argv []string) {
	if engine == nil {
		return "", "", nil
	}
	// The current persistence contract stores a legacy argv slice. Both pieces
	// still originate in the request; preserving command and args as separate
	// fields is a follow-up migration so the runtime adapter can pass them
	// through without flattening them.
	argv = append(argv, engine.GetCommand()...)
	argv = append(argv, engine.GetArgs()...)
	return engine.GetImage(), engine.GetType(), argv
}

func validateEngineInput(engine *inferencev1.EngineSpec) error {
	if engine == nil {
		return errors.New("engine is required")
	}
	if strings.TrimSpace(engine.GetType()) == "" {
		return errors.New("engine.type is required")
	}
	if strings.TrimSpace(engine.GetImage()) == "" {
		return errors.New("engine.image is required")
	}
	if len(engine.GetCommand()) == 0 {
		return errors.New("engine.command is required")
	}
	return nil
}

// validateGPUResourceMap keeps GPU selection in the explicit resource.gpu
// object. A request that omits that object must not silently enable a device
// through a legacy Kubernetes extended-resource key. Non-GPU extended
// resources remain available to callers that need them.
func validateGPUResourceMap(resourceSpec *inferencev1.ResourceSpec) error {
	if resourceSpec == nil || resourceSpec.GetGpu() != nil {
		return nil
	}
	for name := range resourceSpec.GetRequests() {
		if isLegacyGPUResourceName(name) {
			return fmt.Errorf("GPU resource %q must be supplied as resource.gpu", name)
		}
	}
	for name := range resourceSpec.GetLimits() {
		if isLegacyGPUResourceName(name) {
			return fmt.Errorf("GPU resource %q must be supplied as resource.gpu", name)
		}
	}
	return nil
}

func isLegacyGPUResourceName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(name, "gpu") || strings.Contains(name, "gaudi")
}

// InferenceServer is the transport adapter. Business behavior is injected so
// the gRPC layer stays independent of PostgreSQL and Kubernetes adapters.
type InferenceServer struct {
	inferencev1.UnimplementedInferenceServiceManagerServer
	create  CreateUseCase
	read    inferencebiz.ReadUseCase
	command inferencebiz.CommandUseCase
	update  inferencebiz.UpdateUseCase
	refund  gpubiz.RefundReporter
}

// SetManagedGPURefundReporter wires the completion reporter into the real
// service. The lifecycle owner invokes it with an already durable completion
// notification; no public RPC or normal lifecycle step calls this method.
func (s *InferenceServer) SetManagedGPURefundReporter(reporter gpubiz.RefundReporter) {
	s.refund = reporter
}
func (s *InferenceServer) ReportManagedGPUCompletion(ctx context.Context, notification gpubiz.ReleaseNotification) (gpubiz.ReleaseReceipt, error) {
	if s == nil || s.refund == nil {
		return gpubiz.ReleaseReceipt{}, errors.New("managed GPU refund reporter is not configured")
	}
	return s.refund.ReportQuotaRelease(ctx, notification)
}

func NewInferenceServer(create ...CreateUseCase) *InferenceServer {
	var uc CreateUseCase
	if len(create) > 0 {
		uc = create[0]
	}
	return &InferenceServer{create: uc}
}

func NewInferenceServerWithRead(create CreateUseCase, read inferencebiz.ReadUseCase) *InferenceServer {
	return &InferenceServer{create: create, read: read}
}

func NewInferenceServerWithCommands(create CreateUseCase, read inferencebiz.ReadUseCase, command inferencebiz.CommandUseCase) *InferenceServer {
	return &InferenceServer{create: create, read: read, command: command}
}

func NewInferenceServerWithAll(create CreateUseCase, read inferencebiz.ReadUseCase, command inferencebiz.CommandUseCase, update inferencebiz.UpdateUseCase) *InferenceServer {
	return &InferenceServer{create: create, read: read, command: command, update: update}
}

func (s *InferenceServer) UpdateInferenceService(ctx context.Context, req *inferencev1.UpdateInferenceServiceRequest) (*inferencev1.OperationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenantID, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if req.GetRequestId() == "" || req.GetResourceId() == "" || req.GetExpectedGeneration() < 1 {
		return nil, status.Error(codes.InvalidArgument, "request_id, resource_id and positive expected_generation are required")
	}
	if req.GetEngine() != nil {
		if err := validateEngineInput(req.GetEngine()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if s.update == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference update use case is not configured")
	}
	if err := validateGPUResourceMap(req.GetResource()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateUpdateMask(req.GetUpdateMask(), req.GetModelVersionId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	normalized, err := resources.Normalize(resources.Spec{Requests: req.GetResource().GetRequests(), Limits: req.GetResource().GetLimits(), GPU: gpuInput(req.GetResource().GetGpu())})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	mode := runtimeMode(req.GetRuntime())
	provider := strings.ToLower(strings.TrimSpace(req.GetRuntime().GetProvider()))
	if provider != "" && provider != "kserve" {
		return nil, status.Error(codes.InvalidArgument, "runtime provider must be kserve")
	}
	if err := validateEndpoint(req.GetRuntime().GetEndpoint()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	workers := int32(1)
	if req.GetRuntime() != nil && req.GetRuntime().GetWorkerReplicas() > 0 {
		workers = req.GetRuntime().GetWorkerReplicas()
	}
	if err := admission.ValidateCreate(admission.CreateRequest{Replicas: req.GetReplicas(), WorkerReplicas: workers, RuntimeMode: mode, Resources: admission.ResourceInput{Requests: normalized.Requests, Limits: normalized.Limits}}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if normalized.GPU != nil {
		if err := gpubiz.ValidateTopology(normalized.GPU, req.GetReplicas(), mode, workers); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	canonical := proto.Clone(req).(*inferencev1.UpdateInferenceServiceRequest)
	canonical.Resource = &inferencev1.ResourceSpec{Requests: normalized.Requests, Limits: normalized.Limits, Gpu: gpuProto(normalized.GPU)}
	canonical.Runtime = &inferencev1.RuntimeSpec{Mode: inferencev1.RuntimeMode_RUNTIME_MODE_DEPLOYMENT, WorkerReplicas: workers}
	if mode == "leader_worker_set" {
		canonical.Runtime.Mode = inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET
	}
	// Preserve an omitted provider as the inherit marker on update; the
	// persistence layer resolves it against the existing service provider.
	canonical.Runtime.Provider = provider
	canonical.Runtime.Endpoint = req.GetRuntime().GetEndpoint()
	image, engineRuntime, commandArgv := engineInput(req.GetEngine())
	artifactProvider, artifactRef, artifactSHA := "", "", ""
	if artifact := req.GetModelArtifact(); artifact != nil {
		artifactProvider, artifactRef, artifactSHA = artifact.GetProvider(), artifact.GetReference(), artifact.GetSha256()
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return nil, status.Error(codes.Internal, "marshal request: "+err.Error())
	}
	out, err := s.update.Update(ctx, inferencebiz.UpdateInput{TenantID: tenantID, RequestID: req.GetRequestId(), Actor: Actor(ctx), ServiceID: req.GetResourceId(), ExpectedGeneration: req.GetExpectedGeneration(), ModelVersionID: req.GetModelVersionId(), ArtifactProvider: artifactProvider, ArtifactRef: artifactRef, ArtifactSHA256: artifactSHA, ImageRef: image, ServedModelName: req.GetServedModelName(), EngineRuntime: engineRuntime, RuntimeProvider: provider, CommandArgv: commandArgv, Resources: normalized, Replicas: req.GetReplicas(), WorkerReplicas: workers, RuntimeMode: mode, Endpoint: endpointInput(req.GetRuntime().GetEndpoint()), RequestHash: hashBytes(encoded)})
	if err != nil {
		return nil, commandStatusError(err)
	}
	if out == nil || out.GetOperation() == nil {
		return nil, status.Error(codes.Internal, "update use case returned no asynchronous operation")
	}
	return out, nil
}

func validateUpdateMask(mask *fieldmaskpb.FieldMask, modelVersionID string) error {
	if mask == nil || len(mask.GetPaths()) == 0 {
		return errors.New("update_mask must include resource, replicas and runtime")
	}
	seen := map[string]bool{}
	for _, path := range mask.GetPaths() {
		if path != "resource" && path != "replicas" && path != "runtime" && path != "engine" && path != "model_artifact" && path != "served_model_name" && path != "model_version_id" {
			return fmt.Errorf("unsupported update field %q", path)
		}
		seen[path] = true
	}
	for _, path := range []string{"resource", "replicas", "runtime"} {
		if !seen[path] {
			return fmt.Errorf("update_mask must include %s", path)
		}
	}
	if seen["model_version_id"] != (modelVersionID != "") {
		return errors.New("model_version_id and its update_mask entry must be provided together")
	}
	return nil
}

func (s *InferenceServer) StartInferenceService(ctx context.Context, req *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error) {
	return s.acceptCommand(ctx, "start", req)
}

func (s *InferenceServer) StopInferenceService(ctx context.Context, req *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error) {
	return s.acceptCommand(ctx, "stop", req)
}

func (s *InferenceServer) RestartInferenceService(ctx context.Context, req *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error) {
	return s.acceptCommand(ctx, "restart", req)
}

func (s *InferenceServer) acceptCommand(ctx context.Context, kind string, req *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenantID, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if req.GetRequestId() == "" || req.GetResourceId() == "" || req.GetExpectedGeneration() < 1 {
		return nil, status.Error(codes.InvalidArgument, "request_id, resource_id and positive expected_generation are required")
	}
	if s.command == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference command use case is not configured")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	if err != nil {
		return nil, status.Error(codes.Internal, "marshal request: "+err.Error())
	}
	// Each RPC uses the same protobuf message, so include its action in the
	// idempotency hash to reject request IDs reused for another command.
	out, err := s.command.Command(ctx, inferencebiz.CommandInput{
		TenantID: tenantID, RequestID: req.GetRequestId(), Actor: Actor(ctx), ServiceID: req.GetResourceId(),
		Kind: kind, ExpectedGeneration: req.GetExpectedGeneration(),
		RequestHash: hashBytes(append([]byte(kind+"\x00"), encoded...)),
	})
	if err != nil {
		return nil, commandStatusError(err)
	}
	if out == nil || out.GetOperation() == nil {
		return nil, status.Error(codes.Internal, "command use case returned no asynchronous operation")
	}
	return out, nil
}

func commandStatusError(err error) error {
	switch {
	case errors.Is(err, inferencebiz.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, inferencebiz.ErrGenerationConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, inferencebiz.ErrOperationActive), errors.Is(err, inferencebiz.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, inferencebiz.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return err
	}
}

func (s *InferenceServer) GetInferenceService(ctx context.Context, req *inferencev1.GetInferenceServiceRequest) (*inferencev1.InferenceService, error) {
	if req == nil || req.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource_id is required")
	}
	tenant, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if s.read == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference read use case is not configured")
	}
	out, err := s.read.GetService(ctx, tenant, req.GetResourceId())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "inference service not found")
	}
	return out, err
}

func (s *InferenceServer) ListInferenceServices(ctx context.Context, req *inferencev1.ListInferenceServicesRequest) (*inferencev1.ListInferenceServicesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenant, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if s.read == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference read use case is not configured")
	}
	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 100 {
		return nil, status.Error(codes.InvalidArgument, "page_size must be between 1 and 100")
	}
	services, next, err := s.read.ListServices(ctx, tenant, pageSize, req.GetPageToken())
	if errors.Is(err, inferencebiz.ErrInvalidPageToken) {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}
	if err != nil {
		return nil, err
	}
	return &inferencev1.ListInferenceServicesResponse{Services: services, NextPageToken: next}, nil
}

func (s *InferenceServer) GetOperation(ctx context.Context, req *inferencev1.GetOperationRequest) (*inferencev1.Operation, error) {
	if req == nil || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "operation_id is required")
	}
	tenant, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if s.read == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference read use case is not configured")
	}
	out, err := s.read.GetOperation(ctx, tenant, req.GetOperationId())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "operation not found")
	}
	return out, err
}

func (s *InferenceServer) ListOperations(ctx context.Context, req *inferencev1.ListOperationsRequest) (*inferencev1.ListOperationsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenant, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if s.read == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference read use case is not configured")
	}
	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 100 {
		return nil, status.Error(codes.InvalidArgument, "page_size must be between 1 and 100")
	}
	operations, next, err := s.read.ListOperations(ctx, tenant, req.GetResourceId(), pageSize, req.GetPageToken())
	if errors.Is(err, inferencebiz.ErrInvalidPageToken) {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}
	if err != nil {
		return nil, err
	}
	return &inferencev1.ListOperationsResponse{Operations: operations, NextPageToken: next}, nil
}

func (s *InferenceServer) CreateInferenceService(ctx context.Context, req *inferencev1.CreateInferenceServiceRequest) (*inferencev1.OperationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenantID, ok := TenantID(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "tenant identity is required")
	}
	if req.GetRequestId() == "" || req.GetName() == "" || req.GetModelVersionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id, name and model_version_id are required")
	}
	if err := validateEngineInput(req.GetEngine()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateGPUResourceMap(req.GetResource()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	normalized, err := resources.Normalize(resources.Spec{Requests: req.GetResource().GetRequests(), Limits: req.GetResource().GetLimits(), GPU: gpuInput(req.GetResource().GetGpu())})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	mode := runtimeMode(req.GetRuntime())
	provider := runtimeProvider(req.GetRuntime())
	if provider == "unsupported" {
		return nil, status.Error(codes.InvalidArgument, "runtime provider must be kserve")
	}
	if err := validateEndpoint(req.GetRuntime().GetEndpoint()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	workers := int32(1)
	if req.GetRuntime() != nil && req.GetRuntime().GetWorkerReplicas() > 0 {
		workers = req.GetRuntime().GetWorkerReplicas()
	}
	if err := admission.ValidateCreate(admission.CreateRequest{Replicas: req.GetReplicas(), WorkerReplicas: workers, RuntimeMode: mode, Resources: admission.ResourceInput{Requests: normalized.Requests, Limits: normalized.Limits}}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if normalized.GPU != nil {
		if err := gpubiz.ValidateTopology(normalized.GPU, req.GetReplicas(), mode, workers); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	var managed *inferencebiz.ManagedGPUCommand
	if normalized.GPU != nil {
		managed, err = managedCreate(ctx, req)
		if err != nil {
			return nil, err
		}
	} else if req.GetGpuOwnerAttachment() != nil || len(req.GetOriginalCharges()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "GPU attachment requires resource.gpu")
	}
	canonical := proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
	canonical.Resource = &inferencev1.ResourceSpec{Requests: normalized.Requests, Limits: normalized.Limits, Gpu: gpuProto(normalized.GPU)}
	canonical.Runtime = &inferencev1.RuntimeSpec{Mode: inferencev1.RuntimeMode_RUNTIME_MODE_DEPLOYMENT, WorkerReplicas: workers}
	if mode == "leader_worker_set" {
		canonical.Runtime.Mode = inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET
	}
	// Hash the effective provider so an omitted provider (which defaults to
	// KServe) and an explicit provider produce the same idempotency payload.
	canonical.Runtime.Provider = provider
	canonical.Runtime.Endpoint = req.GetRuntime().GetEndpoint()
	image, engineRuntime, commandArgv := engineInput(req.GetEngine())
	artifactProvider, artifactRef, artifactSHA := "", "", ""
	if artifact := req.GetModelArtifact(); artifact != nil {
		artifactProvider, artifactRef, artifactSHA = artifact.GetProvider(), artifact.GetReference(), artifact.GetSha256()
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return nil, status.Error(codes.Internal, "marshal request: "+err.Error())
	}
	if s.create == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference create use case is not configured")
	}
	in := CreateInput{TenantID: tenantID, RequestID: req.GetRequestId(), Actor: Actor(ctx), Name: req.GetName(), ModelVersionID: req.GetModelVersionId(), ArtifactProvider: artifactProvider, ArtifactRef: artifactRef, ArtifactSHA256: artifactSHA, ImageRef: image, ServedModelName: req.GetServedModelName(), EngineRuntime: engineRuntime, RuntimeProvider: provider, CommandArgv: commandArgv, Resources: normalized, Replicas: req.GetReplicas(), WorkerReplicas: workers, RuntimeMode: mode, Endpoint: endpointInput(req.GetRuntime().GetEndpoint()), RequestHash: hashBytes(encoded), ManagedGPU: managed}
	if managed != nil {
		in.GPUPlan, in.GPUPlanDigest = managed.Context.Plan, managed.Context.Plan.ResolutionDigest
		in.RequestHash = hashBytes(managed.Payload)
		in.Actor = req.GetGpuOwnerAttachment().GetActor().GetType() + ":" + req.GetGpuOwnerAttachment().GetActor().GetId()
	}
	out, err := s.create.Create(ctx, in)
	if err != nil {
		if errors.Is(err, inferencebiz.ErrIdempotencyConflict) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, err
	}
	if out == nil || out.GetOperation() == nil {
		return nil, status.Error(codes.Internal, "create use case returned no asynchronous operation")
	}
	return out, nil
}

func endpointInput(in *inferencev1.EndpointSpec) *inferencebiz.EndpointSpec {
	if in == nil {
		return nil
	}
	return &inferencebiz.EndpointSpec{ContainerPort: in.GetContainerPort(), ServicePort: in.GetServicePort(), TargetPort: in.GetTargetPort(), Protocol: in.GetProtocol()}
}

func validateEndpoint(in *inferencev1.EndpointSpec) error {
	if in == nil {
		return nil
	}
	if in.GetContainerPort() < 1 || in.GetContainerPort() > 65535 || in.GetServicePort() < 1 || in.GetServicePort() > 65535 {
		return errors.New("endpoint container_port and service_port must be in 1..65535")
	}
	if in.GetTargetPort() == "" {
		return errors.New("endpoint target_port is required")
	}
	switch in.GetProtocol() {
	case "TCP", "UDP", "SCTP":
	default:
		return errors.New("endpoint protocol must be TCP, UDP or SCTP")
	}
	return nil
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
