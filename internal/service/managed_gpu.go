package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	acceleratorv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func trustedGPUContext(ctx context.Context, ref *acceleratorv1.GpuUsageRef, actor *acceleratorv1.Actor, plan *acceleratorv1.ResolvedGpuPlan, charges []*integrationv1.GpuChargeRef, all []*inferencev1.OriginalQuotaCharge, metering, requestHash string) (gpu.RefundContext, error) {
	if !gpu.GovernanceIdentity(ctx) {
		return gpu.RefundContext{}, status.Error(codes.PermissionDenied, "GPU commands require the authenticated Governance service")
	}
	tenant, ok := TenantID(ctx)
	if !ok || ref == nil || tenant != ref.GetTenantId() {
		return gpu.RefundContext{}, status.Error(codes.PermissionDenied, "GPU command tenant does not match authenticated scope")
	}
	if ref.GetOwnerService() != "ani-inference" || !validGPUUUID(ref.GetTenantId()) || !validGPUUUID(ref.GetResourceId()) || !validGPUUUID(ref.GetCreateOperationId()) {
		return gpu.RefundContext{}, status.Error(codes.InvalidArgument, "invalid GPU owner reference")
	}
	if actor == nil || actor.GetType() == "" || actor.GetId() == "" || metering != "gpu-metering-v1" || !validDigest(requestHash) {
		return gpu.RefundContext{}, status.Error(codes.InvalidArgument, "GPU command actor, request digest and metering version are required")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return gpu.RefundContext{}, err
	}
	var frozen gpu.Plan
	if plan == nil {
		return gpu.RefundContext{}, status.Error(codes.InvalidArgument, "frozen GPU plan is required")
	}
	for _, message := range []proto.Message{ref, actor, plan} {
		if err := rejectUnknownGPUFields(message.ProtoReflect()); err != nil {
			return gpu.RefundContext{}, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if err := json.Unmarshal(raw, &frozen); err != nil {
		return gpu.RefundContext{}, err
	}
	if err := gpu.ValidateManagedPlan(&frozen, frozen.Request); err != nil {
		return gpu.RefundContext{}, status.Error(codes.InvalidArgument, err.Error())
	}
	originalGPU := make([]gpu.OriginalCharge, 0, len(charges))
	for _, charge := range charges {
		if charge == nil {
			return gpu.RefundContext{}, status.Error(codes.InvalidArgument, "nil original GPU charge")
		}
		originalGPU = append(originalGPU, gpu.OriginalCharge{ChargeID: charge.GetChargeId(), QuotaCode: charge.GetQuotaCode(), OriginalUnits: charge.GetOriginalUnits()})
	}
	originalAll := make([]gpu.OriginalCharge, 0, len(all))
	for _, charge := range all {
		if charge == nil {
			return gpu.RefundContext{}, status.Error(codes.InvalidArgument, "nil original charge")
		}
		originalAll = append(originalAll, gpu.OriginalCharge{ChargeID: charge.GetChargeId(), QuotaCode: charge.GetQuotaCode(), OriginalUnits: charge.GetOriginalUnits()})
	}
	if err := validateOriginalCharges(&frozen, originalGPU, originalAll); err != nil {
		return gpu.RefundContext{}, status.Error(codes.InvalidArgument, err.Error())
	}
	sort.Slice(originalGPU, func(i, j int) bool { return originalGPU[i].QuotaCode < originalGPU[j].QuotaCode })
	sort.Slice(originalAll, func(i, j int) bool { return originalAll[i].QuotaCode < originalAll[j].QuotaCode })
	return gpu.RefundContext{TenantID: tenant, ResourceID: ref.GetResourceId(), OriginalCreateOperationID: ref.GetCreateOperationId(), OwnerService: ref.GetOwnerService(), MeteringVersion: metering, Plan: &frozen, GPUCharges: originalGPU, Charges: originalAll}, nil
}

func validateOriginalCharges(plan *gpu.Plan, subset, all []gpu.OriginalCharge) error {
	if len(subset) != 1 || len(all) < 1 || len(all) > 16 {
		return fmt.Errorf("complete original charge vector is required")
	}
	byCode := make(map[string]gpu.OriginalCharge)
	byID := make(map[string]bool)
	for _, charge := range all {
		if !validGPUUUID(charge.ChargeID) || charge.QuotaCode == "" || charge.OriginalUnits <= 0 || byID[charge.ChargeID] {
			return fmt.Errorf("invalid or duplicate original charge")
		}
		if _, exists := byCode[charge.QuotaCode]; exists {
			return fmt.Errorf("duplicate original quota code")
		}
		byCode[charge.QuotaCode] = charge
		byID[charge.ChargeID] = true
	}
	expectedCode, expectedUnits := "gpu.shared_memory_mib", plan.Totals.SharedMemoryMiB
	if plan.Profile.Spec.Mode == 1 {
		expectedCode, expectedUnits = "gpu.physical.count", plan.Totals.ExclusiveDeviceCount
	}
	if subset[0].QuotaCode != expectedCode || subset[0].OriginalUnits != expectedUnits || byCode[expectedCode] != subset[0] {
		return fmt.Errorf("original GPU charge does not match frozen plan and complete charges")
	}
	for code := range byCode {
		if strings.HasPrefix(code, "gpu.") && code != expectedCode {
			return fmt.Errorf("unexpected GPU charge code")
		}
	}
	return nil
}

func managedCreate(ctx context.Context, req *inferencev1.CreateInferenceServiceRequest) (*inferencebiz.ManagedGPUCommand, error) {
	attachment := req.GetGpuOwnerAttachment()
	if attachment == nil {
		return nil, status.Error(codes.InvalidArgument, "resource.gpu requires a Governance attachment")
	}
	if err := rejectUnknownGPUFields(attachment.ProtoReflect()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	frozen, err := trustedGPUContext(ctx, attachment.GetRef(), attachment.GetActor(), attachment.GetGpuPlan(), attachment.GetGpuCharges(), req.GetOriginalCharges(), attachment.GetMeteringVersion(), attachment.GetRequestHash())
	if err != nil {
		return nil, err
	}
	if err := inferencev1.ValidateBusinessPayload(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := gpu.ValidateManagedPlan(frozen.Plan, gpuInput(req.GetResource().GetGpu())); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	business, err := inferencev1.CanonicalBusinessPayload(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	digest, err := inferencev1.BusinessPayloadDigest(req)
	if err != nil {
		return nil, err
	}
	if digest != attachment.GetBusinessPayloadDigest() {
		return nil, status.Error(codes.InvalidArgument, "business payload digest mismatch")
	}
	requestHash, err := inferencev1.ManagedCreateRequestHash(req, frozen.TenantID, attachment.GetActor().GetType(), attachment.GetActor().GetId())
	if err != nil {
		return nil, err
	}
	if requestHash != attachment.GetRequestHash() {
		return nil, status.Error(codes.InvalidArgument, "original Governance CREATE request digest mismatch")
	}
	canonical := proto.Clone(req).(*inferencev1.CreateInferenceServiceRequest)
	canonical.RequestId = ""
	normalizeCreateCharges(canonical)
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return nil, err
	}
	return &inferencebiz.ManagedGPUCommand{Context: frozen, Payload: payload, Business: business, BusinessDigest: digest, RequestHash: attachment.GetRequestHash()}, nil
}

func (s *InferenceServer) DeleteInferenceService(ctx context.Context, req *inferencev1.DeleteInferenceServiceRequest) (*inferencev1.OperationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attachment := req.GetGpuOwnerAttachment()
	if attachment == nil {
		if len(req.GetOriginalCharges()) != 0 {
			return nil, status.Error(codes.InvalidArgument, "original charges require a Governance attachment")
		}
		return s.acceptCommand(ctx, "delete", &inferencev1.ServiceCommandRequest{RequestId: req.GetRequestId(), ResourceId: req.GetResourceId(), ExpectedGeneration: req.GetExpectedGeneration()})
	}
	if len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "unknown DELETE fields are not allowed")
	}
	if err := rejectUnknownGPUFields(attachment.ProtoReflect()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	frozen, err := trustedGPUContext(ctx, attachment.GetRef(), attachment.GetActor(), attachment.GetOriginalGpuPlan(), attachment.GetOriginalGpuCharges(), req.GetOriginalCharges(), attachment.GetMeteringVersion(), attachment.GetRequestHash())
	if err != nil {
		return nil, err
	}
	if req.GetResourceId() != frozen.ResourceID || !validGPUUUID(attachment.GetDeleteOperationId()) || req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid managed DELETE identity")
	}
	frozen.DeleteOperationID = attachment.GetDeleteOperationId()
	requestHash, err := inferencev1.ManagedDeleteRequestHash(frozen.TenantID, frozen.ResourceID, frozen.OriginalCreateOperationID, attachment.GetActor().GetType(), attachment.GetActor().GetId())
	if err != nil {
		return nil, err
	}
	if requestHash != attachment.GetRequestHash() {
		return nil, status.Error(codes.InvalidArgument, "Governance DELETE request digest mismatch")
	}
	canonical := proto.Clone(req).(*inferencev1.DeleteInferenceServiceRequest)
	canonical.RequestId = ""
	canonical.ExpectedGeneration = 0 // local generation is not a Gov command identity
	sort.Slice(canonical.GpuOwnerAttachment.OriginalGpuCharges, func(i, j int) bool {
		a, b := canonical.GpuOwnerAttachment.OriginalGpuCharges[i], canonical.GpuOwnerAttachment.OriginalGpuCharges[j]
		if a.QuotaCode == b.QuotaCode {
			return a.ChargeId < b.ChargeId
		}
		return a.QuotaCode < b.QuotaCode
	})
	sort.Slice(canonical.OriginalCharges, func(i, j int) bool {
		a, b := canonical.OriginalCharges[i], canonical.OriginalCharges[j]
		if a.QuotaCode == b.QuotaCode {
			return a.ChargeId < b.ChargeId
		}
		return a.QuotaCode < b.QuotaCode
	})
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return nil, err
	}
	if s.command == nil {
		return nil, status.Error(codes.FailedPrecondition, "inference command use case is not configured")
	}
	out, err := s.command.Command(ctx, inferencebiz.CommandInput{TenantID: frozen.TenantID, RequestID: req.GetRequestId(), Actor: attachment.GetActor().GetType() + ":" + attachment.GetActor().GetId(), ServiceID: frozen.ResourceID, Kind: "delete", RequestHash: hashBytes(payload), ManagedGPU: &inferencebiz.ManagedGPUCommand{Context: frozen, Payload: payload, RequestHash: attachment.GetRequestHash()}})
	if err != nil {
		return nil, commandStatusError(err)
	}
	return out, nil
}

func normalizeCreateCharges(req *inferencev1.CreateInferenceServiceRequest) {
	sort.Slice(req.GpuOwnerAttachment.GpuCharges, func(i, j int) bool {
		a, b := req.GpuOwnerAttachment.GpuCharges[i], req.GpuOwnerAttachment.GpuCharges[j]
		if a.QuotaCode == b.QuotaCode {
			return a.ChargeId < b.ChargeId
		}
		return a.QuotaCode < b.QuotaCode
	})
	sort.Slice(req.OriginalCharges, func(i, j int) bool {
		a, b := req.OriginalCharges[i], req.OriginalCharges[j]
		if a.QuotaCode == b.QuotaCode {
			return a.ChargeId < b.ChargeId
		}
		return a.QuotaCode < b.QuotaCode
	})
}

func validGPUUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}
func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func rejectUnknownGPUFields(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("unknown GPU owner fields are not allowed")
	}
	var invalid error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind || field.IsMap() {
			return true
		}
		if field.IsList() {
			for i := 0; i < value.List().Len(); i++ {
				if err := rejectUnknownGPUFields(value.List().Get(i).Message()); err != nil {
					invalid = err
					return false
				}
			}
		} else {
			invalid = rejectUnknownGPUFields(value.Message())
		}
		return invalid == nil
	})
	return invalid
}
