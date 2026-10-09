package inferencev1

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

// ValidateBusinessPayload checks the public business contract before quota
// acceptance. Trusted attachments are validated by the Inference receiver.
func ValidateBusinessPayload(req *CreateInferenceServiceRequest) error {
	if req == nil || strings.TrimSpace(req.GetName()) == "" {
		return fmt.Errorf("name is required")
	}
	if version, err := uuid.Parse(req.GetModelVersionId()); err != nil || version == uuid.Nil {
		return fmt.Errorf("model_version_id must be a UUID")
	}
	engine := req.GetEngine()
	if engine == nil || strings.TrimSpace(engine.GetType()) == "" || strings.TrimSpace(engine.GetImage()) == "" || len(engine.GetCommand()) == 0 {
		return fmt.Errorf("engine type, image and command are required")
	}
	if req.GetReplicas() < 1 {
		return fmt.Errorf("replicas must be positive")
	}
	runtime := req.GetRuntime()
	provider := strings.ToLower(strings.TrimSpace(runtime.GetProvider()))
	if provider != "" && provider != "kserve" {
		return fmt.Errorf("runtime provider must be kserve")
	}
	pods := int64(req.GetReplicas())
	container := "kserve-container"
	switch runtime.GetMode() {
	case RuntimeMode_RUNTIME_MODE_UNSPECIFIED, RuntimeMode_RUNTIME_MODE_DEPLOYMENT:
		if runtime.GetWorkerReplicas() > 1 || runtime.GetWorkerReplicas() < 0 {
			return fmt.Errorf("deployment cannot specify multiple workers")
		}
	case RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET:
		if runtime.GetWorkerReplicas() < 1 {
			return fmt.Errorf("LWS worker_replicas must be positive")
		}
		pods *= int64(runtime.GetWorkerReplicas()) + 1
		container = "main"
	default:
		return fmt.Errorf("unsupported runtime mode")
	}
	if endpoint := runtime.GetEndpoint(); endpoint != nil {
		if endpoint.GetContainerPort() < 1 || endpoint.GetContainerPort() > 65535 || endpoint.GetServicePort() < 1 || endpoint.GetServicePort() > 65535 || endpoint.GetTargetPort() == "" {
			return fmt.Errorf("invalid endpoint ports")
		}
		if endpoint.GetProtocol() != "TCP" && endpoint.GetProtocol() != "UDP" && endpoint.GetProtocol() != "SCTP" {
			return fmt.Errorf("unsupported endpoint protocol")
		}
	}
	spec := req.GetResource()
	gpu := spec.GetGpu()
	if gpu != nil {
		if pods > 16 || gpu.GetReplicas() != uint32(pods) || gpu.GetDevicesPerReplica() != 1 || gpu.GetContainerName() != container || gpu.GetProfileVersion() != 1 {
			return fmt.Errorf("GPU selection differs from fixed runtime topology")
		}
		for _, id := range []string{gpu.GetClusterId(), gpu.GetPoolId(), gpu.GetProfileId()} {
			parsed, err := uuid.Parse(id)
			if err != nil || parsed == uuid.Nil || parsed.String() != id {
				return fmt.Errorf("GPU selection IDs must be canonical UUIDs")
			}
		}
	}
	for _, quantities := range []map[string]string{spec.GetRequests(), spec.GetLimits()} {
		for key, raw := range quantities {
			if len(validation.IsQualifiedName(key)) != 0 {
				return fmt.Errorf("invalid resource name %q", key)
			}
			lowered := strings.ToLower(key)
			if gpu != nil && strings.Contains(key, "/") || gpu == nil && (strings.Contains(lowered, "gpu") || strings.Contains(lowered, "gaudi")) {
				return fmt.Errorf("GPU quantities must use resource.gpu")
			}
			parsed, err := resource.ParseQuantity(strings.TrimSpace(raw))
			if err != nil || parsed.Sign() < 0 {
				return fmt.Errorf("invalid resource quantity for %s", key)
			}
		}
	}
	for key, raw := range spec.GetRequests() {
		if limit, exists := spec.GetLimits()[key]; exists {
			requestQuantity, _ := resource.ParseQuantity(strings.TrimSpace(raw))
			limitQuantity, _ := resource.ParseQuantity(strings.TrimSpace(limit))
			if requestQuantity.Cmp(limitQuantity) > 0 {
				return fmt.Errorf("request exceeds limit for %s", key)
			}
			if strings.Contains(key, "/") && !strings.HasPrefix(key, "kubernetes.io/") && requestQuantity.Cmp(limitQuantity) != 0 {
				return fmt.Errorf("extended resource request and limit differ")
			}
		}
	}
	return nil
}
