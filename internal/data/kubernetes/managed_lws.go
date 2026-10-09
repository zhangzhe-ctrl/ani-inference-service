package kubernetes

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	volcanov1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// ProjectManagedGPULeaderWorkerSet projects an accepted frozen plan onto a
// typed LWS before its first API Create. It performs no API writes and leaves
// the supplied object unchanged. A synchronous owner/controller integration
// uses the admission handler for CREATE and UPDATE, including dry-run. This
// standalone entry point remains limited to an object before its first Create.
func ProjectManagedGPULeaderWorkerSet(spec RuntimeSpec, input *lwsv1.LeaderWorkerSet) (*lwsv1.LeaderWorkerSet, error) {
	return projectManagedGPULeaderWorkerSet(spec, input, false)
}

// Admission validates the persisted old UID and owner before allowing the
// same pure projection on UPDATE, including a dry-run UPDATE.
func projectManagedGPULeaderWorkerSet(spec RuntimeSpec, input *lwsv1.LeaderWorkerSet, update bool) (*lwsv1.LeaderWorkerSet, error) {
	if !spec.ManagedGPU || spec.RuntimeMode != "leader_worker_set" {
		return nil, fmt.Errorf("accepted Governance GPU LWS snapshot is required")
	}
	if err := validateGPUPlan(spec); err != nil {
		return nil, err
	}
	if input == nil || !update && (input.GetUID() != "" || input.GetResourceVersion() != "") || input.GetDeletionTimestamp() != nil {
		return nil, fmt.Errorf("LWS projection requires an object before its first Create")
	}
	if input.Namespace != spec.Namespace || input.Name != KServeLLMWorkloadName(spec.Name) || spec.Namespace == "" || spec.Name == "" {
		return nil, fmt.Errorf("LWS name and namespace differ from the frozen runtime")
	}
	if input.Spec.Replicas == nil || *input.Spec.Replicas != spec.Replicas || input.Spec.LeaderWorkerTemplate.Size == nil || *input.Spec.LeaderWorkerTemplate.Size != spec.WorkerReplicas+1 {
		return nil, fmt.Errorf("LWS groups or size differ from the frozen GPU topology")
	}
	if input.Spec.LeaderWorkerTemplate.LeaderTemplate == nil {
		return nil, fmt.Errorf("explicit leader and worker templates are required")
	}
	output := input.DeepCopy()
	allowedResources := map[corev1.ResourceName]bool{}
	for _, item := range spec.GPUPlan.Runtime.LimitsPerContainer {
		allowedResources[corev1.ResourceName(item.Key)] = true
	}
	for _, template := range []*corev1.PodTemplateSpec{output.Spec.LeaderWorkerTemplate.LeaderTemplate, &output.Spec.LeaderWorkerTemplate.WorkerTemplate} {
		for _, container := range template.Spec.InitContainers {
			for _, values := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
				for name := range values {
					key := strings.ToLower(string(name))
					if strings.Contains(key, "gpu") || strings.Contains(key, "gaudi") {
						return nil, fmt.Errorf("GPU resources in init containers are outside the frozen plan")
					}
				}
			}
		}
		for _, container := range template.Spec.Containers {
			for _, values := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
				for name := range values {
					if strings.Contains(string(name), "/") && !allowedResources[name] {
						return nil, fmt.Errorf("container extended resource %q is outside the frozen GPU plan", name)
					}
				}
			}
		}
		annotations, err := applyGPUPlanToPod(spec, &template.Spec, "main")
		if err != nil {
			return nil, err
		}
		if template.Annotations == nil {
			template.Annotations = map[string]string{}
		}
		for key, value := range annotations {
			if current, ok := template.Annotations[key]; ok && current != value {
				return nil, fmt.Errorf("LWS template annotation %q conflicts with the frozen plan", key)
			}
			template.Annotations[key] = value
		}
	}
	if output.Annotations == nil {
		output.Annotations = map[string]string{}
	}
	queue := spec.GPUPlan.Runtime.QueueName
	if current, ok := output.Annotations[volcanov1.QueueNameAnnotationKey]; ok && current != queue {
		return nil, fmt.Errorf("LWS queue annotation conflicts with the frozen plan")
	}
	// LWS v0.10's Volcano provider reads the queue from top-level LWS
	// metadata while HAMI consumes each leader/worker Pod's annotations.
	output.Annotations[volcanov1.QueueNameAnnotationKey] = queue
	return output, nil
}
