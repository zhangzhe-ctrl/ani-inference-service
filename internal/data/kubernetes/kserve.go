package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	bizreconcile "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/reconcile"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	volcanov1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

const (
	KServeInferenceServiceKind          = "KServeInferenceService"
	KServeLLMInferenceServiceKind       = "KServeLLMInferenceService"
	kserveDeploymentMode                = "RawDeployment"
	KServePredictorServicePort    int32 = 80
	KServeLLMWorkloadServicePort  int32 = 8000
)

// KServePredictorServiceName is KServe 0.15's stable predictor Service name
// for an InferenceService without a named predictor. KServe owns this Service;
// ANI only references it from Higress.
func KServePredictorServiceName(name string) string { return name + "-predictor" }

// KServeLLMWorkloadServiceName is the stable Service created by KServe for an
// LLMInferenceService workload. It is the backend for multi-node generation
// traffic; ANI does not create or select the underlying LWS directly.
func KServeLLMWorkloadServiceName(name string) string { return name + "-kserve-workload-svc" }

// KServeRuntimeExecutor implements the existing durable RuntimePort while
// delegating predictor, LWS and workload Service creation to KServe. The
// embedded RuntimeExecutor remains responsible for the ANI InferenceService
// projection and for common source, lease and CR fencing helpers.
type KServeRuntimeExecutor struct {
	*RuntimeExecutor
}

var _ inferencebiz.RuntimePort = (*KServeRuntimeExecutor)(nil)
var _ bizreconcile.Runtime = (*KServeRuntimeExecutor)(nil)

func (e *KServeRuntimeExecutor) ApplyCR(ctx context.Context, op inferencebiz.OperationContext) error {
	if e == nil || e.RuntimeExecutor == nil {
		return errors.New("KServe runtime executor is nil")
	}
	return e.RuntimeExecutor.ApplyCR(ctx, op)
}

func (e *KServeRuntimeExecutor) DeleteCR(ctx context.Context, op inferencebiz.OperationContext) error {
	if e == nil || e.RuntimeExecutor == nil {
		return errors.New("KServe runtime executor is nil")
	}
	return e.RuntimeExecutor.DeleteCR(ctx, op)
}

func (e *KServeRuntimeExecutor) ApplyRuntime(ctx context.Context, op inferencebiz.OperationContext) error {
	if e == nil || e.RuntimeExecutor == nil {
		return errors.New("KServe runtime executor is nil")
	}
	spec, err := e.operationRuntime(ctx, op)
	if err != nil {
		return err
	}
	if e.RequireQuota && !spec.ManagedGPU && !spec.QuotaReserved {
		return errors.New("quota reservation must be confirmed before runtime apply")
	}
	_, err = e.applyKServe(ctx, spec, kserveBindingForSpec(spec))
	return err
}

func (e *KServeRuntimeExecutor) ObserveRuntime(ctx context.Context, op inferencebiz.OperationContext) (inferencebiz.RuntimeObservation, error) {
	if e == nil || e.RuntimeExecutor == nil {
		return inferencebiz.RuntimeObservation{}, errors.New("KServe runtime executor is nil")
	}
	spec, err := e.operationRuntime(ctx, op)
	if err != nil {
		return inferencebiz.RuntimeObservation{}, err
	}
	observation, err := e.observeKServe(ctx, spec, kserveBindingForSpec(spec))
	if err != nil {
		return inferencebiz.RuntimeObservation{}, err
	}
	return inferencebiz.RuntimeObservation{
		Ready: observation.RuntimePhase == "ready", RuntimePhase: observation.RuntimePhase,
		RuntimeMode: runtimeModeForSpec(spec.RuntimeSpec), ReadyReplicas: observation.ReadyReplicas,
		ReadyGroups: observation.ReadyGroups, ReadyWorkers: observation.ReadyWorkers, LWSUID: observation.LWSUID,
		ModelReady: spec.ModelReady, ModelReadyKnown: spec.ModelReadyKnown,
		Objects: observation.Objects, Reason: observation.Reason,
	}, nil
}

func (e *KServeRuntimeExecutor) DeleteRuntime(ctx context.Context, op inferencebiz.OperationContext) error {
	if e == nil || e.RuntimeExecutor == nil {
		return errors.New("KServe runtime executor is nil")
	}
	spec, err := e.operationRuntime(ctx, op)
	if err != nil {
		return err
	}
	if !spec.PublicationWithdrawn {
		return errors.New("publication withdrawal must be confirmed before runtime deletion")
	}
	deleted := false
	for _, binding := range spec.Bindings {
		if binding.Role != "" && binding.Role != "runtime" {
			continue
		}
		if !isKServeBindingKind(binding.Kind) {
			continue
		}
		if err := e.deleteKServe(ctx, binding); err != nil {
			return err
		}
		deleted = true
	}
	if !deleted {
		return errors.New("KServe runtime deletion requires a persisted runtime binding")
	}
	return nil
}

func (e *KServeRuntimeExecutor) ObserveAbsence(ctx context.Context, op inferencebiz.OperationContext) (inferencebiz.RuntimeObservation, error) {
	if e == nil || e.RuntimeExecutor == nil {
		return inferencebiz.RuntimeObservation{}, errors.New("KServe runtime executor is nil")
	}
	spec, err := e.operationRuntime(ctx, op)
	if err != nil {
		return inferencebiz.RuntimeObservation{}, err
	}
	if !spec.PublicationWithdrawn {
		return inferencebiz.RuntimeObservation{}, errors.New("publication withdrawal must be confirmed before runtime absence observation")
	}
	observation, err := e.observeKServeAbsence(ctx, spec)
	if err != nil {
		return inferencebiz.RuntimeObservation{}, err
	}
	return inferencebiz.RuntimeObservation{Absent: observation.RuntimePhase == "stopped", RuntimePhase: observation.RuntimePhase, RuntimeMode: observation.RuntimeMode, Reason: observation.Reason}, nil
}

// Ensure is the continuous reconciler used after an operation becomes
// terminal. It deliberately mirrors the durable operation gates instead of
// falling back to the embedded Deployment implementation.
func (e *KServeRuntimeExecutor) Ensure(ctx context.Context, desired bizreconcile.Desired) (bizreconcile.Observation, error) {
	if e == nil || e.RuntimeExecutor == nil || e.Client == nil {
		return bizreconcile.Observation{}, errors.New("KServe runtime executor is not configured")
	}
	if e.Source == nil || desired.TenantID == "" || desired.ServiceID == "" || desired.Generation < 1 {
		return bizreconcile.Observation{}, errors.New("KServe runtime desired state is incomplete")
	}
	spec, err := e.Source.CurrentRuntime(ctx, desired.TenantID, desired.ServiceID, desired.Generation)
	if err != nil {
		return bizreconcile.Observation{}, err
	}
	if spec.Generation != 0 && spec.Generation != desired.Generation {
		return bizreconcile.Observation{}, bizreconcile.ErrStaleGeneration
	}
	spec.TenantID, spec.ServiceID, spec.Generation = desired.TenantID, desired.ServiceID, desired.Generation
	switch spec.DesiredState {
	case "", "running":
		if e.RequireQuota && !spec.ManagedGPU && !spec.QuotaReserved {
			return bizreconcile.Observation{}, errors.New("quota reservation must be confirmed before runtime apply")
		}
		if err := e.applyCR(ctx, spec.RuntimeSpec, spec.DesiredState, controlBindingForSpec(spec)); err != nil {
			return bizreconcile.Observation{}, err
		}
		observation, err := e.applyKServe(ctx, spec, kserveBindingForSpec(spec))
		if err != nil {
			return bizreconcile.Observation{}, err
		}
		return observation, nil
	case "stopped", "deleted":
		if !spec.PublicationWithdrawn {
			return bizreconcile.Observation{}, errors.New("publication withdrawal must be confirmed before runtime deletion")
		}
		if err := e.applyCR(ctx, spec.RuntimeSpec, spec.DesiredState, controlBindingForSpec(spec)); err != nil {
			return bizreconcile.Observation{}, err
		}
		bindings := kserveBindings(spec)
		if len(bindings) == 0 {
			if spec.RuntimePhase == "stopped" {
				return bizreconcile.Observation{Generation: desired.Generation, RuntimeMode: runtimeModeForSpec(spec.RuntimeSpec), RuntimePhase: "stopped"}, nil
			}
			return bizreconcile.Observation{}, errors.New("KServe runtime deletion requires a persisted runtime binding")
		}
		for _, binding := range bindings {
			if err := e.deleteKServe(ctx, binding); err != nil {
				return bizreconcile.Observation{}, err
			}
		}
		return e.observeKServeAbsence(ctx, spec)
	default:
		return bizreconcile.Observation{}, fmt.Errorf("unsupported desired state %q", spec.DesiredState)
	}
}

func (e *KServeRuntimeExecutor) applyKServe(ctx context.Context, spec DesiredRuntime, expected *RuntimeBinding) (bizreconcile.Observation, error) {
	obj, err := RenderKServeRuntime(spec.RuntimeSpec)
	if err != nil {
		return bizreconcile.Observation{}, err
	}
	reader := client.Reader(e.Client)
	if e.APIReader != nil {
		reader = e.APIReader
	}
	kind := kserveKindForMode(spec.RuntimeMode)
	current := newKServeObject(spec.Namespace, spec.Name, kind)
	if err := reader.Get(ctx, client.ObjectKeyFromObject(current), current); err == nil {
		if !OwnedBy(current, spec.TenantID, spec.ServiceID) {
			return bizreconcile.Observation{}, errors.New("KServe runtime object is occupied by a different tenant or service")
		}
		if generation, parseErr := strconv.ParseInt(current.GetLabels()[generationLabel], 10, 64); parseErr != nil || generation > spec.Generation {
			return bizreconcile.Observation{}, bizreconcile.ErrStaleGeneration
		}
		// KServe updates status asynchronously, so resourceVersion is expected
		// to change after the first apply. UID plus our durable generation label
		// is the ownership fence here; the current RV is copied into the SSA
		// object before the next spec apply.
		if expected == nil || expected.UID != string(current.GetUID()) {
			return bizreconcile.Observation{}, bizreconcile.ErrStaleGeneration
		}
		if current.GetDeletionTimestamp() != nil {
			return bizreconcile.Observation{}, errors.New("KServe runtime object deletion is still in progress")
		}
		obj.SetResourceVersion(current.GetResourceVersion())
	} else if !apierrors.IsNotFound(err) {
		return bizreconcile.Observation{}, err
	}
	if err := e.Client.Patch(ctx, obj, client.Apply, client.FieldOwner(ownerForRuntime(e.FieldManager))); err != nil {
		return bizreconcile.Observation{}, err
	}
	observed := newKServeObject(spec.Namespace, spec.Name, kind)
	if err := reader.Get(ctx, client.ObjectKeyFromObject(obj), observed); err != nil {
		return bizreconcile.Observation{}, err
	}
	if e.Bindings != nil {
		var expectedUID, expectedResourceVersion string
		if expected != nil {
			expectedUID, expectedResourceVersion = expected.UID, expected.ResourceVersion
		}
		if err := e.Bindings.UpsertRuntimeBinding(ctx, BindingRecord{
			TenantID: spec.TenantID, ServiceID: spec.ServiceID, Generation: spec.Generation,
			Kind: kind, Namespace: observed.GetNamespace(), Name: observed.GetName(),
			UID: string(observed.GetUID()), ResourceVersion: observed.GetResourceVersion(), Role: "runtime",
			ExpectedUID: expectedUID, ExpectedResourceVersion: expectedResourceVersion,
		}); err != nil {
			return bizreconcile.Observation{}, err
		}
	}
	return e.observeKServe(ctx, spec, &RuntimeBinding{Kind: kind, Namespace: observed.GetNamespace(), Name: observed.GetName(), UID: string(observed.GetUID()), ResourceVersion: observed.GetResourceVersion(), Role: "runtime", Generation: spec.Generation})
}

func (e *KServeRuntimeExecutor) observeKServe(ctx context.Context, spec DesiredRuntime, expected *RuntimeBinding) (bizreconcile.Observation, error) {
	kind := kserveKindForMode(spec.RuntimeMode)
	obj := newKServeObject(spec.Namespace, spec.Name, kind)
	observation := bizreconcile.Observation{Generation: spec.Generation, RuntimeMode: runtimeModeForSpec(spec.RuntimeSpec), PublicationPhase: "unknown", InvocationHealth: "unknown"}
	reader := client.Reader(e.Client)
	if e.APIReader != nil {
		reader = e.APIReader
	}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			observation.RuntimePhase = "degraded"
			observation.Reason = "KServe runtime object is missing"
			return observation, nil
		}
		return bizreconcile.Observation{}, err
	}
	fact, err := validateKServeObject(obj, spec.RuntimeSpec, expected)
	if err != nil {
		return bizreconcile.Observation{}, err
	}
	observation.Objects = []bizreconcile.RuntimeObject{fact}
	if obj.GetDeletionTimestamp() != nil {
		observation.RuntimePhase = "degraded"
		observation.Reason = "KServe runtime object is being deleted"
		return observation, nil
	}
	ready, reason := kserveReady(obj)
	if ready && kind == KServeInferenceServiceKind {
		ready, reason = e.kservePredictorReady(ctx, reader, obj, spec.Replicas)
	}
	if ready && kind == KServeLLMInferenceServiceKind {
		var readyGroups, readyWorkers int32
		var lwsUID string
		ready, reason, readyGroups, readyWorkers, lwsUID = e.kserveLLMReady(ctx, reader, obj, spec)
		observation.ReadyGroups = readyGroups
		observation.ReadyWorkers = readyWorkers
		observation.LWSUID = lwsUID
		observation.ReadyReplicas = readyGroups
	}
	if ready {
		observation.RuntimePhase = "ready"
		if kind == KServeInferenceServiceKind {
			observation.ReadyReplicas = spec.Replicas
		}
		return observation, nil
	}
	observation.RuntimePhase = "pending"
	observation.Reason = reason
	return observation, nil
}

// RenderKServeRuntime exposes the same frozen-spec renderer used by the
// runtime executor for owner handoff and deterministic projection checks.
func RenderKServeRuntime(spec RuntimeSpec) (*unstructured.Unstructured, error) {
	switch spec.RuntimeMode {
	case "", "deployment":
		return renderKServeInferenceService(spec)
	case "leader_worker_set":
		return renderKServeLLMInferenceService(spec)
	default:
		return nil, fmt.Errorf("unsupported KServe runtime mode %q", spec.RuntimeMode)
	}
}

// validateGPUPlan enforces the accelerator boundary at the rendering edge.
// The request is selected during admission and the plan is immutable after
// resolution; a renderer must never infer GPU settings from a raw quantity or
// silently fall back to a CPU pod when the plan is missing.
func validateGPUPlan(spec RuntimeSpec) error {
	request := spec.Resources.GPU
	if spec.ManagedGPU {
		if err := gpu.ValidateManagedPlan(spec.GPUPlan, request); err != nil {
			return fmt.Errorf("invalid accepted Governance GPU plan: %w", err)
		}
	}
	if request == nil {
		if spec.GPUPlan != nil {
			return errors.New("GPU plan requires a GPU request")
		}
		return nil
	}
	if spec.GPUPlan == nil {
		return errors.New("GPU request requires a frozen GPU plan")
	}
	if err := gpu.ValidatePlan(spec.GPUPlan, request); err != nil {
		return fmt.Errorf("invalid GPU plan: %w", err)
	}
	if err := gpu.ValidateTopology(request, spec.Replicas, spec.RuntimeMode, spec.WorkerReplicas); err != nil {
		return fmt.Errorf("invalid GPU topology: %w", err)
	}
	if spec.GPUPlan.Runtime == nil {
		return errors.New("GPU plan runtime fragment is required")
	}
	return nil
}

// applyGPUPlanToPod projects only the frozen runtime fragment. It deliberately
// does not add a command, args, model flag, or any other engine setting.
func applyGPUPlanToPod(spec RuntimeSpec, pod *corev1.PodSpec, expectedContainer string) (map[string]string, error) {
	if err := validateGPUPlan(spec); err != nil {
		return nil, err
	}
	if spec.Resources.GPU == nil {
		return nil, nil
	}
	if pod == nil || len(pod.Containers) != 1 || pod.Containers[0].Name != expectedContainer {
		return nil, fmt.Errorf("GPU plan container must be %q", expectedContainer)
	}
	runtimeFragment := spec.GPUPlan.Runtime
	if pod.SchedulerName != "" && pod.SchedulerName != runtimeFragment.SchedulerName {
		return nil, errors.New("GPU plan conflicts with pod scheduler")
	}
	pod.SchedulerName = runtimeFragment.SchedulerName
	if pod.NodeSelector == nil && len(runtimeFragment.NodeLabels) > 0 {
		pod.NodeSelector = make(map[string]string, len(runtimeFragment.NodeLabels))
	}
	for _, item := range runtimeFragment.NodeLabels {
		if strings.TrimSpace(item.Key) == "" || strings.TrimSpace(item.Value) == "" {
			return nil, errors.New("GPU plan node label key and value are required")
		}
		if previous, exists := pod.NodeSelector[item.Key]; exists && previous != item.Value {
			return nil, fmt.Errorf("GPU plan node label %q has conflicting values", item.Key)
		}
		pod.NodeSelector[item.Key] = item.Value
	}
	if runtimeFragment.RuntimeClassName != "" {
		if pod.RuntimeClassName != nil && *pod.RuntimeClassName != runtimeFragment.RuntimeClassName {
			return nil, errors.New("GPU plan conflicts with pod runtime class")
		}
		pod.RuntimeClassName = &runtimeFragment.RuntimeClassName
	}
	container := &pod.Containers[0]
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	for _, item := range runtimeFragment.LimitsPerContainer {
		if strings.TrimSpace(item.Key) == "" || strings.TrimSpace(item.Value) == "" {
			return nil, errors.New("GPU plan container limit key and value are required")
		}
		quantity, err := resource.ParseQuantity(item.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid GPU plan limit %q for %s: %w", item.Value, item.Key, err)
		}
		key := corev1.ResourceName(item.Key)
		if current, ok := container.Resources.Limits[key]; ok && current.Cmp(quantity) != 0 {
			return nil, fmt.Errorf("GPU plan conflicts with existing container limit %q", item.Key)
		}
		if current, ok := container.Resources.Requests[key]; ok && current.Cmp(quantity) != 0 {
			return nil, fmt.Errorf("GPU plan conflicts with existing container request %q", item.Key)
		}
		container.Resources.Limits[key] = quantity
	}
	annotations := make(map[string]string, len(runtimeFragment.PodAnnotations))
	for _, item := range runtimeFragment.PodAnnotations {
		if strings.TrimSpace(item.Key) == "" || strings.TrimSpace(item.Value) == "" {
			return nil, errors.New("GPU plan pod annotation key and value are required")
		}
		if previous, exists := annotations[item.Key]; exists && previous != item.Value {
			return nil, fmt.Errorf("GPU plan pod annotation %q has conflicting values", item.Key)
		}
		annotations[item.Key] = item.Value
	}
	// Volcano v1.12.1's normal-Pod PodGroup controller reads this exact pod
	// annotation into PodGroup.spec.queue. Queue is frozen in the same plan.
	if runtimeFragment.QueueName == "" {
		return nil, errors.New("GPU plan queue is required")
	}
	if value, exists := annotations[volcanov1.QueueNameAnnotationKey]; exists && value != runtimeFragment.QueueName {
		return nil, errors.New("GPU queue annotation conflicts with frozen plan")
	}
	annotations[volcanov1.QueueNameAnnotationKey] = runtimeFragment.QueueName
	return annotations, nil
}

func applyGPUPlanToPredictor(predictor map[string]interface{}, pod *corev1.PodSpec, annotations map[string]string) {
	if pod.SchedulerName != "" {
		predictor["schedulerName"] = pod.SchedulerName
	}
	if len(pod.NodeSelector) > 0 {
		selector := make(map[string]interface{}, len(pod.NodeSelector))
		for key, value := range pod.NodeSelector {
			selector[key] = value
		}
		predictor["nodeSelector"] = selector
	}
	if pod.RuntimeClassName != nil && *pod.RuntimeClassName != "" {
		predictor["runtimeClassName"] = *pod.RuntimeClassName
	}
	if len(annotations) > 0 {
		// PredictorSpec embeds ComponentExtensionSpec, whose annotations are
		// copied to the generated predictor Pod template by KServe.
		predictor["annotations"] = mapStringInterface(annotations)
	}
}

func mapStringInterface(values map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func renderKServeInferenceService(spec RuntimeSpec) (*unstructured.Unstructured, error) {
	spec = normalizeEndpoint(spec)
	if spec.Name == "" || spec.Namespace == "" || spec.ServiceID == "" || spec.TenantID == "" || spec.Image == "" {
		return nil, errors.New("name, namespace, tenant ID, service ID and image are required")
	}
	if len(KServePredictorServiceName(spec.Name)) > 63 {
		return nil, errors.New("KServe predictor Service name exceeds 63 characters")
	}
	if spec.Generation < 1 || spec.Replicas < 1 {
		return nil, errors.New("generation and replicas must be positive")
	}
	if err := validateGPUPlan(spec); err != nil {
		return nil, err
	}
	requirements, err := resourceRequirements(spec.Resources)
	if err != nil {
		return nil, err
	}
	container := corev1.Container{Name: "kserve-container", Image: spec.Image, Command: append([]string(nil), spec.CommandArgv...), Resources: requirements}
	if spec.ContainerPort > 0 {
		if !validPort(spec.ContainerPort) || !validProtocol(spec.ServiceProtocol) {
			return nil, errors.New("container port requires a valid port and explicit protocol")
		}
		container.Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: spec.ContainerPort, Protocol: spec.ServiceProtocol}}
	}
	pod := corev1.PodSpec{Containers: []corev1.Container{container}}
	if err := mountModel(&pod, spec); err != nil {
		return nil, err
	}
	gpuAnnotations, err := applyGPUPlanToPod(spec, &pod, "kserve-container")
	if err != nil {
		return nil, err
	}
	containerMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod.Containers[0])
	if err != nil {
		return nil, fmt.Errorf("convert KServe container: %w", err)
	}
	predictor := map[string]interface{}{"containers": []interface{}{containerMap}, "minReplicas": int64(spec.Replicas), "maxReplicas": int64(spec.Replicas)}
	applyGPUPlanToPredictor(predictor, &pod, gpuAnnotations)
	if len(pod.Volumes) > 0 {
		volumes := make([]interface{}, 0, len(pod.Volumes))
		for i := range pod.Volumes {
			volumeMap, convertErr := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod.Volumes[i])
			if convertErr != nil {
				return nil, fmt.Errorf("convert KServe volume: %w", convertErr)
			}
			volumes = append(volumes, volumeMap)
		}
		predictor["volumes"] = volumes
	}
	if pod.AutomountServiceAccountToken != nil {
		predictor["automountServiceAccountToken"] = *pod.AutomountServiceAccountToken
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "serving.kserve.io/v1beta1",
		"kind":       "InferenceService",
		"metadata": map[string]interface{}{
			"name":      spec.Name,
			"namespace": spec.Namespace,
			"labels": map[string]interface{}{
				"app.kubernetes.io/name":      "ani-inference",
				tenantIDLabel:                 spec.TenantID,
				serviceIDLabel:                spec.ServiceID,
				generationLabel:               strconv.FormatInt(spec.Generation, 10),
				"ani.kubercloud.com/provider": "kserve",
			},
			"annotations": map[string]interface{}{
				"serving.kserve.io/deploymentMode": kserveDeploymentMode,
				// KServe 0.15.0 accepts the external autoscaler class. The
				// "none" class was added after this cluster's KServe release.
				"serving.kserve.io/autoscalerClass": "external",
				generationLabel:                     strconv.FormatInt(spec.Generation, 10),
			},
		},
		"spec": map[string]interface{}{"predictor": predictor},
	}}, nil
}

func renderKServeLLMInferenceService(spec RuntimeSpec) (*unstructured.Unstructured, error) {
	spec = normalizeEndpoint(spec)
	if spec.Name == "" || spec.Namespace == "" || spec.ServiceID == "" || spec.TenantID == "" || spec.Image == "" {
		return nil, errors.New("name, namespace, tenant ID, service ID and image are required")
	}
	if len(KServeLLMWorkloadServiceName(spec.Name)) > 63 {
		return nil, errors.New("KServe workload Service name exceeds 63 characters")
	}
	if spec.Generation < 1 || spec.Replicas < 1 {
		return nil, errors.New("generation and replicas must be positive")
	}
	if err := validateGPUPlan(spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ModelClaim) == "" {
		return nil, errors.New("LLMInferenceService requires a materialized model PVC")
	}
	if spec.ContainerPort != 0 && spec.ContainerPort != KServeLLMWorkloadServicePort {
		return nil, fmt.Errorf("LLMInferenceService requires container port %d", KServeLLMWorkloadServicePort)
	}
	if spec.ContainerPort > 0 && spec.ServiceProtocol != corev1.ProtocolTCP {
		return nil, errors.New("LLMInferenceService requires TCP protocol")
	}
	requirements, err := resourceRequirements(spec.Resources)
	if err != nil {
		return nil, err
	}
	container := corev1.Container{Name: "main", Image: spec.Image, Command: append([]string(nil), spec.CommandArgv...), Args: []string{}, Resources: requirements}
	if spec.ContainerPort > 0 {
		if !validPort(spec.ContainerPort) || !validProtocol(spec.ServiceProtocol) {
			return nil, errors.New("container port requires a valid port and explicit protocol")
		}
		container.Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: spec.ContainerPort, Protocol: spec.ServiceProtocol}}
	}
	// Keep the model PVC and shared-memory mount identical to the single-node
	// KServe path when an endpoint is fully specified. A request without an
	// endpoint is still renderable; KServe receives the model URI below and the
	// caller-provided container remains untouched.
	pod := corev1.PodSpec{Containers: []corev1.Container{container}}
	if spec.ContainerPort > 0 {
		if err := mountModel(&pod, spec); err != nil {
			return nil, err
		}
	} else if spec.ModelClaim != "" {
		if err := mountModelVolume(&pod, spec, false); err != nil {
			return nil, err
		}
	}
	_, err = applyGPUPlanToPod(spec, &pod, "main")
	if err != nil {
		return nil, err
	}
	containerMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod.Containers[0])
	if err != nil {
		return nil, fmt.Errorf("convert LLMInferenceService container: %w", err)
	}
	// The built-in KServe config is merged into this container. An explicit
	// empty args list prevents that base config from silently appending engine
	// flags to the caller-provided command.
	containerMap["args"] = []interface{}{}
	workerContainer := map[string]interface{}{}
	for key, value := range containerMap {
		workerContainer[key] = value
	}
	metadata := map[string]interface{}{
		"name":      spec.Name,
		"namespace": spec.Namespace,
		"labels": map[string]interface{}{
			"app.kubernetes.io/name":      "ani-inference",
			tenantIDLabel:                 spec.TenantID,
			serviceIDLabel:                spec.ServiceID,
			generationLabel:               strconv.FormatInt(spec.Generation, 10),
			"ani.kubercloud.com/provider": "kserve",
		},
		"annotations": map[string]interface{}{generationLabel: strconv.FormatInt(spec.Generation, 10)},
	}
	serviceSpec := map[string]interface{}{
		"model": map[string]interface{}{
			// The materializer stores the verified artifact below the PVC's
			// data subPath. Keep that path in the KServe URI so its model
			// volume points at the artifact rather than the PVC root.
			"uri":  "pvc://" + spec.ModelClaim + "/data",
			"name": firstNonEmpty(spec.ServedModelName, spec.Name),
		},
		"replicas": int64(spec.Replicas),
		"template": map[string]interface{}{"containers": []interface{}{containerMap}},
		// An explicit worker section is what makes KServe create the native
		// LeaderWorkerSet. The command is copied exactly from the request.
		"worker": map[string]interface{}{"containers": []interface{}{workerContainer}},
	}
	applyGPUPlanToPredictor(serviceSpec["template"].(map[string]interface{}), &pod, nil)
	applyGPUPlanToPredictor(serviceSpec["worker"].(map[string]interface{}), &pod, nil)
	// KServe v0.16 requires parallelism whenever worker is present. The
	// existing API models WorkerReplicas as workers excluding the leader, so a
	// pipeline-parallel group of worker+leader pods preserves that topology.
	// The cluster must install the empty well-known pipeline config so KServe
	// does not inject an engine command; the caller's command remains untouched.
	workers := spec.WorkerReplicas
	if workers < 1 {
		workers = 1
	}
	serviceSpec["parallelism"] = map[string]interface{}{
		"pipeline": int64(workers + 1),
	}
	if len(pod.Volumes) > 0 {
		volumes := make([]interface{}, 0, len(pod.Volumes))
		for i := range pod.Volumes {
			volumeMap, convertErr := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod.Volumes[i])
			if convertErr != nil {
				return nil, fmt.Errorf("convert LLMInferenceService volume: %w", convertErr)
			}
			volumes = append(volumes, volumeMap)
		}
		serviceSpec["template"].(map[string]interface{})["volumes"] = volumes
		serviceSpec["worker"].(map[string]interface{})["volumes"] = volumes
	}
	if pod.AutomountServiceAccountToken != nil {
		serviceSpec["template"].(map[string]interface{})["automountServiceAccountToken"] = *pod.AutomountServiceAccountToken
		serviceSpec["worker"].(map[string]interface{})["automountServiceAccountToken"] = *pod.AutomountServiceAccountToken
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "serving.kserve.io/v1alpha1",
		"kind":       "LLMInferenceService",
		"metadata":   metadata,
		"spec":       serviceSpec,
	}}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func newKServeInferenceService(namespace, name string) *unstructured.Unstructured {
	return newKServeObject(namespace, name, KServeInferenceServiceKind)
}

func newKServeLLMInferenceService(namespace, name string) *unstructured.Unstructured {
	return newKServeObject(namespace, name, KServeLLMInferenceServiceKind)
}

func newKServeObject(namespace, name, kind string) *unstructured.Unstructured {
	apiVersion, apiKind := "serving.kserve.io/v1beta1", "InferenceService"
	if kind == KServeLLMInferenceServiceKind {
		apiVersion, apiKind = "serving.kserve.io/v1alpha1", "LLMInferenceService"
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": apiVersion, "kind": apiKind, "metadata": map[string]interface{}{}}}
	version := strings.TrimPrefix(apiVersion, "serving.kserve.io/")
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "serving.kserve.io", Version: version, Kind: apiKind})
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}

func kserveKindForMode(mode string) string {
	if mode == "leader_worker_set" {
		return KServeLLMInferenceServiceKind
	}
	return KServeInferenceServiceKind
}

func isKServeBindingKind(kind string) bool {
	return kind == KServeInferenceServiceKind || kind == KServeLLMInferenceServiceKind
}

func kserveBindings(spec DesiredRuntime) []RuntimeBinding {
	bindings := make([]RuntimeBinding, 0, 1)
	for _, binding := range spec.Bindings {
		if isKServeBindingKind(binding.Kind) && (binding.Role == "" || binding.Role == "runtime") {
			bindings = append(bindings, binding)
		}
	}
	return bindings
}

func kserveBindingForSpec(spec DesiredRuntime) *RuntimeBinding {
	for _, binding := range kserveBindings(spec) {
		bindingCopy := binding
		return &bindingCopy
	}
	return nil
}

func (e *KServeRuntimeExecutor) deleteKServe(ctx context.Context, binding RuntimeBinding) error {
	if binding.Namespace == "" || binding.Name == "" || binding.UID == "" || binding.ResourceVersion == "" {
		return errors.New("KServe runtime binding requires namespace, name, UID and resourceVersion")
	}
	reader := client.Reader(e.Client)
	if e.APIReader != nil {
		reader = e.APIReader
	}
	obj := newKServeObject(binding.Namespace, binding.Name, binding.Kind)
	if err := reader.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if string(obj.GetUID()) != binding.UID {
		return bizreconcile.ErrStaleGeneration
	}
	if obj.GetDeletionTimestamp() != nil {
		return nil
	}
	uid, rv := types.UID(obj.GetUID()), obj.GetResourceVersion()
	if err := e.Client.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &rv}, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (e *KServeRuntimeExecutor) observeKServeAbsence(ctx context.Context, spec DesiredRuntime) (bizreconcile.Observation, error) {
	bindings := kserveBindings(spec)
	if len(bindings) == 0 {
		return bizreconcile.Observation{}, errors.New("KServe runtime absence requires a persisted runtime binding")
	}
	reader := client.Reader(e.Client)
	if e.APIReader != nil {
		reader = e.APIReader
	}
	for _, binding := range bindings {
		obj := newKServeObject(binding.Namespace, binding.Name, binding.Kind)
		if err := reader.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			return bizreconcile.Observation{Generation: spec.Generation, RuntimeMode: runtimeModeForSpec(spec.RuntimeSpec), RuntimePhase: "degraded", Reason: "KServe runtime object deletion pending"}, nil
		} else if !apierrors.IsNotFound(err) {
			return bizreconcile.Observation{}, err
		}
	}
	return bizreconcile.Observation{Generation: spec.Generation, RuntimeMode: runtimeModeForSpec(spec.RuntimeSpec), RuntimePhase: "stopped"}, nil
}

func (e *KServeRuntimeExecutor) kservePredictorReady(ctx context.Context, reader client.Reader, obj *unstructured.Unstructured, replicas int32) (bool, string) {
	deployment := &appsv1.Deployment{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: KServePredictorServiceName(obj.GetName())}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "KServe predictor Deployment is not available"
		}
		return false, err.Error()
	}
	owned := false
	for _, owner := range deployment.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller && owner.UID == obj.GetUID() && owner.Kind == "InferenceService" {
			owned = true
			break
		}
	}
	if !owned {
		return false, "KServe predictor Deployment is not owned by this InferenceService"
	}
	// Ready can still describe the previous predictor while KServe is applying
	// a new spec. Compare the desired container before trusting replica counts;
	// Kubernetes defaults may fill fields that the caller did not specify.
	predictor, found, err := unstructured.NestedMap(obj.Object, "spec", "predictor")
	if err != nil || !found {
		return false, "KServe predictor spec is not available"
	}
	containers, found, err := unstructured.NestedSlice(predictor, "containers")
	if err != nil || !found || len(containers) != 1 {
		return false, "KServe predictor container spec is not available"
	}
	containerMap, ok := containers[0].(map[string]interface{})
	if !ok {
		return false, "KServe predictor container spec is invalid"
	}
	var expected corev1.Container
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(containerMap, &expected); err != nil {
		return false, "KServe predictor container spec is invalid"
	}
	matched := false
	for _, actual := range deployment.Spec.Template.Spec.Containers {
		if actual.Name == expected.Name && actual.Image == expected.Image && slices.Equal(expected.Command, actual.Command) && slices.Equal(expected.Args, actual.Args) && apiequality.Semantic.DeepEqual(expected.Resources, actual.Resources) && apiequality.Semantic.DeepEqual(expected.Ports, actual.Ports) && apiequality.Semantic.DeepEqual(expected.Env, actual.Env) && apiequality.Semantic.DeepEqual(expected.VolumeMounts, actual.VolumeMounts) {
			matched = true
			break
		}
	}
	if !matched {
		return false, "KServe predictor Deployment has not applied the current container spec"
	}
	if !kservePodPlacementMatches(predictor, &deployment.Spec.Template.Spec, deployment.Spec.Template.Annotations) {
		return false, "KServe predictor Deployment has not applied the current GPU scheduling spec"
	}
	if deployment.Generation < 1 || deployment.Status.ObservedGeneration < deployment.Generation {
		return false, "KServe predictor Deployment status is stale"
	}
	// KServe 0.15 reports the predictor Deployment's observed generation in
	// InferenceService status. It may legitimately lag the CR metadata
	// generation while that predictor revision is healthy.
	observedGeneration, found, err := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
	if err != nil || !found || observedGeneration < 1 || observedGeneration < deployment.Status.ObservedGeneration {
		return false, "KServe status is stale"
	}
	if replicas < 1 {
		replicas = 1
	}
	if deployment.Status.UpdatedReplicas < replicas || deployment.Status.AvailableReplicas < replicas || deployment.Status.ReadyReplicas < replicas {
		return false, "KServe predictor Deployment is not ready"
	}
	return true, ""
}

// kserveLLMReady verifies the complete LLMI -> LWS -> Pod chain. LLMI's
// Ready condition alone is not sufficient: it can describe an old LWS revision
// while the new LLMInferenceService is still being propagated.
func (e *KServeRuntimeExecutor) kserveLLMReady(ctx context.Context, reader client.Reader, obj *unstructured.Unstructured, spec DesiredRuntime) (bool, string, int32, int32, string) {
	// KServe 0.16 LLMI status may omit observedGeneration, so treat a
	// missing field as unknown rather than stale. When it is present, it must
	// cover the current LLMI generation before trusting the LWS status.
	if observedGeneration, found, err := unstructured.NestedInt64(obj.Object, "status", "observedGeneration"); err != nil || (found && observedGeneration < obj.GetGeneration()) {
		return false, "KServe LLMInferenceService status is stale", 0, 0, ""
	}
	lws := &lwsv1.LeaderWorkerSet{}
	lws.SetNamespace(obj.GetNamespace())
	lws.SetName(KServeLLMWorkloadName(spec.Name))
	if err := reader.Get(ctx, client.ObjectKeyFromObject(lws), lws); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "KServe LeaderWorkerSet is not available", 0, 0, ""
		}
		return false, err.Error(), 0, 0, ""
	}
	owned := false
	for _, owner := range lws.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller && owner.UID == obj.GetUID() && owner.Kind == "LLMInferenceService" {
			owned = true
			break
		}
	}
	if !owned {
		return false, "KServe LeaderWorkerSet is not owned by this LLMInferenceService", 0, 0, string(lws.UID)
	}
	if lws.Generation < 1 || lws.Status.ObservedGeneration < lws.Generation {
		return false, "KServe LeaderWorkerSet status is stale", 0, 0, string(lws.UID)
	}
	desiredGroups := spec.Replicas
	if lws.Spec.Replicas == nil || *lws.Spec.Replicas != desiredGroups {
		return false, "KServe LeaderWorkerSet replica spec is stale", 0, 0, string(lws.UID)
	}
	desiredSize := spec.WorkerReplicas + 1
	if desiredSize < 2 {
		desiredSize = 2
	}
	if lws.Spec.LeaderWorkerTemplate.Size == nil || *lws.Spec.LeaderWorkerTemplate.Size != desiredSize {
		return false, "KServe LeaderWorkerSet parallelism is stale", 0, 0, string(lws.UID)
	}
	if !kserveLLMTemplatesMatch(obj, lws) {
		return false, "KServe LeaderWorkerSet template has not applied the current container spec", 0, 0, string(lws.UID)
	}
	if lws.Status.UpdatedReplicas < desiredGroups {
		return false, "KServe LeaderWorkerSet is not ready", 0, 0, string(lws.UID)
	}
	readyWorkers, err := e.readyKServeLWSWorkers(ctx, reader, lws, desiredGroups, desiredSize-1)
	if err != nil {
		return false, err.Error(), 0, 0, string(lws.UID)
	}
	desiredWorkers := desiredGroups * (desiredSize - 1)
	if lws.Status.ReadyReplicas < desiredGroups {
		return false, "KServe LeaderWorkerSet is not ready", lws.Status.ReadyReplicas, readyWorkers, string(lws.UID)
	}
	if readyWorkers < desiredWorkers {
		return false, "KServe LeaderWorkerSet workers are not ready", lws.Status.ReadyReplicas, readyWorkers, string(lws.UID)
	}
	return true, "", lws.Status.ReadyReplicas, readyWorkers, string(lws.UID)
}

// KServe 0.16 names the multi-node child exactly this way. Model names are
// bounded by the workload Service validation, so the child also remains a
// valid Kubernetes name.
func KServeLLMWorkloadName(name string) string { return name + "-kserve-mn" }

func kserveLLMTemplatesMatch(obj *unstructured.Unstructured, lws *lwsv1.LeaderWorkerSet) bool {
	workloadAnnotations, _, err := unstructured.NestedStringMap(obj.Object, "spec", "annotations")
	if err != nil {
		return false
	}
	for _, path := range [][]string{{"spec", "template", "containers"}, {"spec", "worker", "containers"}} {
		template, found, err := unstructured.NestedMap(obj.Object, path[:2]...)
		if err != nil || !found {
			return false
		}
		containers, found, err := unstructured.NestedSlice(obj.Object, path...)
		if err != nil || !found || len(containers) != 1 {
			return false
		}
		m, ok := containers[0].(map[string]interface{})
		if !ok {
			return false
		}
		var expected corev1.Container
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &expected); err != nil {
			return false
		}
		var actual corev1.Container
		if strings.HasSuffix(path[1], "worker") {
			if len(lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers) != 1 {
				return false
			}
			actual = lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0]
		} else {
			if lws.Spec.LeaderWorkerTemplate.LeaderTemplate == nil || len(lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Containers) != 1 {
				return false
			}
			actual = lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Containers[0]
		}
		if !kserveContainerMatches(expected, actual) {
			return false
		}
		var actualTemplate *corev1.PodTemplateSpec
		if strings.HasSuffix(path[1], "worker") {
			actualTemplate = &lws.Spec.LeaderWorkerTemplate.WorkerTemplate
		} else {
			actualTemplate = lws.Spec.LeaderWorkerTemplate.LeaderTemplate
		}
		if actualTemplate == nil || !kservePodPlacementMatches(template, &actualTemplate.Spec, actualTemplate.Annotations) {
			return false
		}
		for key, value := range workloadAnnotations {
			if actualTemplate.Annotations[key] != value {
				return false
			}
		}
	}
	return true
}

// kservePodPlacementMatches checks the scheduling fields that ANI owns in the
// KServe pod template. KServe and its child controllers may add defaults and
// environment variables, but they must not drop an accelerator scheduler,
// node label, runtime class, or pod annotation from the frozen plan.
func kservePodPlacementMatches(template map[string]interface{}, actual *corev1.PodSpec, actualAnnotations map[string]string) bool {
	if actual == nil {
		return false
	}
	if scheduler, found, _ := unstructured.NestedString(template, "schedulerName"); found && scheduler != actual.SchedulerName {
		return false
	}
	if selector, found, err := unstructured.NestedStringMap(template, "nodeSelector"); err != nil {
		return false
	} else if found {
		for key, value := range selector {
			if actual.NodeSelector[key] != value {
				return false
			}
		}
	}
	if runtimeClass, found, _ := unstructured.NestedString(template, "runtimeClassName"); found {
		if actual.RuntimeClassName == nil || *actual.RuntimeClassName != runtimeClass {
			return false
		}
	}
	annotations, found, err := unstructured.NestedStringMap(template, "annotations")
	if err != nil {
		return false
	}
	if found {
		for key, value := range annotations {
			if actualAnnotations[key] != value {
				return false
			}
		}
	}
	return true
}

func (e *KServeRuntimeExecutor) readyKServeLWSWorkers(ctx context.Context, reader client.Reader, lws *lwsv1.LeaderWorkerSet, groups, workers int32) (int32, error) {
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(lws.Namespace), client.MatchingLabels{lwsv1.SetNameLabelKey: lws.Name}); err != nil {
		return 0, err
	}
	if len(lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers) != 1 {
		return 0, errors.New("KServe LeaderWorkerSet worker template is invalid")
	}
	expected := lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0]
	ready := make(map[[2]int64]struct{})
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || len(pod.Spec.Containers) != 1 {
			continue
		}
		worker, err := strconv.ParseInt(pod.Labels[lwsv1.WorkerIndexLabelKey], 10, 32)
		if err != nil || worker < 1 || worker > int64(workers) || !kserveContainerMatches(expected, pod.Spec.Containers[0]) {
			continue
		}
		group, err := strconv.ParseInt(pod.Labels[lwsv1.GroupIndexLabelKey], 10, 32)
		if err != nil || group < 0 || group >= int64(groups) {
			continue
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready[[2]int64{group, worker}] = struct{}{}
				break
			}
		}
	}
	return int32(len(ready)), nil
}

// kserveContainerMatches compares the fields ANI sends to KServe while
// allowing KServe and the LeaderWorkerSet controller to add their own
// environment variables, model mount and Kubernetes defaults. The command,
// image, resources and ports remain caller-owned and must match exactly.
func kserveContainerMatches(expected, actual corev1.Container) bool {
	if expected.Name != actual.Name || expected.Image != actual.Image ||
		!slices.Equal(expected.Command, actual.Command) || !slices.Equal(expected.Args, actual.Args) ||
		!apiequality.Semantic.DeepEqual(expected.Resources, actual.Resources) ||
		!apiequality.Semantic.DeepEqual(expected.Ports, actual.Ports) {
		return false
	}
	for _, env := range expected.Env {
		if !slices.ContainsFunc(actual.Env, func(candidate corev1.EnvVar) bool {
			return candidate.Name == env.Name && apiequality.Semantic.DeepEqual(candidate, env)
		}) {
			return false
		}
	}
	for _, mount := range expected.VolumeMounts {
		if !slices.ContainsFunc(actual.VolumeMounts, func(candidate corev1.VolumeMount) bool {
			return candidate.Name == mount.Name && candidate.MountPath == mount.MountPath &&
				candidate.SubPath == mount.SubPath && candidate.SubPathExpr == mount.SubPathExpr &&
				candidate.ReadOnly == mount.ReadOnly &&
				apiequality.Semantic.DeepEqual(candidate.MountPropagation, mount.MountPropagation)
		}) {
			return false
		}
	}
	return true
}

func validateKServeObject(obj *unstructured.Unstructured, spec RuntimeSpec, expected *RuntimeBinding) (bizreconcile.RuntimeObject, error) {
	if !OwnedBy(obj, spec.TenantID, spec.ServiceID) {
		return bizreconcile.RuntimeObject{}, errors.New("observed KServe ownership labels do not match")
	}
	generation, err := strconv.ParseInt(obj.GetLabels()[generationLabel], 10, 64)
	if err != nil || generation < 1 || generation != spec.Generation {
		return bizreconcile.RuntimeObject{}, bizreconcile.ErrStaleGeneration
	}
	if obj.GetUID() == "" || obj.GetResourceVersion() == "" {
		return bizreconcile.RuntimeObject{}, errors.New("observed KServe object is missing UID or resourceVersion")
	}
	if expected != nil && (expected.Namespace != obj.GetNamespace() || expected.Name != obj.GetName() || expected.UID != string(obj.GetUID())) {
		return bizreconcile.RuntimeObject{}, bizreconcile.ErrStaleGeneration
	}
	kind := KServeInferenceServiceKind
	if obj.GroupVersionKind().Kind == "LLMInferenceService" {
		kind = KServeLLMInferenceServiceKind
	}
	return bizreconcile.RuntimeObject{BindingGeneration: spec.Generation, Kind: kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: string(obj.GetUID()), ResourceVersion: obj.GetResourceVersion(), Role: "runtime", ExpectedUID: expectedUID(expected), ExpectedResourceVersion: expectedResourceVersion(expected)}, nil
}

func kserveReady(obj *unstructured.Unstructured) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false, "KServe readiness condition is not available"
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok || condition["type"] != "Ready" {
			continue
		}
		if condition["status"] == "True" {
			return true, ""
		}
		if message, ok := condition["message"].(string); ok && strings.TrimSpace(message) != "" {
			return false, message
		}
		if reason, ok := condition["reason"].(string); ok && strings.TrimSpace(reason) != "" {
			return false, reason
		}
		return false, "KServe Ready condition is not true"
	}
	return false, "KServe Ready condition is not available"
}
