// Package kubernetes renders deterministic, owned runtime objects. API calls
// and reconciliation leases live in the controller adapter; this package only
// turns a validated desired spec into typed Kubernetes objects.
package kubernetes

import (
	"fmt"

	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const (
	serviceIDLabel  = "ani.kubercloud.com/service-id"
	tenantIDLabel   = "ani.kubercloud.com/tenant-id"
	generationLabel = "ani.kubercloud.com/generation"
)

type RuntimeSpec struct {
	TenantID, ServiceID, Name, Namespace, Image                   string
	ModelVersionID, ArtifactProvider, ArtifactRef, ArtifactSHA256 string
	ServedModelName, EngineRuntime                                string
	RuntimeProvider                                               string
	Generation                                                    int64
	CommandArgv                                                   []string
	Resources                                                     resources.Normalized
	Replicas, WorkerReplicas                                      int32
	RuntimeMode                                                   string
	// Endpoint fields are intentionally explicit. Renderers never infer a
	// listener port from an image or silently choose 8080.
	ContainerPort, ServicePort int32
	TargetPort                 intstr.IntOrString
	ServiceProtocol            corev1.Protocol
	Endpoint                   *EndpointSpec
	// ModelClaim names a PVC populated and verified by the materializer.
	ModelClaim string
	// GPUPlan is an immutable accelerator resolution. GPU requests are invalid
	// unless this plan is present and validated before rendering.
	GPUPlan *gpu.Plan
	// ManagedGPU is read from the accepted Governance command, never inferred
	// from a GPU quantity or an unsigned runtime request.
	ManagedGPU bool
}

type EndpointSpec struct {
	ContainerPort int32
	ServicePort   int32
	TargetPort    intstr.IntOrString
	Protocol      corev1.Protocol
}

// Deployment returns the typed apps/v1 object accepted by client-go and
// controller-runtime clients. ResourceRequirements keeps Kubernetes quantity
// parsing and requests/limits semantics in the API types instead of an
// unstructured map tree.
func Deployment(spec RuntimeSpec) (*appsv1.Deployment, error) {
	spec = normalizeEndpoint(spec)
	if spec.Name == "" || spec.Namespace == "" || spec.ServiceID == "" || spec.TenantID == "" || spec.Image == "" {
		return nil, fmt.Errorf("name, namespace, tenant ID, service ID and image are required")
	}
	if spec.Generation < 1 || spec.Replicas < 1 {
		return nil, fmt.Errorf("generation and replicas must be positive")
	}
	if spec.RuntimeMode != "" && spec.RuntimeMode != "deployment" {
		return nil, fmt.Errorf("runtime mode %q requires the LeaderWorkerSet renderer", spec.RuntimeMode)
	}
	resourceRequirements, err := resourceRequirements(spec.Resources)
	if err != nil {
		return nil, err
	}
	stableLabels := map[string]string{
		"app.kubernetes.io/name": "ani-inference",
		tenantIDLabel:            spec.TenantID,
		serviceIDLabel:           spec.ServiceID,
	}
	labels := map[string]string{
		"app.kubernetes.io/name": "ani-inference",
		tenantIDLabel:            spec.TenantID,
		serviceIDLabel:           spec.ServiceID,
		generationLabel:          fmt.Sprint(spec.Generation),
	}
	container := corev1.Container{
		Name: "inference", Image: spec.Image, Resources: resourceRequirements,
	}
	if spec.ContainerPort > 0 {
		if !validPort(spec.ContainerPort) || !validProtocol(spec.ServiceProtocol) {
			return nil, fmt.Errorf("container port requires a valid port and explicit protocol")
		}
		container.Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: spec.ContainerPort, Protocol: spec.ServiceProtocol}}
	}
	if len(spec.CommandArgv) > 0 {
		container.Command = append([]string(nil), spec.CommandArgv...)
	}
	obj := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name: spec.Name, Namespace: spec.Namespace, Labels: labels,
			Annotations: map[string]string{generationLabel: fmt.Sprint(spec.Generation)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &spec.Replicas,
			Selector: &metav1.LabelSelector{MatchLabels: stableLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
			},
		},
	}
	if err := mountModel(&obj.Spec.Template.Spec, spec); err != nil {
		return nil, err
	}
	return obj, nil
}

// LeaderWorkerSet renders the official LWS API object. Replicas is the number
// of groups; WorkerReplicas excludes the leader, so LWS size is workers+1.
func LeaderWorkerSet(spec RuntimeSpec) (*lwsv1.LeaderWorkerSet, error) {
	if spec.Name == "" || spec.Namespace == "" || spec.ServiceID == "" || spec.TenantID == "" || spec.Image == "" {
		return nil, fmt.Errorf("name, namespace, tenant ID, service ID and image are required")
	}
	if spec.Generation < 1 || spec.Replicas < 1 || spec.WorkerReplicas < 1 {
		return nil, fmt.Errorf("generation and replicas must be positive and worker replicas must be positive")
	}
	requirements, err := resourceRequirements(spec.Resources)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{"app.kubernetes.io/name": "ani-inference", tenantIDLabel: spec.TenantID, serviceIDLabel: spec.ServiceID, generationLabel: fmt.Sprint(spec.Generation)}
	container := corev1.Container{Name: "inference", Image: spec.Image, Resources: requirements}
	if spec.ContainerPort > 0 {
		if !validPort(spec.ContainerPort) || !validProtocol(spec.ServiceProtocol) {
			return nil, fmt.Errorf("container port requires a valid port and explicit protocol")
		}
		container.Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: spec.ContainerPort, Protocol: spec.ServiceProtocol}}
	}
	if len(spec.CommandArgv) > 0 {
		container.Command = append([]string(nil), spec.CommandArgv...)
	}
	groups, size := spec.Replicas, spec.WorkerReplicas+1
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{container}}}
	if err := mountModel(&template.Spec, spec); err != nil {
		return nil, err
	}
	return &lwsv1.LeaderWorkerSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: lwsv1.GroupVersion.String(), Kind: "LeaderWorkerSet"},
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: spec.Namespace, Labels: labels, Annotations: map[string]string{generationLabel: fmt.Sprint(spec.Generation)}},
		Spec: lwsv1.LeaderWorkerSetSpec{
			Replicas: &groups, LeaderWorkerTemplate: lwsv1.LeaderWorkerTemplate{Size: &size, WorkerTemplate: template},
			StartupPolicy: lwsv1.LeaderCreatedStartupPolicy,
			// Populate the same nested defaults as the official webhook. The
			// controller dereferences this configuration, so relying on webhook
			// defaulting would make renderer output unsafe in webhook-free paths.
			RolloutStrategy: lwsv1.RolloutStrategy{
				Type: lwsv1.RollingUpdateStrategyType,
				RollingUpdateConfiguration: &lwsv1.RollingUpdateConfiguration{
					MaxUnavailable: intstr.FromInt32(1),
					MaxSurge:       intstr.FromInt32(0),
					Partition:      ptr.To[int32](0),
				},
			},
		},
	}, nil
}

// Service renders the Inference-owned stable endpoint. The generation label
// is part of the selector so an old runtime cannot receive traffic after a
// replacement starts. Callers must supply every endpoint field explicitly.
func Service(spec RuntimeSpec) (*corev1.Service, error) {
	spec = normalizeEndpoint(spec)
	if spec.Name == "" || spec.Namespace == "" || spec.ServiceID == "" || spec.TenantID == "" {
		return nil, fmt.Errorf("name, namespace, tenant ID and service ID are required")
	}
	if spec.Generation < 1 {
		return nil, fmt.Errorf("generation must be positive")
	}
	if !validPort(spec.ContainerPort) || !validPort(spec.ServicePort) {
		return nil, fmt.Errorf("container and service ports must be between 1 and 65535")
	}
	if spec.TargetPort.Type != intstr.Int && spec.TargetPort.Type != intstr.String {
		return nil, fmt.Errorf("target port must be an explicit positive port or name")
	}
	if spec.TargetPort.Type == intstr.Int && !validPort(int32(spec.TargetPort.IntValue())) ||
		spec.TargetPort.Type == intstr.String && spec.TargetPort.StrVal == "" {
		return nil, fmt.Errorf("target port must be an explicit positive port or name")
	}
	if !validProtocol(spec.ServiceProtocol) {
		return nil, fmt.Errorf("service protocol must be TCP, UDP or SCTP")
	}
	name := spec.ServiceID + "-endpoint"
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return nil, fmt.Errorf("endpoint service name %q is invalid: %s", name, errs[0])
	}
	labels := map[string]string{
		"app.kubernetes.io/name": "ani-inference",
		tenantIDLabel:            spec.TenantID,
		serviceIDLabel:           spec.ServiceID,
		generationLabel:          fmt.Sprint(spec.Generation),
	}
	selector := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		selector[key] = value
	}
	if spec.RuntimeMode == "leader_worker_set" {
		// LWS creates a same-name headless Service for group discovery and puts
		// worker-index=0 on each leader. The inference endpoint must select only
		// those leaders; workers are coordination backends, not user backends.
		selector[lwsv1.WorkerIndexLabelKey] = "0"
	}
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: spec.Namespace, Labels: labels,
			Annotations: map[string]string{generationLabel: fmt.Sprint(spec.Generation)}},
		Spec: corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Name: "http", Port: spec.ServicePort, TargetPort: spec.TargetPort, Protocol: spec.ServiceProtocol}}},
	}, nil
}

func normalizeEndpoint(spec RuntimeSpec) RuntimeSpec {
	if spec.Endpoint != nil {
		spec.ContainerPort, spec.ServicePort = spec.Endpoint.ContainerPort, spec.Endpoint.ServicePort
		spec.TargetPort, spec.ServiceProtocol = spec.Endpoint.TargetPort, spec.Endpoint.Protocol
	}
	return spec
}

func validPort(port int32) bool { return port >= 1 && port <= 65535 }

func validProtocol(protocol corev1.Protocol) bool {
	return protocol == corev1.ProtocolTCP || protocol == corev1.ProtocolUDP || protocol == corev1.ProtocolSCTP
}

func resourceRequirements(spec resources.Normalized) (corev1.ResourceRequirements, error) {
	result := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	for name, value := range spec.Requests {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid request %q for %s: %w", value, name, err)
		}
		result.Requests[corev1.ResourceName(name)] = quantity
	}
	for name, value := range spec.Limits {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid limit %q for %s: %w", value, name, err)
		}
		result.Limits[corev1.ResourceName(name)] = quantity
	}
	return result, nil
}

func OwnedBy(obj metav1.Object, tenantID, serviceID string) bool {
	if obj == nil || tenantID == "" || serviceID == "" {
		return false
	}
	labels := obj.GetLabels()
	return labels[tenantIDLabel] == tenantID && labels[serviceIDLabel] == serviceID
}
