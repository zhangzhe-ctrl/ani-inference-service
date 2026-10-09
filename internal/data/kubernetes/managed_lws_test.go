package kubernetes

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	volcanov1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// This is an external Acc result fixture, not a replacement for projection.
func managedLWSFixture(t *testing.T, whole bool) (RuntimeSpec, *lwsv1.LeaderWorkerSet) {
	t.Helper()
	const id = "10000000-0000-4000-8000-000000000001"
	r := &gpu.Request{ClusterID: id, PoolID: id, ProfileID: id, ProfileVersion: 1, Replicas: 2, DevicesPerReplica: 1, ContainerName: "main"}
	p := &gpu.Plan{SchemaVersion: 1, Request: r, Profile: &gpu.Profile{ProfileID: id, ProfileVersion: 1, Published: true, DisplayName: "test", BaselineID: id, SpecDigest: "a10a605b164c85aec29cfa13c5e870759da9833825c8a9af089540fbc708add9", Spec: &gpu.ProfileSpec{GroupID: id, Mode: 2, ModelKey: "nvidia-test", SharedMemoryMiB: 6144, CoreLimitPercent: 25, MaxDevicesPerReplica: 1, IsolationClass: "SOFTWARE_COOPERATIVE"}}, Encoding: &gpu.MemoryEncoding{MemoryBlockMiB: 1024, MemoryBlocksPerDevice: 6, SharedMemoryMiB: 6144, Policy: "EXACT"}, Totals: &gpu.ResourceTotals{LogicalDeviceCount: 2, SharedMemoryMiB: 12288}, Runtime: &gpu.RuntimeFragment{SchedulerName: "volcano", QueueName: "gpu", RuntimeClassName: "gpu-runtime", RecipeVersion: "volcano-hami-v1", NodeLabels: []gpu.KeyValue{{Key: "accelerator.ani.io/baseline-id", Value: id}, {Key: "accelerator.ani.io/model-key", Value: "nvidia-test"}, {Key: "accelerator.ani.io/supply-group", Value: id}}, PodAnnotations: []gpu.KeyValue{{Key: "volcano.sh/vgpu-mode", Value: "hami-core"}}, LimitsPerContainer: []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "25"}, {Key: "volcano.sh/vgpu-memory", Value: "6"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}}, BaselineDigest: "52245607efe2656462e8aba6b9660b2e5eb9abb55bc381892952fd77ff71a70a"}
	if whole {
		p.Profile.Spec.Mode, p.Profile.Spec.SharedMemoryMiB, p.Profile.Spec.CoreLimitPercent, p.Profile.Spec.IsolationClass = 1, 0, 100, "WHOLE_DEVICE_EXCLUSIVE"
		const body = `{"baseline_id":"10000000-0000-4000-8000-000000000001","spec":{"core_limit_percent":"100","group_id":"10000000-0000-4000-8000-000000000001","isolation_class":"WHOLE_DEVICE_EXCLUSIVE","max_devices_per_replica":"1","mode":"1","model_key":"nvidia-test","shared_memory_mib":"0"}}`
		sum := sha256.Sum256(append([]byte("acc-c14n-v1\n"), []byte(body)...))
		p.Profile.SpecDigest = hex.EncodeToString(sum[:])
		p.Encoding.MemoryBlocksPerDevice, p.Encoding.SharedMemoryMiB, p.Encoding.MemoryPercentage = 0, 0, 100
		p.Totals = &gpu.ResourceTotals{ExclusiveDeviceCount: 2}
		p.Runtime.LimitsPerContainer = []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "100"}, {Key: "volcano.sh/vgpu-memory-percentage", Value: "100"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}
	}
	var err error
	p.ResolutionDigest, err = gpu.PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	spec := RuntimeSpec{TenantID: id, ServiceID: id, Namespace: "models", Name: "distributed", Image: "engine:v1", Generation: 1, RuntimeMode: "leader_worker_set", Replicas: 1, WorkerReplicas: 1, ManagedGPU: true, GPUPlan: p, Resources: resources.Normalized{GPU: r}}
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"keep": "value"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "engine:v1", Command: []string{"engine"}, Args: []string{"serve", "--caller-flag"}, Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")}}}}}}
	input := &lwsv1.LeaderWorkerSet{ObjectMeta: metav1.ObjectMeta{Name: KServeLLMWorkloadName(spec.Name), Namespace: spec.Namespace, Annotations: map[string]string{"keep": "value"}}, Spec: lwsv1.LeaderWorkerSetSpec{Replicas: ptr.To[int32](1), LeaderWorkerTemplate: lwsv1.LeaderWorkerTemplate{Size: ptr.To[int32](2), LeaderTemplate: template.DeepCopy(), WorkerTemplate: *template.DeepCopy()}}}
	return spec, input
}

func TestManagedLWSPreCreateProjection(t *testing.T) {
	for _, whole := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared_two_pods_12288MiB", true: "whole_two_devices"}[whole], func(t *testing.T) {
			spec, input := managedLWSFixture(t, whole)
			before := input.DeepCopy()
			output, err := ProjectManagedGPULeaderWorkerSet(spec, input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(input, before) {
				t.Fatal("projection mutated its caller's object")
			}
			if output.Annotations[volcanov1.QueueNameAnnotationKey] != spec.GPUPlan.Runtime.QueueName || output.Annotations["keep"] != "value" {
				t.Fatalf("queue/top-level metadata lost: %v", output.Annotations)
			}
			for _, template := range []*corev1.PodTemplateSpec{output.Spec.LeaderWorkerTemplate.LeaderTemplate, &output.Spec.LeaderWorkerTemplate.WorkerTemplate} {
				if template.Spec.SchedulerName != "volcano" || template.Spec.RuntimeClassName == nil || *template.Spec.RuntimeClassName != "gpu-runtime" || template.Annotations["volcano.sh/vgpu-mode"] != "hami-core" || template.Annotations[volcanov1.QueueNameAnnotationKey] != "gpu" || template.Annotations["keep"] != "value" {
					t.Fatalf("incomplete leader/worker projection: %+v", template)
				}
				for _, item := range spec.GPUPlan.Runtime.NodeLabels {
					if template.Spec.NodeSelector[item.Key] != item.Value {
						t.Fatalf("selector %s missing", item.Key)
					}
				}
				container := template.Spec.Containers[0]
				for _, item := range spec.GPUPlan.Runtime.LimitsPerContainer {
					value := container.Resources.Limits[corev1.ResourceName(item.Key)]
					if value.Cmp(resource.MustParse(item.Value)) != 0 {
						t.Fatalf("limit %s lost: %v", item.Key, container.Resources.Limits)
					}
				}
				if !reflect.DeepEqual(container.Command, []string{"engine"}) || !reflect.DeepEqual(container.Args, []string{"serve", "--caller-flag"}) {
					t.Fatalf("projection changed engine argv: %+v", container)
				}
			}
		})
	}
}

func TestManagedLWSPreCreateRejectsConflicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*lwsv1.LeaderWorkerSet)
	}{
		{"already created", func(l *lwsv1.LeaderWorkerSet) { l.ResourceVersion = "2" }},
		{"different namespace", func(l *lwsv1.LeaderWorkerSet) { l.Namespace = "other" }},
		{"groups", func(l *lwsv1.LeaderWorkerSet) { l.Spec.Replicas = ptr.To[int32](2) }},
		{"group size", func(l *lwsv1.LeaderWorkerSet) { l.Spec.LeaderWorkerTemplate.Size = ptr.To[int32](3) }},
		{"missing leader", func(l *lwsv1.LeaderWorkerSet) { l.Spec.LeaderWorkerTemplate.LeaderTemplate = nil }},
		{"worker container", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Name = "other"
		}},
		{"extra sidecar", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers = append(l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers, corev1.Container{Name: "gpu-sidecar"})
		}},
		{"GPU init container", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}
		}},
		{"unplanned device", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1")
		}},
		{"queue", func(l *lwsv1.LeaderWorkerSet) { l.Annotations[volcanov1.QueueNameAnnotationKey] = "other" }},
		{"pod annotation", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.LeaderTemplate.Annotations["volcano.sh/vgpu-mode"] = "other"
		}},
		{"scheduler", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.SchedulerName = "other"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, input := managedLWSFixture(t, false)
			tc.mutate(input)
			if _, err := ProjectManagedGPULeaderWorkerSet(spec, input); err == nil {
				t.Fatal("conflicting LWS accepted")
			}
		})
	}
}
