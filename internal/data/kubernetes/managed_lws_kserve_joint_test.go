package kubernetes_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	acceleratorv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	kube "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/postgres"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type lwsAdmissionJointCase struct {
	Name            string                 `json:"name"`
	Owner           map[string]interface{} `json:"owner"`
	Plan            *gpu.Plan              `json:"plan"`
	OriginalContext gpu.RefundContext      `json:"original_context"`
	Groups          int32                  `json:"groups"`
	Size            int32                  `json:"size"`
	Command         []string               `json:"command"`
	Args            []string               `json:"args"`
}

// TestManagedLWSAdmissionProcess is an opt-in software-integration subprocess.
// Only Acc's external resolver result, the model PVC and Kubernetes APIReader
// are fixtures. CREATE admission, PG persistence/binding lookup, the runtime
// conversion, renderer, HTTPS authentication and webhook are production code.
// The separately pinned KServe module invokes its public Reconcile method and
// sends each API CREATE/UPDATE/dry-run through this real HTTPS handler.
func TestManagedLWSAdmissionProcess(t *testing.T) {
	if os.Getenv("INFERENCE_LWS_ADMISSION_PROCESS") != "1" {
		t.Skip("external pinned KServe admission integration is opt-in")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	pool, err := pgxpool.New(ctx, lwsJointEnv(t, "INFERENCE_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	namespace := lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_NAMESPACE")
	repo := postgres.NewRepository(pool)
	ownerService := service.NewInferenceServer(postgres.NewCreateUseCase(repo))
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "serving.kserve.io", Version: "v1alpha1", Kind: "LLMInferenceService"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	var objects []runtime.Object
	var cases []lwsAdmissionJointCase
	for _, whole := range []bool{false, true} {
		name := "shared"
		if whole {
			name = "whole"
		}
		request := lwsJointCreateFixture(t, whole)
		a := request.GetGpuOwnerAttachment()
		// Seed the actual trusted intake, without pretending that this in-process
		// test context performs mTLS. The webhook requests below must use TLS.
		seedContext := service.WithTenantID(gpu.WithGovernanceIdentity(ctx), a.Ref.TenantId)
		ack, err := ownerService.CreateInferenceService(seedContext, request)
		if err != nil {
			t.Fatal(err)
		}
		if !ack.GetDurableOwnerAck().GetAccepted() || ack.GetDurableOwnerAck().GetResourceId() != a.Ref.ResourceId || ack.GetOperation().GetId() != a.Ref.CreateOperationId {
			t.Fatal("actual intake changed the Governance command identity or ACK")
		}
		desired, err := postgres.NewRuntimeSource(pool, namespace).CurrentRuntime(ctx, a.Ref.TenantId, a.Ref.ResourceId, 1)
		if err != nil {
			t.Fatal(err)
		}
		object, err := kube.RenderKServeRuntime(desired.RuntimeSpec)
		if err != nil {
			t.Fatal(err)
		}
		// UID/resourceVersion are returned by the external Kubernetes boundary.
		// Persist that observation with the same repository CAS as the executor.
		object.SetUID(types.UID(uuid.NewString()))
		object.SetResourceVersion("1")
		object.SetGeneration(1)
		if err := repo.UpsertRuntimeBindingCAS(ctx, postgres.RuntimeBindingInput{TenantID: a.Ref.TenantId, ServiceID: a.Ref.ResourceId, Generation: 1, ObjectKind: kube.KServeLLMInferenceServiceKind, ObjectNamespace: namespace, ObjectName: object.GetName(), ObjectUID: string(object.GetUID()), ResourceVersion: object.GetResourceVersion(), Role: "runtime"}); err != nil {
			t.Fatal(err)
		}
		original, err := repo.LoadRefundContext(ctx, a.Ref.TenantId, a.Ref.ResourceId, a.Ref.CreateOperationId)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(original.Plan, desired.GPUPlan) {
			t.Fatal("runtime projection differs from the persisted original CREATE plan")
		}
		// Containers is a list; decode the actual rendered first container.
		containers, found, decodeErr := unstructured.NestedSlice(object.Object, "spec", "template", "containers")
		if decodeErr != nil || !found || len(containers) != 1 {
			t.Fatal("actual renderer did not produce one explicit main container")
		}
		container, ok := containers[0].(map[string]interface{})
		if !ok {
			t.Fatal("invalid rendered container")
		}
		command, _, err := unstructured.NestedStringSlice(container, "command")
		if err != nil {
			t.Fatal(err)
		}
		args, _, err := unstructured.NestedStringSlice(container, "args")
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, lwsAdmissionJointCase{Name: name, Owner: object.Object, Plan: desired.GPUPlan, OriginalContext: original, Groups: desired.Replicas, Size: desired.WorkerReplicas + 1, Command: command, Args: args})
		objects = append(objects, object)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	handler, err := kube.NewManagedLWSAdmissionWebhook(postgres.NewManagedLWSAdmissionSource(pool, namespace), reader, kube.ManagedLWSAdmissionConfig{Namespace: namespace, APIServerClientDNSName: lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_CLIENT_DNS"), ControllerUsername: lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_CONTROLLER_USER")})
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_CERT_FILE"), lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_CA_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid isolated admission CA")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(kube.ManagedLWSAdmissionPath, handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca, Certificates: []tls.Certificate{cert}}}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	manifest, err := json.MarshalIndent(struct {
		Address string                  `json:"address"`
		Cases   []lwsAdmissionJointCase `json:"cases"`
	}{"https://" + listener.Addr().String() + kube.ManagedLWSAdmissionPath, cases}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lwsJointEnv(t, "INFERENCE_LWS_ADMISSION_READY_FILE"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		stopContext, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Shutdown(stopContext); err != nil {
			t.Fatal(err)
		}
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	}
}

func lwsJointEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required for opted-in software integration", key)
	}
	return value
}

// This is a clearly identified external Accelerator result fixture, matching
// the existing managed PG fixture contract. No KServe builder or Inference
// admission/persistence/projection logic is copied into the external harness.
func lwsJointCreateFixture(t *testing.T, whole bool) *inferencev1.CreateInferenceServiceRequest {
	t.Helper()
	const profile = "10000000-0000-4000-8000-000000000001"
	r := &gpu.Request{ClusterID: profile, PoolID: profile, ProfileID: profile, ProfileVersion: 1, Replicas: 4, DevicesPerReplica: 1, ContainerName: "main"}
	p := &gpu.Plan{SchemaVersion: 1, Request: r, Profile: &gpu.Profile{ProfileID: profile, ProfileVersion: 1, Published: true, DisplayName: "external test result", BaselineID: profile, SpecDigest: "a10a605b164c85aec29cfa13c5e870759da9833825c8a9af089540fbc708add9", Spec: &gpu.ProfileSpec{GroupID: profile, Mode: 2, ModelKey: "nvidia-test", SharedMemoryMiB: 6144, CoreLimitPercent: 25, MaxDevicesPerReplica: 1, IsolationClass: "SOFTWARE_COOPERATIVE"}}, Encoding: &gpu.MemoryEncoding{MemoryBlockMiB: 1024, MemoryBlocksPerDevice: 6, SharedMemoryMiB: 6144, Policy: "EXACT"}, Totals: &gpu.ResourceTotals{LogicalDeviceCount: 4, SharedMemoryMiB: 24576}, Runtime: &gpu.RuntimeFragment{SchedulerName: "volcano", QueueName: "gpu", RuntimeClassName: "gpu-runtime", RecipeVersion: "volcano-hami-v1", NodeLabels: []gpu.KeyValue{{Key: "accelerator.ani.io/baseline-id", Value: profile}, {Key: "accelerator.ani.io/model-key", Value: "nvidia-test"}, {Key: "accelerator.ani.io/supply-group", Value: profile}}, PodAnnotations: []gpu.KeyValue{{Key: "volcano.sh/vgpu-mode", Value: "hami-core"}}, LimitsPerContainer: []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "25"}, {Key: "volcano.sh/vgpu-memory", Value: "6"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}}, BaselineDigest: "52245607efe2656462e8aba6b9660b2e5eb9abb55bc381892952fd77ff71a70a"}
	code, units := "gpu.shared_memory_mib", int64(24576)
	if whole {
		p.Profile.Spec.Mode, p.Profile.Spec.SharedMemoryMiB, p.Profile.Spec.CoreLimitPercent, p.Profile.Spec.IsolationClass = 1, 0, 100, "WHOLE_DEVICE_EXCLUSIVE"
		const body = `{"baseline_id":"10000000-0000-4000-8000-000000000001","spec":{"core_limit_percent":"100","group_id":"10000000-0000-4000-8000-000000000001","isolation_class":"WHOLE_DEVICE_EXCLUSIVE","max_devices_per_replica":"1","mode":"1","model_key":"nvidia-test","shared_memory_mib":"0"}}`
		sum := sha256.Sum256(append([]byte("acc-c14n-v1\n"), []byte(body)...))
		p.Profile.SpecDigest = hex.EncodeToString(sum[:])
		p.Encoding.MemoryBlocksPerDevice, p.Encoding.SharedMemoryMiB, p.Encoding.MemoryPercentage = 0, 0, 100
		p.Totals = &gpu.ResourceTotals{ExclusiveDeviceCount: 4}
		p.Runtime.LimitsPerContainer = []gpu.KeyValue{{Key: "volcano.sh/vgpu-cores", Value: "100"}, {Key: "volcano.sh/vgpu-memory-percentage", Value: "100"}, {Key: "volcano.sh/vgpu-number", Value: "1"}}
		code, units = "gpu.physical.count", 4
	}
	var err error
	p.ResolutionDigest, err = gpu.PlanDigest(p)
	if err != nil {
		t.Fatal(err)
	}
	var plan acceleratorv1.ResolvedGpuPlan
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	charge := &integrationv1.GpuChargeRef{ChargeId: uuid.NewString(), QuotaCode: code, OriginalUnits: units}
	resource := uuid.NewString()
	request := &inferencev1.CreateInferenceServiceRequest{RequestId: uuid.NewString(), Name: "c02-" + resource[:13], ModelVersionId: uuid.NewString(), Replicas: 2, Runtime: &inferencev1.RuntimeSpec{Mode: inferencev1.RuntimeMode_RUNTIME_MODE_LEADER_WORKER_SET, WorkerReplicas: 1, Provider: "kserve"}, ModelArtifact: &inferencev1.ModelArtifact{Provider: "model", Reference: "external-test-model-artifact"}, Engine: &inferencev1.EngineSpec{Type: "vllm", Image: "fixture.invalid/held-engine:software-only", Command: []string{"engine"}, Args: []string{"serve", "--caller-flag", "/models"}}, Resource: &inferencev1.ResourceSpec{Gpu: &inferencev1.GpuRequest{ClusterId: r.ClusterID, PoolId: r.PoolID, ProfileId: r.ProfileID, ProfileVersion: r.ProfileVersion, Replicas: r.Replicas, DevicesPerReplica: r.DevicesPerReplica, ContainerName: r.ContainerName}, Requests: map[string]string{"cpu": "1", "memory": "8Gi"}}, GpuOwnerAttachment: &integrationv1.GpuOwnerCreateAttachment{Ref: &acceleratorv1.GpuUsageRef{TenantId: uuid.NewString(), OwnerService: "ani-inference", ResourceId: resource, CreateOperationId: uuid.NewString()}, Actor: &acceleratorv1.Actor{Type: "user", Id: "42"}, GpuPlan: &plan, GpuCharges: []*integrationv1.GpuChargeRef{charge}, MeteringVersion: "gpu-metering-v1"}, OriginalCharges: []*inferencev1.OriginalQuotaCharge{{ChargeId: charge.ChargeId, QuotaCode: code, OriginalUnits: units}}}
	request.GpuOwnerAttachment.BusinessPayloadDigest, err = inferencev1.BusinessPayloadDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.GpuOwnerAttachment.RequestHash, err = inferencev1.ManagedCreateRequestHash(request, request.GpuOwnerAttachment.Ref.TenantId, "user", "42")
	if err != nil {
		t.Fatal(err)
	}
	return request
}
