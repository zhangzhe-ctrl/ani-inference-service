package kservecontract

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	servingv1 "github.com/kserve/kserve/pkg/apis/serving/v1alpha1"
	"github.com/kserve/kserve/pkg/constants"
	"github.com/kserve/kserve/pkg/controller/v1alpha1/llmisvc"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	igwapi "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const queueKey = "scheduling.volcano.sh/queue-name"

type frozenPlan struct {
	Profile struct {
		Spec struct {
			Mode   int32 `json:"mode"`
			Shared int64 `json:"shared_memory_mib"`
			Cores  int32 `json:"core_limit_percent"`
		} `json:"spec"`
	} `json:"profile"`
	Encoding struct {
		F          int64 `json:"memory_block_mib"`
		Q          int64 `json:"memory_blocks_per_device"`
		Percentage int32 `json:"memory_percentage"`
	} `json:"encoding"`
	Totals struct {
		Shared    int64 `json:"shared_memory_mib"`
		Exclusive int64 `json:"exclusive_device_count"`
	} `json:"totals"`
	Runtime struct {
		Scheduler   string     `json:"scheduler_name"`
		Queue       string     `json:"queue_name"`
		Class       string     `json:"runtime_class_name"`
		Labels      []keyValue `json:"node_labels"`
		Annotations []keyValue `json:"pod_annotations"`
		Limits      []keyValue `json:"limits_per_container"`
	} `json:"runtime"`
}
type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type jointCase struct {
	Name            string          `json:"name"`
	Owner           json.RawMessage `json:"owner"`
	Plan            frozenPlan      `json:"plan"`
	OriginalContext json.RawMessage `json:"original_context"`
	Groups          int32           `json:"groups"`
	Size            int32           `json:"size"`
	Command         []string        `json:"command"`
	Args            []string        `json:"args"`
}
type jointManifest struct {
	Address string      `json:"address"`
	Cases   []jointCase `json:"cases"`
}
type admittedWrite struct {
	Operation admissionv1.Operation  `json:"operation"`
	DryRun    bool                   `json:"dry_run"`
	Object    *lwsv1.LeaderWorkerSet `json:"object"`
}

// admissionBoundary replaces only the external Kubernetes API. It invokes the
// actual Inference HTTPS webhook before storing CREATE/UPDATE, and returns the
// mutated dry-run result to the real KServe reconciler without storing it.
type admissionBoundary struct {
	client.Client
	HTTP              *http.Client
	Address, Username string
	Writes            []admittedWrite
}

func (c *admissionBoundary) Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error {
	lws, ok := object.(*lwsv1.LeaderWorkerSet)
	if !ok {
		return c.Client.Create(ctx, object, opts...)
	}
	options := &client.CreateOptions{}
	options.ApplyOptions(opts)
	dryRun := len(options.DryRun) != 0
	mutated, err := c.admit(ctx, admissionv1.Create, lws, nil, dryRun, c.Username)
	if err != nil {
		return err
	}
	*lws = *mutated
	c.Writes = append(c.Writes, admittedWrite{admissionv1.Create, dryRun, lws.DeepCopy()})
	if dryRun {
		return nil
	}
	lws.UID = types.UID("external-kubernetes-" + lws.Name)
	return c.Client.Create(ctx, object, opts...)
}

func (c *admissionBoundary) Update(ctx context.Context, object client.Object, opts ...client.UpdateOption) error {
	lws, ok := object.(*lwsv1.LeaderWorkerSet)
	if !ok {
		return c.Client.Update(ctx, object, opts...)
	}
	old := &lwsv1.LeaderWorkerSet{}
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(lws), old); err != nil {
		return err
	}
	// The Kubernetes update boundary fills an omitted immutable UID from the
	// existing object. Preserve a supplied UID so a forged one is rejected.
	if lws.UID == "" {
		lws.UID = old.UID
	}
	options := &client.UpdateOptions{}
	options.ApplyOptions(opts)
	dryRun := len(options.DryRun) != 0
	mutated, err := c.admit(ctx, admissionv1.Update, lws, old, dryRun, c.Username)
	if err != nil {
		return err
	}
	*lws = *mutated
	c.Writes = append(c.Writes, admittedWrite{admissionv1.Update, dryRun, lws.DeepCopy()})
	if dryRun {
		return nil
	}
	return c.Client.Update(ctx, object, opts...)
}

func (c *admissionBoundary) admit(ctx context.Context, operation admissionv1.Operation, object, old *lwsv1.LeaderWorkerSet, dryRun bool, username string) (*lwsv1.LeaderWorkerSet, error) {
	// The external typed Kubernetes serializer supplies wire GVK.
	wire := object.DeepCopy()
	wire.SetGroupVersionKind(lwsv1.GroupVersion.WithKind("LeaderWorkerSet"))
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	var oldRaw []byte
	if old != nil {
		oldWire := old.DeepCopy()
		oldWire.SetGroupVersionKind(lwsv1.GroupVersion.WithKind("LeaderWorkerSet"))
		oldRaw, err = json.Marshal(oldWire)
		if err != nil {
			return nil, err
		}
	}
	review := admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Request: &admissionv1.AdmissionRequest{UID: types.UID(fmt.Sprintf("c02-%d", len(c.Writes)+1)), Kind: metav1.GroupVersionKind{Group: lwsv1.GroupVersion.Group, Version: lwsv1.GroupVersion.Version, Kind: "LeaderWorkerSet"}, Resource: metav1.GroupVersionResource{Group: lwsv1.GroupVersion.Group, Version: lwsv1.GroupVersion.Version, Resource: "leaderworkersets"}, Namespace: object.Namespace, Name: object.Name, Operation: operation, UserInfo: authenticationv1.UserInfo{Username: username}, Object: runtime.RawExtension{Raw: raw}, OldObject: runtime.RawExtension{Raw: oldRaw}, DryRun: &dryRun}}
	body, err := json.Marshal(review)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Address, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("actual webhook HTTP %d: %s", response.StatusCode, responseBody)
	}
	var answer admissionv1.AdmissionReview
	if err := json.Unmarshal(responseBody, &answer); err != nil {
		return nil, err
	}
	if answer.Response == nil || answer.Response.UID != review.Request.UID || !answer.Response.Allowed {
		return nil, fmt.Errorf("actual webhook denied %s: %+v", operation, answer.Response)
	}
	if len(answer.Response.Patch) != 0 {
		if answer.Response.PatchType == nil || *answer.Response.PatchType != admissionv1.PatchTypeJSONPatch {
			return nil, fmt.Errorf("unexpected admission patch type")
		}
		patch, err := jsonpatch.DecodePatch(answer.Response.Patch)
		if err != nil {
			return nil, err
		}
		raw, err = patch.Apply(raw)
		if err != nil {
			return nil, err
		}
	}
	var result lwsv1.LeaderWorkerSet
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func TestActualKServeReconcileThroughInferenceAdmission(t *testing.T) {
	path := os.Getenv("INFERENCE_LWS_ADMISSION_READY_FILE")
	if path == "" {
		t.Skip("real Inference HTTPS/PG subprocess manifest is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest jointManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Cases) != 2 {
		t.Fatalf("expected actual shared and whole PG cases, got %d", len(manifest.Cases))
	}
	for _, fixture := range manifest.Cases {
		t.Run(fixture.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			var owner servingv1.LLMInferenceService
			if err := json.Unmarshal(fixture.Owner, &owner); err != nil {
				t.Fatal(err)
			}
			if owner.UID == "" || fixture.Groups != 2 || fixture.Size != 2 || len(fixture.OriginalContext) == 0 {
				t.Fatal("manifest lacks the real accepted PG topology/context/binding")
			}
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, rbacv1.AddToScheme, servingv1.AddToScheme, lwsv1.AddToScheme, gwapiv1.Install, igwapi.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, appsv1.SchemeGroupVersion, rbacv1.SchemeGroupVersion, servingv1.SchemeGroupVersion, lwsv1.GroupVersion})
			for _, gvk := range []schema.GroupVersionKind{corev1.SchemeGroupVersion.WithKind("Secret"), corev1.SchemeGroupVersion.WithKind("Service"), corev1.SchemeGroupVersion.WithKind("ServiceAccount"), appsv1.SchemeGroupVersion.WithKind("Deployment"), rbacv1.SchemeGroupVersion.WithKind("Role"), rbacv1.SchemeGroupVersion.WithKind("RoleBinding"), servingv1.SchemeGroupVersion.WithKind("LLMInferenceService"), lwsv1.GroupVersion.WithKind("LeaderWorkerSet")} {
				mapper.Add(gvk, meta.RESTScopeNamespace)
			}
			// Empty pipeline preset is explicit external cluster configuration;
			// it supplies no engine flags and preserves the caller's rendered argv.
			preset := &servingv1.LLMInferenceServiceConfig{ObjectMeta: metav1.ObjectMeta{Name: "kserve-config-llm-worker-pipeline-parallel", Namespace: owner.Namespace}}
			base := fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(&owner, preset).WithStatusSubresource(&servingv1.LLMInferenceService{}).Build()
			boundary := &admissionBoundary{Client: base, HTTP: admissionHTTPClient(t, "apiserver"), Address: manifest.Address, Username: requiredEnv(t, "INFERENCE_LWS_ADMISSION_CONTROLLER_USER")}
			config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.KServeNamespace}, Data: map[string]string{"ingress": `{"enableGatewayApi":true,"kserveIngressGateway":"kserve/kserve-ingress-gateway","ingressGateway":"knative-serving/knative-ingress-gateway","localGateway":"knative-serving/knative-local-gateway","localGatewayService":"knative-local-gateway.istio-system.svc.cluster.local"}`, "storageInitializer": `{"memoryRequest":"100Mi","memoryLimit":"1Gi","cpuRequest":"100m","cpuLimit":"1","cpuModelcar":"10m","memoryModelcar":"15Mi"}`}}
			reconciler := &llmisvc.LLMISVCReconciler{Client: boundary, EventRecorder: record.NewFakeRecorder(100), Clientset: clientsetfake.NewSimpleClientset(config)}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&owner)}
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatalf("actual KServe CREATE Reconcile: %v", err)
			}
			if len(boundary.Writes) != 1 || boundary.Writes[0].Operation != admissionv1.Create {
				t.Fatalf("actual first create not observed: %+v", boundary.Writes)
			}
			assertFrozenLWS(t, fixture, boundary.Writes[0].Object)
			current := &lwsv1.LeaderWorkerSet{}
			key := client.ObjectKey{Namespace: owner.Namespace, Name: owner.Name + "-kserve-mn"}
			if err := base.Get(ctx, key, current); err != nil {
				t.Fatal(err)
			}
			beforeDryRun := current.DeepCopy()
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatalf("actual unchanged/dry-run Reconcile: %v", err)
			}
			if err := base.Get(ctx, key, current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(beforeDryRun, current) {
				t.Fatal("KServe dry-run changed the persisted external object")
			}
			// External object drift forces the actual KServe full UPDATE path.
			delete(current.Annotations, queueKey)
			for _, pod := range []*corev1.PodTemplateSpec{current.Spec.LeaderWorkerTemplate.LeaderTemplate, &current.Spec.LeaderWorkerTemplate.WorkerTemplate} {
				for _, annotation := range fixture.Plan.Runtime.Annotations {
					delete(pod.Annotations, annotation.Key)
				}
				delete(pod.Annotations, queueKey)
			}
			if err := base.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatalf("actual KServe full UPDATE Reconcile: %v", err)
			}
			if err := base.Get(ctx, key, current); err != nil {
				t.Fatal(err)
			}
			assertFrozenLWS(t, fixture, current)
			creates, updates, dryRuns := 0, 0, 0
			for _, write := range boundary.Writes {
				assertFrozenLWS(t, fixture, write.Object)
				if write.DryRun {
					dryRuns++
				} else if write.Operation == admissionv1.Create {
					creates++
				} else {
					updates++
				}
			}
			if creates != 1 || updates != 1 || dryRuns < 2 {
				t.Fatalf("missing actual create/update/dry-run paths: %d/%d/%d", creates, updates, dryRuns)
			}
			for _, negative := range []struct {
				name               string
				mutate             func(*lwsv1.LeaderWorkerSet)
				username, identity string
			}{
				{"forged-owner-uid", func(o *lwsv1.LeaderWorkerSet) { o.OwnerReferences[0].UID = "forged-owner" }, boundary.Username, "apiserver"},
				{"untrusted-controller", func(*lwsv1.LeaderWorkerSet) {}, "system:serviceaccount:other:controller", "apiserver"},
				{"same-ca-wrong-client-dns", func(*lwsv1.LeaderWorkerSet) {}, boundary.Username, "untrusted"},
				{"missing-client-certificate", func(*lwsv1.LeaderWorkerSet) {}, boundary.Username, "none"},
			} {
				t.Run(negative.name, func(t *testing.T) {
					forged := current.DeepCopy()
					negative.mutate(forged)
					rejected := &admissionBoundary{HTTP: admissionHTTPClient(t, negative.identity), Address: manifest.Address}
					if _, err := rejected.admit(ctx, admissionv1.Update, forged, current, true, negative.username); err == nil {
						t.Fatal("actual HTTPS admission accepted forged/untrusted request")
					}
				})
			}
			badOld := current.DeepCopy()
			badOld.UID = "different-stored-lws"
			if _, err := boundary.admit(ctx, admissionv1.Update, current.DeepCopy(), badOld, true, boundary.Username); err == nil {
				t.Fatal("UPDATE accepted a changed old object UID")
			}
			result, err := json.MarshalIndent(struct {
				Case                      string `json:"case"`
				KServeVersion             string `json:"kserve_version"`
				Creates, Updates, DryRuns int
				Writes                    []admittedWrite `json:"actual_admitted_writes"`
			}{fixture.Name, "v0.16.0", creates, updates, dryRuns, boundary.Writes}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if dir := os.Getenv("C02_RESULT_DIR"); dir != "" {
				if err := os.WriteFile(dir+"/"+fixture.Name+"-actual-kserve-admission.json", result, 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("actual KServe0.16 Reconcile -> real HTTPS/PG admission: creates=%d updates=%d dry_runs=%d groups=%d size=%d", creates, updates, dryRuns, fixture.Groups, fixture.Size)
		})
	}
}

func assertFrozenLWS(t *testing.T, fixture jointCase, object *lwsv1.LeaderWorkerSet) {
	t.Helper()
	if object.Annotations[queueKey] != fixture.Plan.Runtime.Queue || object.Spec.Replicas == nil || *object.Spec.Replicas != fixture.Groups || object.Spec.LeaderWorkerTemplate.Size == nil || *object.Spec.LeaderWorkerTemplate.Size != fixture.Size {
		t.Fatal("first queue/group topology differs from actual PG snapshot")
	}
	if fixture.Name == "shared" && (fixture.Plan.Profile.Spec.Shared != 6144 || fixture.Plan.Encoding.F != 1024 || fixture.Plan.Encoding.Q != 6 || fixture.Plan.Totals.Shared != 24576) {
		t.Fatal("shared accepted MiB/F/q/total changed")
	}
	if fixture.Name == "whole" && (fixture.Plan.Profile.Spec.Cores != 100 || fixture.Plan.Encoding.Q != 0 || fixture.Plan.Encoding.Percentage != 100 || fixture.Plan.Totals.Exclusive != 4 || fixture.Plan.Totals.Shared != 0) {
		t.Fatal("whole accepted exclusive total/percentage changed")
	}
	for _, pod := range []*corev1.PodTemplateSpec{object.Spec.LeaderWorkerTemplate.LeaderTemplate, &object.Spec.LeaderWorkerTemplate.WorkerTemplate} {
		if pod == nil || pod.Spec.SchedulerName != fixture.Plan.Runtime.Scheduler || pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != fixture.Plan.Runtime.Class || pod.Annotations[queueKey] != fixture.Plan.Runtime.Queue {
			t.Fatal("leader/worker scheduler/queue/runtime class was lost")
		}
		for _, field := range fixture.Plan.Runtime.Labels {
			if pod.Spec.NodeSelector[field.Key] != field.Value {
				t.Fatalf("node selector %s lost", field.Key)
			}
		}
		for _, field := range fixture.Plan.Runtime.Annotations {
			if pod.Annotations[field.Key] != field.Value {
				t.Fatalf("GPU annotation %s lost", field.Key)
			}
		}
		var main *corev1.Container
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == "main" {
				main = &pod.Spec.Containers[i]
			}
		}
		if main == nil || !slices.Equal(main.Command, fixture.Command) || !slices.Equal(main.Args, fixture.Args) {
			t.Fatalf("actual KServe altered caller main argv: %+v", main)
		}
		for _, field := range fixture.Plan.Runtime.Limits {
			value, exists := main.Resources.Limits[corev1.ResourceName(field.Key)]
			if !exists || value.Cmp(resource.MustParse(field.Value)) != 0 {
				t.Fatalf("frozen limit %s lost", field.Key)
			}
		}
	}
}

func admissionHTTPClient(t *testing.T, identity string) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(requiredEnv(t, "INFERENCE_LWS_ADMISSION_CA_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid task-only CA")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca, ServerName: requiredEnv(t, "INFERENCE_LWS_ADMISSION_SERVER_DNS")}
	if identity != "none" {
		cert, err := tls.LoadX509KeyPair(requiredEnv(t, "C02_PKI_DIR")+"/"+identity+".pem", requiredEnv(t, "C02_PKI_DIR")+"/"+identity+".key")
		if err != nil {
			t.Fatal(err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func requiredEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required", key)
	}
	return value
}
