package kubernetes

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

type admissionRuntimeFixture struct {
	runtime DesiredRuntime
	err     error
}

func (f admissionRuntimeFixture) ManagedLWSRuntime(context.Context, string, string, string) (DesiredRuntime, error) {
	return f.runtime, f.err
}

func managedAdmissionFixture(t *testing.T) (*managedLWSAdmission, admission.Request, *lwsv1.LeaderWorkerSet) {
	t.Helper()
	spec, input := managedLWSFixture(t, false)
	owner := metav1.OwnerReference{APIVersion: "serving.kserve.io/v1alpha1", Kind: "LLMInferenceService", Name: spec.Name, UID: "observed-owner", Controller: ptr.To(true)}
	input.OwnerReferences = []metav1.OwnerReference{owner}
	parent := newKServeLLMInferenceService(spec.Namespace, spec.Name)
	parent.SetUID(owner.UID)
	reader := fake.NewClientBuilder().WithRuntimeObjects(parent).Build()
	desired := DesiredRuntime{RuntimeSpec: spec, DesiredState: "running", Bindings: []RuntimeBinding{{Kind: KServeLLMInferenceServiceKind, Namespace: spec.Namespace, Name: spec.Name, UID: string(owner.UID), Role: "runtime", Generation: 1}}}
	handler := &managedLWSAdmission{source: admissionRuntimeFixture{runtime: desired}, reader: reader, config: ManagedLWSAdmissionConfig{Namespace: spec.Namespace, APIServerClientDNSName: "test-api.internal", ControllerUsername: "system:serviceaccount:kserve:controller"}}
	raw, _ := json.Marshal(input)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{UID: "request-1", Name: input.Name, Namespace: input.Namespace, Resource: metav1.GroupVersionResource{Group: lwsv1.GroupVersion.Group, Version: "v1", Resource: "leaderworkersets"}, Operation: admissionv1.Create, Object: runtime.RawExtension{Raw: raw}, UserInfo: authenticationv1.UserInfo{Username: handler.config.ControllerUsername}}}
	return handler, req, input
}

func TestManagedLWSAdmissionProjectsCreateUpdateAndDryRun(t *testing.T) {
	for _, operation := range []admissionv1.Operation{admissionv1.Create, admissionv1.Update} {
		for _, dryRun := range []bool{false, true} {
			h, req, input := managedAdmissionFixture(t)
			req.Operation, req.DryRun = operation, ptr.To(dryRun)
			if operation == admissionv1.Update {
				input.UID, input.ResourceVersion = "old-lws", "3"
				old, _ := json.Marshal(input)
				req.OldObject.Raw = old
			}
			req.Object.Raw, _ = json.Marshal(input)
			before := input.DeepCopy()
			ctx := context.WithValue(context.Background(), managedLWSAPIServerIdentity{}, true)
			response := h.Handle(ctx, req)
			if !response.Allowed {
				t.Fatalf("%s dryrun=%v denied: %+v", operation, dryRun, response.Result)
			}
			if err := response.Complete(req); err != nil {
				t.Fatal(err)
			}
			patch, err := jsonpatch.DecodePatch(response.Patch)
			if err != nil {
				t.Fatal(err)
			}
			patched, err := patch.Apply(req.Object.Raw)
			if err != nil {
				t.Fatal(err)
			}
			var out lwsv1.LeaderWorkerSet
			if err := json.Unmarshal(patched, &out); err != nil {
				t.Fatal(err)
			}
			if out.Annotations["scheduling.volcano.sh/queue-name"] != "gpu" || out.Spec.LeaderWorkerTemplate.LeaderTemplate.Annotations["volcano.sh/vgpu-mode"] != "hami-core" || out.Spec.LeaderWorkerTemplate.WorkerTemplate.Annotations["volcano.sh/vgpu-mode"] != "hami-core" {
				t.Fatalf("incomplete synchronous projection: %+v", out)
			}
			if !reflect.DeepEqual(before.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Command, out.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Command) || !reflect.DeepEqual(before.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Args, out.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Args) {
				t.Fatal("admission changed engine argv")
			}
			if out.UID != input.UID || out.ResourceVersion != input.ResourceVersion {
				t.Fatal("admission changed existing object identity")
			}
		}
	}
}

func TestManagedLWSAdmissionRejectsForgeryAndPendingBinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*managedLWSAdmission, *admission.Request, *lwsv1.LeaderWorkerSet)
	}{
		{"controller", func(_ *managedLWSAdmission, r *admission.Request, _ *lwsv1.LeaderWorkerSet) {
			r.UserInfo.Username = "system:serviceaccount:other:controller"
		}},
		{"owner UID", func(_ *managedLWSAdmission, _ *admission.Request, l *lwsv1.LeaderWorkerSet) {
			l.OwnerReferences[0].UID = "forged"
		}},
		{"PG binding pending", func(h *managedLWSAdmission, _ *admission.Request, _ *lwsv1.LeaderWorkerSet) {
			h.source = admissionRuntimeFixture{err: ErrManagedLWSBindingPending}
		}},
		{"PG binding mismatch", func(h *managedLWSAdmission, _ *admission.Request, _ *lwsv1.LeaderWorkerSet) {
			f := h.source.(admissionRuntimeFixture)
			f.runtime.Bindings[0].UID = "other"
			h.source = f
		}},
		{"topology", func(_ *managedLWSAdmission, _ *admission.Request, l *lwsv1.LeaderWorkerSet) {
			l.Spec.Replicas = ptr.To[int32](3)
		}},
		{"update UID", func(_ *managedLWSAdmission, r *admission.Request, l *lwsv1.LeaderWorkerSet) {
			r.Operation = admissionv1.Update
			old := l.DeepCopy()
			old.UID = "old"
			old.ResourceVersion = "1"
			r.OldObject.Raw, _ = json.Marshal(old)
			l.UID = "replacement"
			l.ResourceVersion = "1"
		}},
		{"remove managed owner", func(_ *managedLWSAdmission, r *admission.Request, l *lwsv1.LeaderWorkerSet) {
			r.Operation = admissionv1.Update
			old := l.DeepCopy()
			old.UID = "old"
			old.ResourceVersion = "1"
			r.OldObject.Raw, _ = json.Marshal(old)
			l.UID, l.ResourceVersion = "old", "1"
			l.OwnerReferences = nil
		}},
		{"replace managed owner", func(_ *managedLWSAdmission, r *admission.Request, l *lwsv1.LeaderWorkerSet) {
			r.Operation = admissionv1.Update
			old := l.DeepCopy()
			old.UID = "old"
			old.ResourceVersion = "1"
			r.OldObject.Raw, _ = json.Marshal(old)
			l.UID, l.ResourceVersion = "old", "1"
			l.OwnerReferences[0].Kind = "Deployment"
			l.OwnerReferences[0].APIVersion = "apps/v1"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, req, obj := managedAdmissionFixture(t)
			tc.change(h, &req, obj)
			req.Object.Raw, _ = json.Marshal(obj)
			r := h.Handle(context.WithValue(context.Background(), managedLWSAPIServerIdentity{}, true), req)
			if r.Allowed {
				t.Fatal("forged/pending managed object allowed")
			}
		})
	}
	h, req, _ := managedAdmissionFixture(t)
	if h.Handle(context.Background(), req).Allowed {
		t.Fatal("metadata-only admission allowed")
	}
	// Input labels cannot switch the tenant chosen from the durable binding.
	req.Object.Raw = []byte(`{}`)
	if h.Handle(context.WithValue(context.Background(), managedLWSAPIServerIdentity{}, true), req).Allowed {
		t.Fatal("missing identity object allowed")
	}
}

func TestManagedLWSAdmissionLeavesProvenUnmanagedOwnerUnchanged(t *testing.T) {
	h, req, _ := managedAdmissionFixture(t)
	h.source = admissionRuntimeFixture{err: ErrUnmanagedLWSOwner}
	req.UserInfo.Username = "ordinary-user"
	response := h.Handle(context.WithValue(context.Background(), managedLWSAPIServerIdentity{}, true), req)
	if !response.Allowed || len(response.Patches) != 0 {
		t.Fatalf("CPU/direct behavior changed: %+v", response)
	}
}

func TestManagedLWSAdmissionAllowsOnlyFinalizerChangesDuringCleanup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*lwsv1.LeaderWorkerSet)
		allowed bool
	}{
		{"finalizers only", nil, true},
		{"GPU pod spec", func(l *lwsv1.LeaderWorkerSet) {
			l.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Image = "replacement"
		}, false},
		{"queue metadata", func(l *lwsv1.LeaderWorkerSet) {
			l.Annotations = map[string]string{"scheduling.volcano.sh/queue-name": "other"}
		}, false},
		{"owner removed", func(l *lwsv1.LeaderWorkerSet) { l.OwnerReferences = nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, req, obj := managedAdmissionFixture(t)
			req.Operation = admissionv1.Update
			req.UserInfo.Username = "system:serviceaccount:kube-system:garbage-collector"
			obj.UID, obj.ResourceVersion = "existing-lws", "7"
			obj.Finalizers = []string{"external-controller/cleanup"}
			old := obj.DeepCopy()
			req.OldObject.Raw, _ = json.Marshal(old)
			obj.Finalizers = nil
			obj.ResourceVersion = "8"
			obj.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "api-server"}}
			if tc.mutate != nil {
				tc.mutate(obj)
			}
			req.Object.Raw, _ = json.Marshal(obj)
			// Even closing context cannot block a pure cleanup write. Any other
			// change must still reach the ordinary fail-closed managed path.
			h.source = admissionRuntimeFixture{err: ErrManagedLWSBindingPending}
			r := h.Handle(context.WithValue(context.Background(), managedLWSAPIServerIdentity{}, true), req)
			if r.Allowed != tc.allowed || (r.Allowed && len(r.Patches) != 0) {
				t.Fatalf("cleanup allowed=%v patches=%v result=%+v", r.Allowed, r.Patches, r.Result)
			}
		})
	}
}

func TestManagedLWSAdmissionHTTPAuthenticatesAPIServerBeforeUserInfo(t *testing.T) {
	h, req, _ := managedAdmissionFixture(t)
	for _, tc := range []struct {
		name     string
		dns      []string
		verified bool
		allowed  bool
	}{
		{"registered", []string{"test-api.internal"}, true, true}, {"wrong same CA", []string{"other.internal"}, true, false}, {"ambiguous", []string{"test-api.internal", "other.internal"}, true, false}, {"unverified", []string{"test-api.internal"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			webhook, err := NewManagedLWSAdmissionWebhook(h.source, h.reader, h.config)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Request: &req.AdmissionRequest})
			r := httptest.NewRequest("POST", ManagedLWSAdmissionPath, bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			leaf := &x509.Certificate{DNSNames: tc.dns}
			r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}}
			if tc.verified {
				r.TLS.VerifiedChains = [][]*x509.Certificate{{leaf}}
			}
			recorder := httptest.NewRecorder()
			webhook.ServeHTTP(recorder, r)
			var response admissionv1.AdmissionReview
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Response == nil || response.Response.Allowed != tc.allowed || response.Response.UID != types.UID("request-1") {
				t.Fatalf("response=%+v", response.Response)
			}
		})
	}
}
