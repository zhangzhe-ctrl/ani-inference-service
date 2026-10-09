package kubernetes

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const ManagedLWSAdmissionPath = "/admission/managed-gpu-lws"

var ErrUnmanagedLWSOwner = errors.New("LLMI owner has no Governance managed GPU record")
var ErrManagedLWSBindingPending = errors.New("managed LLMI runtime binding is not recorded yet")

type ManagedLWSAdmissionConfig struct {
	Namespace, APIServerClientDNSName, ControllerUsername string
}

// The implementation derives tenant/resource from an observed PG binding,
// not from caller-supplied LWS labels or owner-reference metadata.
type ManagedLWSAdmissionSource interface {
	ManagedLWSRuntime(context.Context, string, string, string) (DesiredRuntime, error)
}

type managedLWSAPIServerIdentity struct{}

type managedLWSAdmission struct {
	source ManagedLWSAdmissionSource
	reader client.Reader
	config ManagedLWSAdmissionConfig
}

func NewManagedLWSAdmissionWebhook(source ManagedLWSAdmissionSource, reader client.Reader, cfg ManagedLWSAdmissionConfig) (http.Handler, error) {
	if source == nil || reader == nil || strings.TrimSpace(cfg.Namespace) == "" || strings.TrimSpace(cfg.APIServerClientDNSName) == "" || strings.TrimSpace(cfg.ControllerUsername) == "" {
		return nil, fmt.Errorf("managed LWS admission requires source, API reader, namespace and explicit API server/controller identities")
	}
	if !strings.HasPrefix(cfg.ControllerUsername, "system:serviceaccount:") || len(strings.Split(cfg.ControllerUsername, ":")) != 4 {
		return nil, fmt.Errorf("managed LWS controller identity must be an explicit service account username")
	}
	handler := &managedLWSAdmission{source: source, reader: reader, config: cfg}
	return &admission.Webhook{Handler: handler, WithContextFunc: func(ctx context.Context, request *http.Request) context.Context {
		state := request.TLS
		if state == nil || state.Version < tls.VersionTLS13 || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return ctx
		}
		leaf := state.PeerCertificates[0]
		if len(leaf.DNSNames) == 1 && leaf.DNSNames[0] == cfg.APIServerClientDNSName && len(leaf.IPAddresses) == 0 && len(leaf.URIs) == 0 && len(leaf.EmailAddresses) == 0 {
			return context.WithValue(ctx, managedLWSAPIServerIdentity{}, true)
		}
		return ctx
	}}, nil
}

func (h *managedLWSAdmission) Handle(ctx context.Context, req admission.Request) admission.Response {
	if trusted, _ := ctx.Value(managedLWSAPIServerIdentity{}).(bool); !trusted {
		return admission.Denied("verified API server identity is required")
	}
	if req.Namespace != h.config.Namespace {
		return admission.Allowed("outside the configured Inference namespace")
	}
	if req.Resource.Group != lwsv1.GroupVersion.Group || req.Resource.Version != lwsv1.GroupVersion.Version || req.Resource.Resource != "leaderworkersets" || req.SubResource != "" {
		return admission.Allowed("outside the managed LWS resource")
	}
	if req.Operation != admissionv1.Create && req.Operation != admissionv1.Update {
		return admission.Allowed("operation does not create or update an LWS")
	}
	var proposed lwsv1.LeaderWorkerSet
	if err := json.Unmarshal(req.Object.Raw, &proposed); err != nil || proposed.Name != req.Name || proposed.Namespace != req.Namespace {
		return admission.Denied("invalid LWS admission object identity")
	}
	owner, err := llmiControllerOwner(proposed.OwnerReferences)
	if err != nil {
		return admission.Denied(err.Error())
	}
	// An UPDATE cannot escape projection by removing or replacing the original
	// managed controller reference. OldObject is supplied by the verified API
	// server; the source still proves whether that old owner is managed in PG.
	if req.Operation == admissionv1.Update {
		var old lwsv1.LeaderWorkerSet
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil {
			return admission.Denied("invalid existing LWS object")
		}
		oldOwner, err := llmiControllerOwner(old.OwnerReferences)
		if err != nil {
			return admission.Denied(err.Error())
		}
		if owner != nil && oldOwner != nil && managedLWSFinalizersOnlyUpdate(&old, &proposed) {
			return admission.Allowed("finalizer-only update preserves the existing managed workload")
		}
		if oldOwner != nil && (owner == nil || oldOwner.UID != owner.UID || oldOwner.Name != owner.Name) {
			_, err := h.source.ManagedLWSRuntime(ctx, req.Namespace, oldOwner.Name, string(oldOwner.UID))
			if !errors.Is(err, ErrUnmanagedLWSOwner) {
				return admission.Denied("UPDATE cannot remove or replace a managed LLMI controller owner")
			}
		}
	}
	if owner == nil {
		return admission.Allowed("LWS is not controlled by a KServe LLMI")
	}
	// Live owner lookup prevents a supplied owner name/UID from inventing an
	// existing LLMI. The PG source separately verifies our observed binding.
	parent := newKServeLLMInferenceService(req.Namespace, owner.Name)
	if err := h.reader.Get(ctx, client.ObjectKeyFromObject(parent), parent); err != nil {
		return admission.Denied("LLMI owner could not be verified")
	}
	if parent.GetUID() != owner.UID || parent.GetDeletionTimestamp() != nil {
		return admission.Denied("LLMI owner UID differs from the live object or is deleting")
	}
	spec, err := h.source.ManagedLWSRuntime(ctx, req.Namespace, owner.Name, string(owner.UID))
	if errors.Is(err, ErrUnmanagedLWSOwner) {
		return admission.Allowed("verified LLMI has no managed GPU context")
	}
	if err != nil {
		return admission.Denied("managed LWS runtime context is unavailable: " + err.Error())
	}
	if req.UserInfo.Username != h.config.ControllerUsername {
		return admission.Denied("managed LWS writes require the configured KServe controller")
	}
	if !spec.ManagedGPU || spec.Namespace != req.Namespace || spec.Name != owner.Name || !managedLLMIBindingMatches(spec, parent) {
		return admission.Denied("LLMI owner is not the recorded managed runtime")
	}
	if spec.DesiredState != "running" {
		return admission.Denied("managed runtime is not requested to run")
	}
	update := req.Operation == admissionv1.Update
	if update {
		var old lwsv1.LeaderWorkerSet
		if err := json.Unmarshal(req.OldObject.Raw, &old); err != nil || old.UID == "" || old.Name != proposed.Name || old.Namespace != proposed.Namespace || proposed.UID != old.UID || old.ResourceVersion == "" || proposed.ResourceVersion != old.ResourceVersion {
			return admission.Denied("UPDATE must preserve the API server's existing LWS identity")
		}
		oldOwner, err := llmiControllerOwner(old.OwnerReferences)
		if err != nil || oldOwner == nil || oldOwner.UID != owner.UID || oldOwner.Name != owner.Name || oldOwner.APIVersion != owner.APIVersion || oldOwner.Kind != owner.Kind {
			return admission.Denied("UPDATE cannot replace the recorded LLMI controller owner")
		}
	}
	projected, err := projectManagedGPULeaderWorkerSet(spec.RuntimeSpec, &proposed, update)
	if err != nil {
		return admission.Denied(err.Error())
	}
	// CREATE, UPDATE and UPDATE dry-run take the same read-only path. The API
	// server applies this patch before storing or returning the defaulted LWS.
	patched, err := json.Marshal(projected)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// GC/finalizer removal must remain possible while the managed intent is
// closing or its LLMI owner is already gone. No PodSpec or GPU metadata change
// is admitted here. ResourceVersion and ManagedFields are the only ignored
// API-server bookkeeping fields; UID, owners, timestamps, labels, annotations
// and every other metadata field must remain equal.
func managedLWSFinalizersOnlyUpdate(old, proposed *lwsv1.LeaderWorkerSet) bool {
	if old.UID == "" || old.UID != proposed.UID || old.Name != proposed.Name || old.Namespace != proposed.Namespace || old.ResourceVersion == "" || proposed.ResourceVersion == "" || apiequality.Semantic.DeepEqual(old.Finalizers, proposed.Finalizers) {
		return false
	}
	before, after := old.DeepCopy(), proposed.DeepCopy()
	before.Finalizers, after.Finalizers = nil, nil
	before.ResourceVersion, after.ResourceVersion = "", ""
	before.ManagedFields, after.ManagedFields = nil, nil
	return apiequality.Semantic.DeepEqual(before, after)
}

func llmiControllerOwner(refs []metav1.OwnerReference) (*metav1.OwnerReference, error) {
	var owner *metav1.OwnerReference
	for i := range refs {
		ref := &refs[i]
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if owner != nil {
			return nil, fmt.Errorf("LWS has multiple controller owners")
		}
		owner = ref
	}
	if owner != nil {
		if owner.APIVersion != "serving.kserve.io/v1alpha1" || owner.Kind != "LLMInferenceService" {
			return nil, nil
		}
		if owner.Name == "" || owner.UID == "" {
			return nil, fmt.Errorf("LLMI controller owner name and UID are required")
		}
	}
	return owner, nil
}

func managedLLMIBindingMatches(spec DesiredRuntime, parent *unstructured.Unstructured) bool {
	for _, binding := range spec.Bindings {
		if binding.Kind == KServeLLMInferenceServiceKind && binding.Role == "runtime" && binding.Namespace == parent.GetNamespace() && binding.Name == parent.GetName() && binding.UID == string(parent.GetUID()) && binding.Generation <= spec.Generation {
			return true
		}
	}
	return false
}
