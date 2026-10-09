// Package gpu contains the transport-neutral accelerator request and plan
// contract. It deliberately has no Kubernetes or protobuf dependency.
package gpu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
)

// ResolveInput is the caller-owned context for one accelerator resolution.
// The request is optional at the inference boundary: callers must not invoke a
// Resolver when Request is nil. A non-nil request is always resolved through
// the configured accelerator service; there is intentionally no CPU fallback.
type ResolveInput struct {
	TenantID  string
	RequestID string
	Actor     string
	Request   *Request
}

// Resolver resolves a caller's explicit GPU request into an immutable plan.
// Implementations must not mutate the request or manufacture engine flags.
type Resolver interface {
	Resolve(context.Context, ResolveInput) (*Plan, error)
}

type Request struct {
	ClusterID         string `json:"cluster_id"`
	PoolID            string `json:"pool_id"`
	ProfileID         string `json:"profile_id"`
	ProfileVersion    uint64 `json:"profile_version"`
	Replicas          uint32 `json:"replicas"`
	DevicesPerReplica uint32 `json:"devices_per_replica"`
	ContainerName     string `json:"container_name"`
}
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type ProfileSpec struct {
	GroupID              string `json:"group_id"`
	Mode                 int32  `json:"mode"`
	ModelKey             string `json:"model_key"`
	SharedMemoryMiB      int64  `json:"shared_memory_mib"`
	CoreLimitPercent     uint32 `json:"core_limit_percent"`
	MaxDevicesPerReplica uint32 `json:"max_devices_per_replica"`
	IsolationClass       string `json:"isolation_class"`
}
type Profile struct {
	ProfileID      string       `json:"profile_id"`
	ProfileVersion uint64       `json:"profile_version"`
	DisplayName    string       `json:"display_name"`
	Spec           *ProfileSpec `json:"spec"`
	BaselineID     string       `json:"baseline_id"`
	SpecDigest     string       `json:"spec_digest"`
	Published      bool         `json:"published"`
}
type MemoryEncoding struct {
	MemoryBlockMiB        uint32 `json:"memory_block_mib"`
	MemoryBlocksPerDevice int64  `json:"memory_blocks_per_device"`
	SharedMemoryMiB       int64  `json:"shared_memory_mib"`
	MemoryPercentage      uint32 `json:"memory_percentage"`
	Policy                string `json:"policy"`
}
type ResourceTotals struct {
	LogicalDeviceCount   int64 `json:"logical_device_count"`
	ExclusiveDeviceCount int64 `json:"exclusive_device_count"`
	SharedMemoryMiB      int64 `json:"shared_memory_mib"`
}
type RuntimeFragment struct {
	SchedulerName      string     `json:"scheduler_name"`
	QueueName          string     `json:"queue_name"`
	NodeLabels         []KeyValue `json:"node_labels"`
	PodAnnotations     []KeyValue `json:"pod_annotations"`
	LimitsPerContainer []KeyValue `json:"limits_per_container"`
	RuntimeClassName   string     `json:"runtime_class_name"`
	RecipeVersion      string     `json:"recipe_version"`
}
type Plan struct {
	SchemaVersion    uint32           `json:"schema_version"`
	Request          *Request         `json:"request"`
	Profile          *Profile         `json:"profile"`
	Encoding         *MemoryEncoding  `json:"encoding"`
	Totals           *ResourceTotals  `json:"totals"`
	Runtime          *RuntimeFragment `json:"runtime"`
	BaselineDigest   string           `json:"baseline_digest"`
	ResolutionDigest string           `json:"resolution_digest"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var namePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func ValidateRequest(r *Request) error {
	if r == nil || !uuidPattern.MatchString(r.ClusterID) || !uuidPattern.MatchString(r.PoolID) || !uuidPattern.MatchString(r.ProfileID) || r.ProfileVersion != 1 {
		return fmt.Errorf("invalid GPU request")
	}
	if r.Replicas < 1 || r.Replicas > 16 || r.DevicesPerReplica != 1 || len(r.ContainerName) == 0 || len(r.ContainerName) > 63 || !namePattern.MatchString(r.ContainerName) {
		return fmt.Errorf("unsupported GPU request shape")
	}
	return nil
}

// ValidateTopology keeps the accelerator request aligned with the two KServe
// pod templates. Runtime replicas are LWS groups; the accelerator request's
// replicas is the resulting GPU-bearing Pod count.
func ValidateTopology(r *Request, replicas int32, mode string, workers int32) error {
	if err := ValidateRequest(r); err != nil {
		return err
	}
	if replicas < 1 {
		return fmt.Errorf("replicas must be positive")
	}
	expectedPods, err := TotalPods(replicas, mode, workers)
	if err != nil {
		return err
	}
	if r.Replicas != uint32(expectedPods) {
		return fmt.Errorf("GPU replicas must match GPU-bearing pod count %d", expectedPods)
	}
	switch mode {
	case "", "deployment":
		if workers > 1 || workers < 0 {
			return fmt.Errorf("worker_replicas is only valid for leader_worker_set")
		}
		if r.ContainerName != "kserve-container" {
			return fmt.Errorf("deployment GPU container must be kserve-container")
		}
	case "leader_worker_set":
		if workers < 1 {
			return fmt.Errorf("leader_worker_set requires workers")
		}
		if r.ContainerName != "main" {
			return fmt.Errorf("leader_worker_set GPU container must be main")
		}
	default:
		return fmt.Errorf("unsupported runtime mode %q", mode)
	}
	return nil
}

// TotalPods returns the number of pods carrying the GPU request. For LWS,
// replicas is the number of groups and each group has one leader plus workers.
func TotalPods(replicas int32, mode string, workers int32) (int32, error) {
	if replicas < 1 {
		return 0, fmt.Errorf("replicas must be positive")
	}
	switch mode {
	case "", "deployment":
		if workers > 1 || workers < 0 {
			return 0, fmt.Errorf("worker_replicas is only valid for leader_worker_set")
		}
		return replicas, nil
	case "leader_worker_set":
		if workers < 1 {
			return 0, fmt.Errorf("leader_worker_set requires workers")
		}
		total := int64(replicas) * (int64(workers) + 1)
		if total > math.MaxInt32 {
			return 0, fmt.Errorf("GPU topology overflows pod count")
		}
		return int32(total), nil
	default:
		return 0, fmt.Errorf("unsupported runtime mode %q", mode)
	}
}

// PlanDigest implements the accelerator acc-c14n-v1 shape: integer JSON values
// are represented as decimal strings, object keys are lexical, and digest input
// is prefixed to prevent accidental cross-protocol hash reuse.
func PlanDigest(p *Plan) (string, error) {
	if p == nil || p.Request == nil || p.Profile == nil || p.Runtime == nil {
		return "", fmt.Errorf("invalid GPU plan")
	}
	c := *p
	c.ResolutionDigest = ""
	profile := *p.Profile
	profile.Published = false
	c.Profile = &profile
	runtime := *p.Runtime
	var err error
	if runtime.NodeLabels, err = sorted(runtime.NodeLabels); err != nil {
		return "", err
	}
	if runtime.PodAnnotations, err = sorted(runtime.PodAnnotations); err != nil {
		return "", err
	}
	if runtime.LimitsPerContainer, err = sorted(runtime.LimitsPerContainer); err != nil {
		return "", err
	}
	c.Runtime = &runtime
	b, err := canonical(c)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(append([]byte("acc-c14n-v1\n"), b...))
	return hex.EncodeToString(h[:]), nil
}
func ValidatePlan(p *Plan, request *Request) error {
	if p == nil || p.SchemaVersion != 1 || request == nil || p.Request == nil || *p.Request != *request {
		return fmt.Errorf("invalid GPU plan")
	}
	d, err := PlanDigest(p)
	if err != nil {
		return err
	}
	if d != p.ResolutionDigest {
		return fmt.Errorf("GPU plan digest mismatch")
	}
	return nil
}

// ValidateManagedPlan adds static metering/runtime consistency checks at the
// trusted owner boundary. Resolution authority remains Governance/Accelerator;
// this validates their frozen snapshot without querying today's catalog.
func ValidateManagedPlan(p *Plan, request *Request) error {
	if err := ValidateRequest(request); err != nil {
		return err
	}
	if err := ValidatePlan(p, request); err != nil {
		return err
	}
	if p.Profile == nil || p.Profile.Spec == nil || p.Encoding == nil || p.Totals == nil || p.Runtime == nil {
		return fmt.Errorf("incomplete GPU plan")
	}
	s, e, t, r := p.Profile.Spec, p.Encoding, p.Totals, p.Runtime
	if p.Profile.ProfileID != request.ProfileID || p.Profile.ProfileVersion != request.ProfileVersion || !uuidPattern.MatchString(s.GroupID) || !uuidPattern.MatchString(p.Profile.BaselineID) || s.ModelKey == "" || s.MaxDevicesPerReplica != 1 || s.CoreLimitPercent < 1 || s.CoreLimitPercent > 100 || s.IsolationClass == "" {
		return fmt.Errorf("GPU profile does not match request")
	}
	if r.SchedulerName != "volcano" || r.QueueName == "" || !namePattern.MatchString(r.QueueName) || r.RecipeVersion != "volcano-hami-v1" || e.Policy != "EXACT" || e.MemoryBlockMiB == 0 {
		return fmt.Errorf("unsupported frozen GPU runtime or encoding")
	}
	specBytes, err := canonical(map[string]any{"spec": s, "baseline_id": p.Profile.BaselineID})
	if err != nil {
		return err
	}
	specSum := sha256.Sum256(append([]byte("acc-c14n-v1\n"), specBytes...))
	if hex.EncodeToString(specSum[:]) != p.Profile.SpecDigest {
		return fmt.Errorf("GPU spec digest mismatch")
	}
	if len(p.BaselineDigest) != 64 {
		return fmt.Errorf("GPU baseline digest is required")
	}
	limits := make(map[string]string)
	for _, field := range r.LimitsPerContainer {
		limits[field.Key] = field.Value
	}
	if limits["volcano.sh/vgpu-number"] != "1" || limits["volcano.sh/vgpu-cores"] != strconv.FormatUint(uint64(s.CoreLimitPercent), 10) {
		return fmt.Errorf("GPU runtime limits mismatch")
	}
	switch s.Mode {
	case 1:
		if s.SharedMemoryMiB != 0 || s.CoreLimitPercent != 100 || e.MemoryPercentage != 100 || e.SharedMemoryMiB != 0 || e.MemoryBlocksPerDevice != 0 || t.LogicalDeviceCount != 0 || t.ExclusiveDeviceCount != int64(request.Replicas) || t.SharedMemoryMiB != 0 || limits["volcano.sh/vgpu-memory-percentage"] != "100" || len(limits) != 3 {
			return fmt.Errorf("whole GPU metering mismatch")
		}
	case 2:
		if s.SharedMemoryMiB <= 0 || s.SharedMemoryMiB > math.MaxInt32 || s.SharedMemoryMiB%int64(e.MemoryBlockMiB) != 0 || e.MemoryBlocksPerDevice != s.SharedMemoryMiB/int64(e.MemoryBlockMiB) || e.MemoryBlocksPerDevice <= 0 || e.MemoryBlocksPerDevice > math.MaxInt32 || e.SharedMemoryMiB != s.SharedMemoryMiB || e.MemoryPercentage != 0 || t.LogicalDeviceCount != int64(request.Replicas) || t.ExclusiveDeviceCount != 0 || t.SharedMemoryMiB != int64(request.Replicas)*s.SharedMemoryMiB || limits["volcano.sh/vgpu-memory"] != strconv.FormatInt(e.MemoryBlocksPerDevice, 10) || len(limits) != 3 {
			return fmt.Errorf("shared GPU metering mismatch")
		}
	default:
		return fmt.Errorf("unsupported GPU supply mode")
	}
	labels := make(map[string]string)
	for _, field := range r.NodeLabels {
		labels[field.Key] = field.Value
	}
	if labels["accelerator.ani.io/supply-group"] != s.GroupID || labels["accelerator.ani.io/model-key"] != s.ModelKey || labels["accelerator.ani.io/baseline-id"] != p.Profile.BaselineID || len(labels) != 3 {
		return fmt.Errorf("GPU frozen selectors mismatch")
	}
	annotations := make(map[string]string)
	for _, field := range r.PodAnnotations {
		annotations[field.Key] = field.Value
	}
	if annotations["volcano.sh/vgpu-mode"] != "hami-core" {
		return fmt.Errorf("GPU runtime annotation mismatch")
	}
	return nil
}
func sorted(in []KeyValue) ([]KeyValue, error) {
	c := append([]KeyValue(nil), in...)
	sort.Slice(c, func(i, j int) bool { return c[i].Key < c[j].Key })
	for i, v := range c {
		if v.Key == "" || v.Value == "" || (i > 0 && c[i-1].Key == v.Key) {
			return nil, fmt.Errorf("duplicate or empty GPU plan key")
		}
	}
	return c, nil
}
func canonical(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var x any
	if e = d.Decode(&x); e != nil {
		return nil, e
	}
	x, e = numbers(x)
	if e != nil {
		return nil, e
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if e := encoder.Encode(x); e != nil {
		return nil, e
	}
	return bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), nil
}
func numbers(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if _, e := x.Int64(); e != nil {
			return nil, e
		}
		return x.String(), nil
	case map[string]any:
		for k, vv := range x {
			n, e := numbers(vv)
			if e != nil {
				return nil, e
			}
			x[k] = n
		}
	case []any:
		for i, vv := range x {
			n, e := numbers(vv)
			if e != nil {
				return nil, e
			}
			x[i] = n
		}
	}
	return v, nil
}
