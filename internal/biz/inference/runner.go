package inference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/audit"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/publication"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/quota"
	bizreconcile "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/reconcile"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/work"
)

var (
	ErrOperationProviderMissing = errors.New("operation provider is not configured")
	ErrOperationNotReady        = errors.New("operation provider has not reached the required state")
)

// OperationContext is the tenant-scoped durable projection needed by one runner
// step. The store must load it from PostgreSQL using the work item's lease and
// generation; a process-local operation queue is deliberately not supported.
type OperationContext struct {
	TenantID, ServiceID, ID, Kind string
	// Actor and RequestID are copied from the durable acceptance audit event.
	// They let asynchronous step events retain the original caller context
	// after the request process has returned.
	Actor, RequestID    string
	LeaseToken          string
	Phase               OperationPhase
	Step                string
	Attempt             int32
	TargetGeneration    int64
	Reservation         quota.Reservation
	PreviousReservation quota.Reservation
	Publication         publication.Publication
	// GPURequest and GPUPlan are loaded from the immutable target generation.
	// A plan is only present after StepResolveGPU has durably saved it.
	GPURequest     *gpu.Request
	GPUPlan        *gpu.Plan
	ManagedGPU     bool
	Replicas       int32
	WorkerReplicas int32
	RuntimeMode    string
}

// RuntimeObservation contains facts returned by the runtime adapter. Ready
// and Absent are intentionally separate: a successful delete request is not
// evidence that the object has disappeared yet.
type RuntimeObservation struct {
	Ready           bool
	RuntimePhase    string
	RuntimeMode     string
	ReadyReplicas   int32
	ReadyGroups     int32
	ReadyWorkers    int32
	LWSUID          string
	ModelReady      bool
	ModelReadyKnown bool
	Absent          bool
	Reason          string
	Objects         []bizreconcile.RuntimeObject
}

// ModelObservation is the durable fact returned by the model materializer.
// Known=false means the provider could not yet establish a result and the
// operation must remain retryable.
type ModelObservation struct {
	Ready  bool
	Known  bool
	Reason string
}

// OperationStore is the durable operation boundary. Implementations normally
// adapt these methods to sqlc queries in one local transaction with the audit
// event and fact projection. Every method receives tenant and generation via
// Operation/StepTransition; implementations must preserve those predicates.
type OperationStore interface {
	CurrentOperation(context.Context, work.Item) (OperationContext, error)
	AdvanceOperationStepCAS(context.Context, StepTransition) error
	RetryOperationStepCAS(context.Context, StepTransition, time.Duration) error
	SaveQuotaReservation(context.Context, quota.Reservation) error
	SavePublication(context.Context, publication.Publication) error
	SaveRuntimeObservation(context.Context, OperationContext, RuntimeObservation) error
}

// ModelObservationStore persists model materialization without overwriting
// runtime, publication, or invocation fields owned by other observers.
type ModelObservationStore interface {
	MarkModelMaterializing(context.Context, OperationContext) error
	SaveModelObservation(context.Context, OperationContext, ModelObservation) error
}

// GPUPlanStore durably saves the accelerator result behind the operation
// lease/generation fence. It is only needed when resource.gpu is present; a
// requested GPU operation fails closed when the store does not implement it.
type GPUPlanStore interface {
	SaveGPUPlan(context.Context, OperationContext, *gpu.Plan) error
}

// AtomicStepStore is an optional stronger adapter. Production PostgreSQL
// implementations should use it so the operation CAS and audit event commit
// in one local transaction. The fallback path still requires an idempotent
// audit sink and is useful for incremental adapter rollout.
type AtomicStepStore interface {
	AdvanceOperationStepCASWithAudit(context.Context, StepTransition, audit.Event) error
	RetryOperationStepCASWithAudit(context.Context, StepTransition, time.Duration, audit.Event) error
}

// StepTransition is the provider result that the store fences with
// expected phase/step and target generation. It mirrors the persisted CAS
// fields without importing a data adapter into the domain package.
type StepTransition struct {
	TenantID, OperationID, Kind string
	LeaseToken                  string
	TargetGeneration            int64
	ExpectedPhase               OperationPhase
	ExpectedStep                string
	NextPhase                   OperationPhase
	NextStep                    string
	ErrorCode                   string
	ErrorMessage                string
}

type AdmissionPort interface {
	Admit(context.Context, OperationContext) error
}

type ModelPort interface {
	EnsureModel(context.Context, OperationContext) (ModelObservation, error)
}

// GPUResolver resolves the request through the accelerator service. The
// resolver is never contacted for an operation without resource.gpu.
type GPUResolver interface {
	ResolveGPU(context.Context, OperationContext) (*gpu.Plan, error)
}

type RuntimePort interface {
	ApplyCR(context.Context, OperationContext) error
	ApplyRuntime(context.Context, OperationContext) error
	ObserveRuntime(context.Context, OperationContext) (RuntimeObservation, error)
	DeleteRuntime(context.Context, OperationContext) error
	ObserveAbsence(context.Context, OperationContext) (RuntimeObservation, error)
	DeleteCR(context.Context, OperationContext) error
}

// Runner executes exactly one durable operation step for a claimed work
// item. It never treats provider absence as success. The surrounding
// work.Worker owns the resource_work lease and calls Retry when Run returns an
// error; Runner also retries the operation row so its step remains durable.
type Runner struct {
	Store       OperationStore
	Admission   AdmissionPort
	Model       ModelPort
	GPU         GPUResolver
	Quota       quota.Port
	Publication publication.Port
	Runtime     RuntimePort
	Audit       audit.Sink
	RetryAfter  time.Duration
}

func (r *Runner) Execute(ctx context.Context, item work.Item) (work.Result, error) {
	if err := r.validate(item); err != nil {
		return work.Result{}, err
	}
	op, err := r.Store.CurrentOperation(ctx, item)
	if err != nil {
		return work.Result{}, err
	}
	if op.TenantID != item.TenantID || op.ServiceID != item.ServiceID || op.TargetGeneration != item.Generation || op.ID == "" {
		return work.Result{}, work.ErrStaleGeneration
	}
	op.LeaseToken = item.LeaseToken
	if op.Phase == OperationSucceeded || op.Phase == OperationFailed {
		return work.Result{Generation: item.Generation}, nil
	}
	if op.Phase != OperationPending && op.Phase != OperationRunning {
		return work.Result{}, fmt.Errorf("unsupported operation phase %q", op.Phase)
	}
	if op.Phase == OperationPending {
		if err := r.transition(ctx, op, OperationPhase(OperationRunning), op.Step, "claimed", ""); err != nil {
			return work.Result{}, r.retry(ctx, op, err)
		}
		op.Phase = OperationRunning
	}

	next, err := r.runStep(ctx, op)
	if err != nil {
		return work.Result{}, r.retry(ctx, op, err)
	}
	if err := r.transition(ctx, op, next.phase, next.step, next.event, next.errorMessage); err != nil {
		return work.Result{}, r.retry(ctx, op, err)
	}
	return work.Result{Generation: item.Generation}, nil
}

type stepResult struct {
	phase        OperationPhase
	step         string
	event        string
	errorMessage string
}

func (r *Runner) runStep(ctx context.Context, op OperationContext) (stepResult, error) {
	switch op.Step {
	case string(StepAdmission):
		if r.Admission == nil {
			return stepResult{}, fmt.Errorf("%w: admission", ErrOperationProviderMissing)
		}
		if err := r.Admission.Admit(ctx, op); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepReserveQuota), event: "admission.accepted"}, nil

	case string(StepReserveQuota):
		if op.ManagedGPU {
			return stepResult{phase: OperationRunning, step: nextAfterQuota(op), event: "quota.governance_owned"}, nil
		}
		if r.Quota == nil {
			return stepResult{phase: OperationRunning, step: nextAfterQuota(op), event: "quota.skipped"}, nil
		}
		// A durable reservation may already have crossed the provider boundary
		// before this worker lost its lease.  Never call Reserve again for a
		// reservation that is already reserved or confirmed: Reserve must be
		// idempotent for the initial pending request, while a persisted reserved
		// result is recovered by Confirm alone.
		reservation := op.Reservation
		if reservation.ReservationID == "" {
			return stepResult{}, fmt.Errorf("quota reservation is missing")
		}
		if err := validateReservation(op, reservation); err != nil {
			return stepResult{}, err
		}
		if reservation.State == "confirmed" {
			return stepResult{phase: OperationRunning, step: nextAfterQuota(op), event: "quota.already_confirmed"}, nil
		}
		recovered := false
		if reservation.State != "reserved" {
			remote, lookupErr := r.Quota.Get(ctx, reservation.TenantID, reservation.ReservationID)
			if lookupErr == nil && remote.ReservationID != "" {
				if err := validateReservation(op, remote); err != nil {
					return stepResult{}, err
				}
				reservation = remote
				recovered = true
			} else if lookupErr != nil && !errors.Is(lookupErr, quota.ErrReservationNotFound) {
				return stepResult{}, lookupErr
			}
		}
		if reservation.State == "confirmed" {
			if recovered {
				reservation.LeaseToken = op.LeaseToken
				reservation.FenceOperationID = op.ID
				reservation.LastErrorCode = ""
				if err := r.Store.SaveQuotaReservation(ctx, reservation); err != nil {
					return stepResult{}, err
				}
			}
			return stepResult{phase: OperationRunning, step: nextAfterQuota(op), event: "quota.recovered_confirmed"}, nil
		}
		if reservation.State == "reserved" {
			if err := r.Quota.Confirm(ctx, reservation); err != nil {
				reservation.LeaseToken = op.LeaseToken
				reservation.FenceOperationID = op.ID
				reservation.LastErrorCode = "quota_confirm_failed"
				return stepResult{}, errors.Join(err, r.Store.SaveQuotaReservation(ctx, reservation))
			}
		} else {
			var err error
			reservation, err = r.Quota.Reserve(ctx, reservation)
			if err != nil {
				return stepResult{}, err
			}
			if err := validateReservation(op, reservation); err != nil {
				return stepResult{}, err
			}
			if err := r.Quota.Confirm(ctx, reservation); err != nil {
				reservation.State = "reserved"
				reservation.LeaseToken = op.LeaseToken
				reservation.FenceOperationID = op.ID
				reservation.LastErrorCode = "quota_confirm_failed"
				if saveErr := r.Store.SaveQuotaReservation(ctx, reservation); saveErr != nil {
					return stepResult{}, errors.Join(err, saveErr)
				}
				return stepResult{}, err
			}
		}
		reservation.State = "confirmed"
		reservation.LeaseToken = op.LeaseToken
		reservation.FenceOperationID = op.ID
		reservation.LastErrorCode = ""
		if err := r.Store.SaveQuotaReservation(ctx, reservation); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: nextAfterQuota(op), event: "quota.confirmed"}, nil

	case string(StepResolveGPU):
		if op.ManagedGPU && op.GPURequest == nil {
			return stepResult{}, fmt.Errorf("managed GPU operation has no frozen GPU request")
		}
		// A missing GPU request means this generation does not use a GPU. It
		// must not contact the accelerator service or add scheduling/runtime
		// fields; the caller's engine and command remain unchanged.
		if op.GPURequest == nil {
			return stepResult{phase: OperationRunning, step: string(StepApplyCR), event: "gpu.skipped"}, nil
		}
		if err := gpu.ValidateTopology(op.GPURequest, op.Replicas, op.RuntimeMode, op.WorkerReplicas); err != nil {
			return stepResult{}, fmt.Errorf("validate GPU request: %w", err)
		}
		if op.GPUPlan != nil {
			validate := gpu.ValidatePlan
			if op.ManagedGPU {
				validate = gpu.ValidateManagedPlan
			}
			if err := validate(op.GPUPlan, op.GPURequest); err != nil {
				return stepResult{}, fmt.Errorf("validate persisted GPU plan: %w", err)
			}
			return stepResult{phase: OperationRunning, step: string(StepApplyCR), event: "gpu.reused"}, nil
		}
		if op.ManagedGPU {
			return stepResult{}, fmt.Errorf("managed GPU operation has no frozen Governance plan")
		}
		if r.GPU == nil {
			return stepResult{}, fmt.Errorf("%w: GPU resolver", ErrOperationProviderMissing)
		}
		store, ok := r.Store.(GPUPlanStore)
		if !ok {
			return stepResult{}, fmt.Errorf("%w: GPU plan store", ErrOperationProviderMissing)
		}
		plan, err := r.GPU.ResolveGPU(ctx, op)
		if err != nil {
			return stepResult{}, err
		}
		if err := gpu.ValidateTopology(op.GPURequest, op.Replicas, op.RuntimeMode, op.WorkerReplicas); err != nil {
			return stepResult{}, fmt.Errorf("validate GPU request: %w", err)
		}
		if err := gpu.ValidatePlan(plan, op.GPURequest); err != nil {
			return stepResult{}, fmt.Errorf("validate resolved GPU plan: %w", err)
		}
		if err := store.SaveGPUPlan(ctx, op, plan); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepApplyCR), event: "gpu.resolved"}, nil

	case string(StepReleasePreviousQuota):
		if op.ManagedGPU {
			return stepResult{phase: OperationRunning, step: string(StepReserveQuota), event: "previous_quota.governance_owned"}, nil
		}
		if r.Quota == nil {
			return stepResult{phase: OperationRunning, step: string(StepReserveQuota), event: "previous_quota.skipped"}, nil
		}
		if op.PreviousReservation.ReservationID == "" {
			return stepResult{phase: OperationRunning, step: string(StepReserveQuota), event: "previous_quota.none"}, nil
		}
		if err := r.Quota.Release(ctx, op.PreviousReservation); err != nil {
			op.PreviousReservation.State = "release_pending"
			op.PreviousReservation.LeaseToken = op.LeaseToken
			op.PreviousReservation.FenceOperationID = op.ID
			op.PreviousReservation.LastErrorCode = "quota_release_failed"
			return stepResult{}, errors.Join(err, r.Store.SaveQuotaReservation(ctx, op.PreviousReservation))
		}
		op.PreviousReservation.State = "released"
		op.PreviousReservation.LeaseToken = op.LeaseToken
		op.PreviousReservation.FenceOperationID = op.ID
		op.PreviousReservation.LastErrorCode = ""
		if err := r.Store.SaveQuotaReservation(ctx, op.PreviousReservation); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepReserveQuota), event: "previous_quota.released"}, nil

	case string(StepApplyCR):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		if err := r.Runtime.ApplyCR(ctx, op); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepMaterializeModel), event: "cr.applied"}, nil

	case string(StepMaterializeModel):
		if r.Model == nil {
			err := fmt.Errorf("%w: model", ErrOperationProviderMissing)
			recordModelMaterialization(ctx, 0, err)
			return stepResult{}, err
		}
		store, ok := r.Store.(ModelObservationStore)
		if !ok {
			err := fmt.Errorf("%w: model observation store", ErrOperationProviderMissing)
			recordModelMaterialization(ctx, 0, err)
			return stepResult{}, err
		}
		if err := store.MarkModelMaterializing(ctx, op); err != nil {
			return stepResult{}, err
		}
		started := time.Now()
		observation, err := r.Model.EnsureModel(ctx, op)
		if err != nil {
			recordModelMaterialization(ctx, time.Since(started), err)
			return stepResult{}, err
		}
		if !observation.Known || !observation.Ready {
			notReadyErr := fmt.Errorf("%w: model materialization: %s", ErrOperationNotReady, observation.Reason)
			recordModelMaterialization(ctx, time.Since(started), notReadyErr)
			return stepResult{}, notReadyErr
		}
		recordModelMaterialization(ctx, time.Since(started), nil)
		if err := store.SaveModelObservation(ctx, op, observation); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepApplyRuntime), event: "model.materialized"}, nil

	case string(StepApplyRuntime):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		if err := r.Runtime.ApplyRuntime(ctx, op); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepObserveRuntime), event: "runtime.apply_requested"}, nil

	case string(StepObserveRuntime):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		observation, err := r.Runtime.ObserveRuntime(ctx, op)
		if err != nil {
			return stepResult{}, err
		}
		if err := r.Store.SaveRuntimeObservation(ctx, op, observation); err != nil {
			return stepResult{}, err
		}
		if !observation.Ready || !observation.ModelReadyKnown || !observation.ModelReady {
			return stepResult{}, fmt.Errorf("%w: runtime/model: %s", ErrOperationNotReady, observation.Reason)
		}
		return stepResult{phase: OperationRunning, step: string(StepPublish), event: "runtime.ready"}, nil

	case string(StepWithdrawPublication):
		if r.Publication == nil {
			return stepResult{}, fmt.Errorf("%w: publication", ErrOperationProviderMissing)
		}
		// Persist intent before the remote call. A process crash after this
		// write but before Withdraw is recoverable because the same durable step
		// is retried; a crash before the write would lose the safety gate.
		op.Publication.State = "withdrawing"
		op.Publication.LeaseToken = op.LeaseToken
		op.Publication.FenceOperationID = op.ID
		if err := r.Store.SavePublication(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		if err := r.Publication.Withdraw(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		confirmed, err := r.Publication.ConfirmWithdrawn(ctx, op.Publication)
		if err != nil {
			return stepResult{}, err
		}
		if !confirmed {
			return stepResult{}, fmt.Errorf("%w: publication withdrawal", ErrOperationNotReady)
		}
		op.Publication.State = "withdrawn"
		op.Publication.LeaseToken = op.LeaseToken
		op.Publication.FenceOperationID = op.ID
		if err := r.Store.SavePublication(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepDeleteRuntime), event: "publication.withdrawn"}, nil

	case string(StepDeleteRuntime):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		if err := r.Runtime.DeleteRuntime(ctx, op); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepObserveAbsence), event: "runtime.delete_requested"}, nil

	case string(StepObserveAbsence):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		observation, err := r.Runtime.ObserveAbsence(ctx, op)
		if err != nil {
			return stepResult{}, err
		}
		if err := r.Store.SaveRuntimeObservation(ctx, op, observation); err != nil {
			return stepResult{}, err
		}
		if !observation.Absent {
			return stepResult{}, fmt.Errorf("%w: runtime deletion: %s", ErrOperationNotReady, observation.Reason)
		}
		next := string(StepReserveQuota)
		if op.Kind == "update" || op.Kind == "restart" {
			next = string(StepReleasePreviousQuota)
		}
		if op.Kind == "stop" || op.Kind == "delete" {
			next = string(StepReleaseQuota)
		}
		if op.Kind == "delete" {
			next = string(StepDeleteCR)
		}
		return stepResult{phase: OperationRunning, step: next, event: "runtime.absent"}, nil

	case string(StepDeleteCR):
		if r.Runtime == nil {
			return stepResult{}, fmt.Errorf("%w: runtime", ErrOperationProviderMissing)
		}
		if err := r.Runtime.DeleteCR(ctx, op); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationRunning, step: string(StepReleaseQuota), event: "cr.deleted"}, nil

	case string(StepPublish):
		if r.Publication == nil {
			return stepResult{}, fmt.Errorf("%w: publication", ErrOperationProviderMissing)
		}
		op.Publication.State = "publishing"
		op.Publication.LeaseToken = op.LeaseToken
		op.Publication.FenceOperationID = op.ID
		if err := r.Store.SavePublication(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		if err := r.Publication.Publish(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		confirmed, err := r.Publication.ConfirmPublished(ctx, op.Publication)
		if err != nil {
			return stepResult{}, err
		}
		if !confirmed {
			return stepResult{}, fmt.Errorf("%w: publication", ErrOperationNotReady)
		}
		if resolver, ok := r.Publication.(publication.EndpointResolver); ok {
			endpoint, err := resolver.Endpoint(ctx, op.Publication)
			if err != nil {
				return stepResult{}, err
			}
			if endpoint == "" {
				return stepResult{}, fmt.Errorf("published endpoint is empty")
			}
			op.Publication.URL = endpoint
		}
		op.Publication.State = "published"
		op.Publication.LeaseToken = op.LeaseToken
		op.Publication.FenceOperationID = op.ID
		if err := r.Store.SavePublication(ctx, op.Publication); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationSucceeded, step: string(StepComplete), event: "publication.published"}, nil

	case string(StepReleaseQuota):
		if op.ManagedGPU {
			return stepResult{phase: OperationSucceeded, step: string(StepComplete), event: "quota.governance_owned"}, nil
		}
		// The store returns only outstanding reservations. A prior stop, or
		// a retry after the release was persisted, can leave none to release.
		if op.Reservation.ReservationID == "" {
			return stepResult{phase: OperationSucceeded, step: string(StepComplete), event: "quota.none"}, nil
		}
		if r.Quota == nil {
			return stepResult{phase: OperationSucceeded, step: string(StepComplete), event: "quota.skipped"}, nil
		}
		if err := r.Quota.Release(ctx, op.Reservation); err != nil {
			op.Reservation.State = "release_pending"
			op.Reservation.LeaseToken = op.LeaseToken
			op.Reservation.FenceOperationID = op.ID
			op.Reservation.LastErrorCode = "quota_release_failed"
			if saveErr := r.Store.SaveQuotaReservation(ctx, op.Reservation); saveErr != nil {
				return stepResult{}, errors.Join(err, saveErr)
			}
			return stepResult{}, err
		}
		op.Reservation.State = "released"
		op.Reservation.LeaseToken = op.LeaseToken
		op.Reservation.FenceOperationID = op.ID
		op.Reservation.LastErrorCode = ""
		if err := r.Store.SaveQuotaReservation(ctx, op.Reservation); err != nil {
			return stepResult{}, err
		}
		return stepResult{phase: OperationSucceeded, step: string(StepComplete), event: "quota.released"}, nil
	default:
		return stepResult{}, fmt.Errorf("unsupported operation step %q", op.Step)
	}
}

func (r *Runner) transition(ctx context.Context, op OperationContext, phase OperationPhase, step, event, message string) error {
	payload, _ := json.Marshal(map[string]string{"step": step, "event": event})
	auditEvent := audit.Event{TenantID: op.TenantID, EventID: operationEventID(op, phase, step), ServiceID: op.ServiceID, OperationID: op.ID, Generation: op.TargetGeneration, EventType: "inference.operation." + event, Actor: op.Actor, RequestID: op.RequestID, BeforeState: operationState(op.Phase, op.Step, ""), AfterState: operationState(phase, step, message), Payload: payload}
	transition := StepTransition{TenantID: op.TenantID, OperationID: op.ID, Kind: op.Kind, LeaseToken: op.LeaseToken, TargetGeneration: op.TargetGeneration, ExpectedPhase: op.Phase, ExpectedStep: op.Step, NextPhase: phase, NextStep: step, ErrorMessage: message}
	if atomic, ok := r.Store.(AtomicStepStore); ok {
		return atomic.AdvanceOperationStepCASWithAudit(ctx, transition, auditEvent)
	}
	if r.Audit == nil {
		return fmt.Errorf("%w: audit", ErrOperationProviderMissing)
	}
	// The sink must de-duplicate EventID. This is intentionally only a
	// compatibility fallback; the atomic adapter is the production contract.
	if err := r.Audit.Append(ctx, auditEvent); err != nil {
		return err
	}
	return r.Store.AdvanceOperationStepCAS(ctx, transition)
}

func (r *Runner) retry(ctx context.Context, op OperationContext, cause error) error {
	transition := StepTransition{TenantID: op.TenantID, OperationID: op.ID, Kind: op.Kind, LeaseToken: op.LeaseToken, TargetGeneration: op.TargetGeneration, ExpectedPhase: op.Phase, ExpectedStep: op.Step, ErrorCode: "provider_retry", ErrorMessage: cause.Error()}
	payload, _ := json.Marshal(map[string]string{"step": op.Step, "error": cause.Error()})
	auditEvent := audit.Event{TenantID: op.TenantID, EventID: operationEventID(op, OperationPending, op.Step), ServiceID: op.ServiceID, OperationID: op.ID, Generation: op.TargetGeneration, EventType: "inference.operation.step.retry", Actor: op.Actor, RequestID: op.RequestID, BeforeState: operationState(op.Phase, op.Step, ""), AfterState: operationState(OperationPending, op.Step, cause.Error()), Payload: payload}
	var err error
	if atomic, ok := r.Store.(AtomicStepStore); ok {
		err = atomic.RetryOperationStepCASWithAudit(ctx, transition, r.retryAfter(), auditEvent)
	} else {
		if r.Audit == nil {
			return errors.Join(cause, fmt.Errorf("%w: audit", ErrOperationProviderMissing))
		}
		if auditErr := r.Audit.Append(ctx, auditEvent); auditErr != nil {
			return errors.Join(cause, auditErr)
		}
		err = r.Store.RetryOperationStepCAS(ctx, transition, r.retryAfter())
	}
	if err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func operationState(phase OperationPhase, step, message string) []byte {
	state, _ := json.Marshal(map[string]string{"phase": string(phase), "step": step, "message": message})
	return state
}

func operationEventID(op OperationContext, phase OperationPhase, step string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, fmt.Appendf(nil, "%s|%s|%s|%d", op.ID, phase, step, op.Attempt)).String()
}

func validateReservation(op OperationContext, reservation quota.Reservation) error {
	if reservation.TenantID != op.TenantID || reservation.ServiceID != op.ServiceID || reservation.OperationID != op.ID || reservation.Generation != strconv.FormatInt(op.TargetGeneration, 10) || reservation.ReservationID == "" {
		return fmt.Errorf("quota reservation identity does not match operation")
	}
	return nil
}

func (r *Runner) retryAfter() time.Duration {
	if r.RetryAfter <= 0 {
		return 5 * time.Second
	}
	return r.RetryAfter
}

func nextAfterQuota(op OperationContext) string {
	// Every running generation crosses the durable gate. The gate itself is a
	// no-op for an omitted resource.gpu and never contacts the accelerator.
	return string(StepResolveGPU)
}

func (r *Runner) validate(item work.Item) error {
	if r == nil || r.Store == nil {
		return fmt.Errorf("%w: operation store", ErrOperationProviderMissing)
	}
	if item.TenantID == "" || item.ServiceID == "" || item.Generation < 1 || item.LeaseToken == "" {
		return work.ErrStaleGeneration
	}
	return nil
}
