package postgres

// This adapter connects the domain operation runner to PostgreSQL. It does
// not implement quota or publication policy; those remain injected ports.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/audit"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/publication"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/quota"
	bizreconcile "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/reconcile"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/work"
)

type OperationStore struct{ pool *pgxpool.Pool }

func NewOperationStore(pool *pgxpool.Pool) *OperationStore { return &OperationStore{pool: pool} }

var _ inferencebiz.OperationStore = (*OperationStore)(nil)
var _ inferencebiz.AtomicStepStore = (*OperationStore)(nil)

// SaveGPUPlan persists one validated accelerator result while the worker still
// owns the resolve_gpu operation lease. The SQL predicate also permits an
// idempotent retry to write the same bytes, but rejects replacing a different
// plan for the same immutable generation.
func (s *OperationStore) SaveGPUPlan(ctx context.Context, op inferencebiz.OperationContext, plan *gpu.Plan) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	if op.GPURequest == nil {
		return errors.New("GPU request is required")
	}
	if err := gpu.ValidatePlan(plan, op.GPURequest); err != nil {
		return err
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	tenant, err := tenantUUID(op.TenantID)
	if err != nil {
		return err
	}
	service, err := workUUID("service_id", op.ServiceID)
	if err != nil {
		return err
	}
	operation, err := workUUID("operation_id", op.ID)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", op.LeaseToken)
	if err != nil {
		return err
	}
	rows, err := New(s.pool).SaveGPUPlanCAS(ctx, SaveGPUPlanCASParams{
		TenantID: tenant, ServiceID: service, OperationID: operation,
		TargetGeneration: op.TargetGeneration, LeaseToken: lease,
		GpuPlan: payload, GpuPlanDigest: pgtype.Text{String: plan.ResolutionDigest, Valid: true},
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrOperationCAS
	}
	return nil
}

func (s *OperationStore) AdvanceOperationStepCAS(ctx context.Context, in inferencebiz.StepTransition) error {
	return NewRepository(s.pool).AdvanceOperationStepCAS(ctx, operationStepInput(in))
}

func (s *OperationStore) RetryOperationStepCAS(ctx context.Context, in inferencebiz.StepTransition, after time.Duration) error {
	return NewRepository(s.pool).RetryOperationStepCAS(ctx, operationStepInput(in), after)
}

func operationStepInput(in inferencebiz.StepTransition) OperationStepInput {
	return OperationStepInput{TenantID: in.TenantID, OperationID: in.OperationID, Kind: in.Kind, LeaseToken: in.LeaseToken, TargetGeneration: in.TargetGeneration, ExpectedPhase: in.ExpectedPhase, ExpectedStep: in.ExpectedStep, NextPhase: in.NextPhase, NextStep: in.NextStep, ErrorCode: in.ErrorCode, ErrorMessage: in.ErrorMessage}
}

func (s *OperationStore) CurrentOperation(ctx context.Context, item work.Item) (inferencebiz.OperationContext, error) {
	if s == nil || s.pool == nil {
		return inferencebiz.OperationContext{}, errors.New("nil postgres operation store")
	}
	tenant, err := tenantUUID(item.TenantID)
	if err != nil {
		return inferencebiz.OperationContext{}, err
	}
	lease, err := workUUID("lease_token", item.LeaseToken)
	if err != nil {
		return inferencebiz.OperationContext{}, err
	}
	row, err := New(s.pool).GetLeasedOperation(ctx, GetLeasedOperationParams{TenantID: tenant, LeaseToken: lease})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return inferencebiz.OperationContext{}, work.ErrStaleGeneration
		}
		return inferencebiz.OperationContext{}, err
	}
	op := inferencebiz.OperationContext{TenantID: row.TenantID.String(), ServiceID: row.ServiceID.String(), ID: row.ID.String(), Kind: row.Kind, Phase: inferencebiz.OperationPhase(row.Phase), Step: row.Step, Attempt: row.Attempt, TargetGeneration: row.TargetGeneration, LeaseToken: item.LeaseToken}
	managed, err := New(s.pool).GetManagedGPUState(ctx, GetManagedGPUStateParams{TenantID: row.TenantID, ResourceID: row.ServiceID})
	if err != nil {
		return inferencebiz.OperationContext{}, err
	}
	op.ManagedGPU = managed.Managed
	// Load accelerator state from the immutable target generation. This is
	// intentionally best-effort for delete/legacy rows that have no spec; a
	// malformed persisted GPU snapshot remains an error so a worker cannot
	// project an unverified plan.
	if spec, specErr := New(s.pool).GetSpec(ctx, GetSpecParams{TenantID: tenant, ServiceID: row.ServiceID, Generation: row.TargetGeneration}); specErr == nil {
		op.Replicas, op.WorkerReplicas, op.RuntimeMode = spec.Replicas, spec.WorkerReplicas, spec.RuntimeMode
		if op.WorkerReplicas == 0 {
			op.WorkerReplicas = 1
		}
		if op.RuntimeMode == "" {
			op.RuntimeMode = "deployment"
		}
		if len(spec.GpuRequest) > 0 {
			op.GPURequest = new(gpu.Request)
			if err := json.Unmarshal(spec.GpuRequest, op.GPURequest); err != nil {
				return inferencebiz.OperationContext{}, fmt.Errorf("decode GPU request: %w", err)
			}
		}
		if len(spec.GpuPlan) > 0 {
			op.GPUPlan = new(gpu.Plan)
			if err := json.Unmarshal(spec.GpuPlan, op.GPUPlan); err != nil {
				return inferencebiz.OperationContext{}, fmt.Errorf("decode GPU plan: %w", err)
			}
			if spec.GpuPlanDigest.Valid && op.GPUPlan.ResolutionDigest != spec.GpuPlanDigest.String {
				return inferencebiz.OperationContext{}, fmt.Errorf("GPU plan digest does not match persisted digest")
			}
		}
	} else if !errors.Is(specErr, pgx.ErrNoRows) {
		return inferencebiz.OperationContext{}, specErr
	}
	// The request has already returned by the time a worker runs. Reload the
	// acceptance audit row so step events retain the durable caller context.
	if contextRow, contextErr := New(s.pool).GetOperationAuditContext(ctx, GetOperationAuditContextParams{TenantID: tenant, OperationID: row.ID}); contextErr == nil {
		op.Actor, op.RequestID = contextRow.Actor, contextRow.RequestID
	} else if !errors.Is(contextErr, pgx.ErrNoRows) {
		return inferencebiz.OperationContext{}, contextErr
	}
	reservation, err := New(s.pool).GetActiveQuotaReservation(ctx, GetActiveQuotaReservationParams{TenantID: tenant, ServiceID: row.ServiceID, Generation: row.TargetGeneration})
	if err == nil {
		op.Reservation, err = reservationFromRow(reservation, op)
		if err != nil {
			return inferencebiz.OperationContext{}, err
		}
		if err := s.populateQuotaDemand(ctx, &op.Reservation, tenant); err != nil {
			return inferencebiz.OperationContext{}, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return inferencebiz.OperationContext{}, err
	}
	if op.Kind == "update" || op.Kind == "restart" {
		previous, previousErr := New(s.pool).GetPreviousQuotaReservation(ctx, GetPreviousQuotaReservationParams{TenantID: tenant, ServiceID: row.ServiceID, Generation: row.TargetGeneration})
		if previousErr == nil {
			op.PreviousReservation, err = reservationFromRow(previous, op)
			if err != nil {
				return inferencebiz.OperationContext{}, err
			}
			if err := s.populateQuotaDemand(ctx, &op.PreviousReservation, tenant); err != nil {
				return inferencebiz.OperationContext{}, err
			}
		} else if !errors.Is(previousErr, pgx.ErrNoRows) {
			return inferencebiz.OperationContext{}, previousErr
		}
	}
	var pub InferencePublication
	if op.Step == string(inferencebiz.StepPublish) {
		// Replacement withdraws the old generation, but publishing must load
		// only the target generation so it cannot revive the previous route.
		pub, err = New(s.pool).GetPublication(ctx, GetPublicationParams{TenantID: tenant, ServiceID: row.ServiceID, Generation: row.TargetGeneration})
	} else {
		pub, err = New(s.pool).GetLatestPublication(ctx, GetLatestPublicationParams{TenantID: tenant, ServiceID: row.ServiceID, Generation: row.TargetGeneration})
	}
	if err == nil {
		op.Publication = publication.Publication{TenantID: pub.TenantID.String(), ServiceID: pub.ServiceID.String(), OperationID: row.ID.String(), Generation: pub.Generation, LeaseToken: item.LeaseToken, FenceOperationID: row.ID.String(), URL: pub.InvocationUrl, State: pub.ObservedPhase}
	} else if errors.Is(err, pgx.ErrNoRows) {
		op.Publication = publication.Publication{TenantID: item.TenantID, ServiceID: item.ServiceID, OperationID: row.ID.String(), Generation: row.TargetGeneration, LeaseToken: item.LeaseToken, FenceOperationID: row.ID.String(), State: "withdrawn"}
	} else {
		return inferencebiz.OperationContext{}, err
	}
	return op, nil
}

func reservationFromRow(row InferenceQuotaReservation, op inferencebiz.OperationContext) (quota.Reservation, error) {
	reservation := quota.Reservation{
		TenantID: row.TenantID.String(), ServiceID: row.ServiceID.String(), OperationID: row.OperationID.String(),
		Generation: strconv.FormatInt(row.Generation, 10), ReservationID: row.ReservationID,
		LeaseToken: op.LeaseToken, FenceOperationID: op.ID, State: row.State,
		LastErrorCode: row.LastErrorCode,
	}
	spec, err := decodeResourceSpec(row.RequestedResources)
	if err != nil {
		return quota.Reservation{}, fmt.Errorf("decode quota resource snapshot: %w", err)
	}
	reservation.RequestedResources = spec
	return reservation, nil
}

func decodeResourceSpec(payload []byte) (resources.Spec, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var spec *resources.Spec
	if err := decoder.Decode(&spec); err != nil {
		return resources.Spec{}, err
	}
	if spec == nil {
		return resources.Spec{}, fmt.Errorf("object required")
	}
	normalized, err := resources.Normalize(*spec)
	if err != nil {
		return resources.Spec{}, err
	}
	return resources.Spec{Requests: normalized.Requests, Limits: normalized.Limits, GPU: normalized.GPU}, nil
}

func (s *OperationStore) populateQuotaDemand(ctx context.Context, reservation *quota.Reservation, tenant pgtype.UUID) error {
	service, err := workUUID("service_id", reservation.ServiceID)
	if err != nil {
		return err
	}
	generation, err := strconv.ParseInt(reservation.Generation, 10, 64)
	if err != nil || generation < 1 {
		return fmt.Errorf("invalid reservation generation")
	}
	q := New(s.pool)
	spec, err := q.GetSpec(ctx, GetSpecParams{TenantID: tenant, ServiceID: service, Generation: generation})
	if errors.Is(err, pgx.ErrNoRows) {
		spec, err = q.GetLatestSpec(ctx, GetLatestSpecParams{TenantID: tenant, ServiceID: service, Generation: generation})
	}
	if err != nil {
		return fmt.Errorf("load quota demand spec: %w", err)
	}
	demandSpec, err := decodeResourceSpec(spec.Resources)
	if err != nil {
		return fmt.Errorf("decode quota demand spec: %w", err)
	}
	workers := spec.WorkerReplicas
	if workers == 0 {
		workers = 1
	}
	demand, err := resources.Aggregate(demandSpec, spec.Replicas, spec.RuntimeMode, workers)
	if err != nil {
		return fmt.Errorf("aggregate quota demand: %w", err)
	}
	reservation.Demand = demand
	return nil
}

func (s *OperationStore) SaveQuotaReservation(ctx context.Context, in quota.Reservation) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	tenant, err := tenantUUID(in.TenantID)
	if err != nil {
		return err
	}
	op, err := workUUID("operation_id", in.OperationID)
	if err != nil {
		return err
	}
	gen, err := strconv.ParseInt(in.Generation, 10, 64)
	if err != nil || gen < 1 {
		return fmt.Errorf("invalid reservation generation")
	}
	payload, err := json.Marshal(in.RequestedResources)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", in.LeaseToken)
	if err != nil {
		return err
	}
	fenceOperationID := in.FenceOperationID
	if fenceOperationID == "" {
		fenceOperationID = in.OperationID
	}
	fence, err := workUUID("fence_operation_id", fenceOperationID)
	if err != nil {
		return err
	}
	rows, err := New(s.pool).UpdateQuotaReservationFenced(ctx, UpdateQuotaReservationFencedParams{TenantID: tenant, OperationID: op, ReservationID: in.ReservationID, RequestedResources: payload, State: in.State, LastErrorCode: in.LastErrorCode, Generation: gen, FenceOperationID: fence, LeaseToken: lease})
	if err != nil {
		return err
	}
	if rows != 1 {
		return bizreconcile.ErrStaleGeneration
	}
	return nil
}

func (s *OperationStore) SavePublication(ctx context.Context, in publication.Publication) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	tenant, err := tenantUUID(in.TenantID)
	if err != nil {
		return err
	}
	service, err := workUUID("service_id", in.ServiceID)
	if err != nil {
		return err
	}
	state := in.State
	if state != "withdrawn" && state != "publishing" && state != "published" && state != "withdrawing" {
		return fmt.Errorf("invalid publication state %q", state)
	}
	// Intermediate states are durable desired intent. They must not overwrite
	// the last provider-confirmed observed state; otherwise a failed withdraw
	// would look withdrawn and runtime deletion could proceed unsafely. For a
	// first publication row, the safe observed baseline is withdrawn.
	observedState := state
	if state == "publishing" || state == "withdrawing" {
		observedState = "withdrawn"
	}
	fenceOperationID := in.FenceOperationID
	if fenceOperationID == "" {
		fenceOperationID = in.OperationID
	}
	op, err := workUUID("fence_operation_id", fenceOperationID)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", in.LeaseToken)
	if err != nil {
		return err
	}
	rows, err := New(s.pool).UpsertPublicationFenced(ctx, UpsertPublicationFencedParams{TenantID: tenant, ServiceID: service, FenceOperationID: op, Generation: in.Generation, DesiredPhase: state, ObservedPhase: observedState, InvocationUrl: in.URL, LastErrorCode: "", LeaseToken: lease})
	if err != nil {
		return err
	}
	if rows != 1 {
		return bizreconcile.ErrStaleGeneration
	}
	return nil
}

func (s *OperationStore) SaveRuntimeObservation(ctx context.Context, op inferencebiz.OperationContext, fact inferencebiz.RuntimeObservation) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	phase, health := "degraded", "unknown"
	if fact.Absent {
		phase, health = "stopped", "unknown"
	} else if fact.Ready {
		phase = "ready"
	}
	return NewReconcileStore(s.pool).SaveObservationForWork(ctx, work.Item{TenantID: op.TenantID, ServiceID: op.ServiceID, Generation: op.TargetGeneration, LeaseToken: op.LeaseToken}, bizObservation(op, phase, health, fact))
}

func (s *OperationStore) SaveModelObservation(ctx context.Context, op inferencebiz.OperationContext, fact inferencebiz.ModelObservation) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	if !fact.Known {
		return fmt.Errorf("model observation is not known")
	}
	tenant, err := tenantUUID(op.TenantID)
	if err != nil {
		return err
	}
	service, err := workUUID("service_id", op.ServiceID)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", op.LeaseToken)
	if err != nil {
		return err
	}
	rows, err := New(s.pool).UpdateModelObservationForWork(ctx, UpdateModelObservationForWorkParams{
		Generation: op.TargetGeneration, ModelReady: fact.Ready, Reason: fact.Reason,
		TenantID: tenant, ServiceID: service, LeaseToken: lease,
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return bizreconcile.ErrStaleGeneration
	}
	return nil
}

func (s *OperationStore) MarkModelMaterializing(ctx context.Context, op inferencebiz.OperationContext) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	tenant, err := tenantUUID(op.TenantID)
	if err != nil {
		return err
	}
	service, err := workUUID("service_id", op.ServiceID)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", op.LeaseToken)
	if err != nil {
		return err
	}
	rows, err := New(s.pool).MarkModelMaterializingForWork(ctx, MarkModelMaterializingForWorkParams{
		Generation: op.TargetGeneration, TenantID: tenant, ServiceID: service, LeaseToken: lease,
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return bizreconcile.ErrStaleGeneration
	}
	return nil
}

func bizObservation(op inferencebiz.OperationContext, phase, health string, fact inferencebiz.RuntimeObservation) bizreconcile.Observation {
	if fact.RuntimePhase != "" {
		phase = fact.RuntimePhase
	}
	return bizreconcile.Observation{Generation: op.TargetGeneration, RuntimePhase: phase, RuntimeMode: fact.RuntimeMode, ReadyReplicas: fact.ReadyReplicas, ReadyGroups: fact.ReadyGroups, ReadyWorkers: fact.ReadyWorkers, LWSUID: fact.LWSUID, Objects: fact.Objects, ModelReady: fact.ModelReady, ModelReadyKnown: fact.ModelReadyKnown, InvocationHealth: health, Reason: fact.Reason}
}

func (s *OperationStore) AdvanceOperationStepCASWithAudit(ctx context.Context, in inferencebiz.StepTransition, event audit.Event) error {
	return s.stepWithAudit(ctx, in, event, false, 0)
}
func (s *OperationStore) RetryOperationStepCASWithAudit(ctx context.Context, in inferencebiz.StepTransition, after time.Duration, event audit.Event) error {
	return s.stepWithAudit(ctx, in, event, true, after)
}

func (s *OperationStore) stepWithAudit(ctx context.Context, in inferencebiz.StepTransition, event audit.Event, retry bool, after time.Duration) error {
	if s == nil || s.pool == nil {
		return errors.New("nil postgres operation store")
	}
	tenant, err := tenantUUID(in.TenantID)
	if err != nil {
		return err
	}
	op, err := workUUID("operation_id", in.OperationID)
	if err != nil {
		return err
	}
	lease, err := workUUID("lease_token", in.LeaseToken)
	if err != nil {
		return err
	}
	if in.TargetGeneration < 1 {
		return fmt.Errorf("target generation must be positive")
	}
	if event.TenantID != in.TenantID || event.OperationID != in.OperationID || event.Generation != in.TargetGeneration {
		return fmt.Errorf("audit identity does not match operation transition")
	}
	if !retry {
		if err := inferencebiz.ValidateOperationStepTransition(in.Kind, in.ExpectedPhase, in.ExpectedStep, in.NextPhase, in.NextStep); err != nil {
			return err
		}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := New(tx)
	var rows int64
	if retry {
		rows, err = q.RetryOperationStepCAS(ctx, RetryOperationStepCASParams{RetrySeconds: after.Seconds(), ErrorCode: in.ErrorCode, ErrorMessage: in.ErrorMessage, TenantID: tenant, OperationID: op, TargetGeneration: in.TargetGeneration, ExpectedPhase: string(in.ExpectedPhase), ExpectedStep: in.ExpectedStep, LeaseToken: lease})
	} else {
		rows, err = q.AdvanceOperationStepCAS(ctx, AdvanceOperationStepCASParams{NextPhase: string(in.NextPhase), NextStep: in.NextStep, ErrorCode: in.ErrorCode, ErrorMessage: in.ErrorMessage, TenantID: tenant, OperationID: op, TargetGeneration: in.TargetGeneration, ExpectedPhase: string(in.ExpectedPhase), ExpectedStep: in.ExpectedStep, LeaseToken: lease})
	}
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrOperationCAS
	}
	current, err := q.GetOperation(ctx, GetOperationParams{TenantID: tenant, ID: op})
	if err != nil {
		return err
	}
	if current.ServiceID.String() != event.ServiceID {
		return fmt.Errorf("audit service does not match operation")
	}
	if !retry && in.Kind == "delete" && in.NextPhase == inferencebiz.OperationSucceeded {
		rows, err := q.MarkServiceDeleted(ctx, MarkServiceDeletedParams{TenantID: tenant, ID: current.ServiceID, CurrentOperationID: op, DesiredGeneration: in.TargetGeneration})
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrOperationCAS
		}
	}
	eTenant, err := tenantUUID(event.TenantID)
	if err != nil {
		return err
	}
	eService, err := workUUID("service_id", event.ServiceID)
	if err != nil {
		return err
	}
	eOperation, err := workUUID("operation_id", event.OperationID)
	if err != nil {
		return err
	}
	eID, err := uuid.Parse(event.EventID)
	if err != nil {
		return err
	}
	if err := q.AppendAuditEvent(ctx, AppendAuditEventParams{TenantID: eTenant, EventID: pgtype.UUID{Bytes: eID, Valid: true}, ServiceID: eService, OperationID: eOperation, Generation: event.Generation, EventType: event.EventType, Actor: event.Actor, RequestID: event.RequestID, BeforeState: event.BeforeState, AfterState: event.AfterState, Payload: jsonOrEmpty(event.Payload, []byte("{}"))}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
