package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
)

// CommandUseCase durably accepts lifecycle commands. Runtime changes happen
// later in the worker; this transaction only advances desired state and adds
// a fenced generation/work item.
type CommandUseCase struct{ repo *Repository }

func NewCommandUseCase(repo *Repository) *CommandUseCase { return &CommandUseCase{repo: repo} }

func (u *CommandUseCase) Command(ctx context.Context, in inferencebiz.CommandInput) (*inferencev1.OperationResponse, error) {
	if u == nil || u.repo == nil || u.repo.pool == nil {
		return nil, errors.New("nil postgres command repository")
	}
	if in.ManagedGPU != nil {
		return u.acceptManagedGPUDelete(ctx, in)
	}
	tenant, err := parseUUID("tenant_id", in.TenantID, false)
	if err != nil {
		return nil, err
	}
	serviceID, err := parseUUID("service_id", in.ServiceID, false)
	if err != nil {
		return nil, err
	}
	if in.ExpectedGeneration < 1 {
		return nil, inferencebiz.ErrGenerationConflict
	}
	if in.Kind != "start" && in.Kind != "stop" && in.Kind != "restart" && in.Kind != "delete" {
		return nil, fmt.Errorf("%w: %s", inferencebiz.ErrInvalidState, in.Kind)
	}
	opID := pgUUID(uuid.New())
	tx, err := u.repo.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	q := New(tx)
	if in.RequestID != "" {
		if err := q.LockIdempotency(ctx, LockIdempotencyParams{TenantID: in.TenantID, Method: "InferenceService/" + in.Kind, IdempotencyKey: in.RequestID}); err != nil {
			return nil, err
		}
		existing, e := q.GetIdempotency(ctx, GetIdempotencyParams{TenantID: tenant, Method: "InferenceService/" + in.Kind, IdempotencyKey: in.RequestID})
		if e == nil {
			if existing.PayloadHash != in.RequestHash {
				return nil, inferencebiz.ErrIdempotencyConflict
			}
			op, e := q.GetOperation(ctx, GetOperationParams{TenantID: tenant, ID: existing.OperationID})
			if e != nil {
				return nil, e
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return commandResponseFromRow(op), nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return nil, e
		}
	}
	current, err := q.GetServiceForUpdate(ctx, GetServiceForUpdateParams{TenantID: tenant, ID: serviceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, inferencebiz.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if current.DesiredGeneration != in.ExpectedGeneration || current.DeletedAt.Valid {
		return nil, inferencebiz.ErrGenerationConflict
	}
	managedState, err := q.GetManagedGPUState(ctx, GetManagedGPUStateParams{TenantID: tenant, ResourceID: serviceID})
	if err != nil {
		return nil, err
	}
	managed, closing := managedState.Managed, managedState.Closing
	if managed && in.Kind == "delete" {
		return nil, fmt.Errorf("%w: managed GPU DELETE requires a trusted Governance attachment", inferencebiz.ErrInvalidState)
	}
	if closing && (in.Kind == "start" || in.Kind == "restart") {
		return nil, fmt.Errorf("%w: managed GPU resource has a closing intent", inferencebiz.ErrInvalidState)
	}
	if err := validCommandState(in.Kind, current.DesiredState); err != nil {
		return nil, err
	}
	targetGeneration := current.DesiredGeneration + 1
	var requestedResources []byte
	if in.Kind == "start" || in.Kind == "restart" {
		currentSpec, specErr := q.GetSpec(ctx, GetSpecParams{TenantID: tenant, ServiceID: serviceID, Generation: current.DesiredGeneration})
		if specErr != nil {
			return nil, specErr
		}
		requestedResources = currentSpec.Resources
	}
	desiredState := current.DesiredState
	if in.Kind == "start" || in.Kind == "restart" {
		desiredState = "running"
	} else if in.Kind == "stop" {
		desiredState = "stopped"
	} else if in.Kind == "delete" {
		desiredState = "deleted"
	}
	if in.Kind != "delete" {
		cloneID := pgUUID(uuid.New())
		rows, e := q.CloneSpecGeneration(ctx, CloneSpecGenerationParams{ID: cloneID, TargetGeneration: targetGeneration, TenantID: tenant, ServiceID: serviceID, SourceGeneration: current.DesiredGeneration})
		if e != nil {
			return nil, e
		}
		if rows != 1 {
			return nil, fmt.Errorf("%w: missing current specification", inferencebiz.ErrInvalidState)
		}
	}
	step := "withdraw_publication"
	if in.Kind == "start" {
		// Starting a stopped service creates a new runtime generation.  It must
		// pass through the quota adapter before any Kubernetes apply, just like
		// create and update; the worker advances to apply_runtime only after a
		// confirmed reservation is persisted.
		step = "reserve_quota"
	}
	rows, err := q.TransitionService(ctx, TransitionServiceParams{DesiredState: desiredState, TargetGeneration: targetGeneration, OperationID: opID, TenantID: tenant, ServiceID: serviceID, ExpectedGeneration: current.DesiredGeneration})
	if err != nil {
		return nil, err
	}
	if rows != 1 {
		return nil, inferencebiz.ErrGenerationConflict
	}
	if err := q.InsertOperation(ctx, InsertOperationParams{TenantID: tenant, ID: opID, ServiceID: serviceID, Kind: in.Kind, Phase: "pending", Step: step, TargetGeneration: targetGeneration, RequestHash: in.RequestHash}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, inferencebiz.ErrOperationActive
		}
		return nil, err
	}
	if !managed && (in.Kind == "start" || in.Kind == "restart") {
		if err := q.InsertQuotaReservation(ctx, InsertQuotaReservationParams{TenantID: tenant, ServiceID: serviceID, OperationID: opID, Generation: targetGeneration, ReservationID: "pending-" + opID.String(), RequestedResources: requestedResources}); err != nil {
			return nil, err
		}
	}
	if err := q.UpsertResourceWork(ctx, UpsertResourceWorkParams{TenantID: tenant, ServiceID: serviceID, DirtyVersion: targetGeneration}); err != nil {
		return nil, err
	}
	if in.RequestID != "" {
		if _, err := q.PutIdempotency(ctx, PutIdempotencyParams{TenantID: tenant, Method: "InferenceService/" + in.Kind, IdempotencyKey: in.RequestID, PayloadHash: in.RequestHash, OperationID: opID}); err != nil {
			return nil, err
		}
	}
	payload, _ := json.Marshal(map[string]any{"kind": in.Kind, "desired_state": desiredState})
	if err := q.AppendAuditEvent(ctx, AppendAuditEventParams{TenantID: tenant, EventID: pgUUID(uuid.New()), ServiceID: serviceID, OperationID: opID, Generation: targetGeneration, EventType: "inference." + in.Kind + ".accepted", Actor: in.Actor, RequestID: in.RequestID, Payload: payload}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return commandResponse(serviceID.String(), opID.String(), targetGeneration, in.Kind, step), nil
}

func pgUUID(id uuid.UUID) (v pgtype.UUID) { return pgtype.UUID{Bytes: id, Valid: true} }

func validCommandState(kind, state string) error {
	switch kind {
	case "start":
		if state != "stopped" {
			return fmt.Errorf("%w: start requires stopped state", inferencebiz.ErrInvalidState)
		}
	case "stop":
		if state != "running" {
			return fmt.Errorf("%w: stop requires running state", inferencebiz.ErrInvalidState)
		}
	case "restart":
		if state != "running" && state != "stopped" {
			return fmt.Errorf("%w: restart requires running or stopped state", inferencebiz.ErrInvalidState)
		}
	case "delete":
		if state == "deleted" {
			return fmt.Errorf("%w: service already deleted", inferencebiz.ErrInvalidState)
		}
	}
	return nil
}

func commandResponse(serviceID, operationID string, generation int64, kind, step string) *inferencev1.OperationResponse {
	return &inferencev1.OperationResponse{
		Resource:  &inferencev1.InferenceService{Id: serviceID, DesiredState: map[string]string{"start": "running", "restart": "running", "stop": "stopped", "delete": "deleted"}[kind], Generation: generation},
		Operation: &inferencev1.Operation{Id: operationID, ServiceId: serviceID, Kind: kind, Phase: "pending", Step: step, TargetGeneration: generation},
	}
}

// commandResponseFromRow preserves the durable operation state on an
// idempotent replay. A replay must not claim that a previously succeeded,
// failed, or retried operation is still pending.
func commandResponseFromRow(op InferenceOperation) *inferencev1.OperationResponse {
	return &inferencev1.OperationResponse{
		Resource:  &inferencev1.InferenceService{Id: op.ServiceID.String(), Generation: op.TargetGeneration},
		Operation: &inferencev1.Operation{Id: op.ID.String(), ServiceId: op.ServiceID.String(), Kind: op.Kind, Phase: op.Phase, Step: op.Step, TargetGeneration: op.TargetGeneration, ErrorCode: op.ErrorCode, ErrorMessage: op.ErrorMessage},
	}
}
