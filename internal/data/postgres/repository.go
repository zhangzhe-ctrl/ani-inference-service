package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	ErrInvalidUUID         = errors.New("invalid UUID")
	ErrIdempotencyConflict = inferencebiz.ErrIdempotencyConflict
)

// CreateAggregateInput contains all facts needed to durably accept a create
// command. Every UUID is tenant scoped; callers must supply the tenant id.
type CreateAggregateInput struct {
	TenantID, ServiceID, SpecID, OperationID string
	Name                                     string
	ModelID, ModelVersionID                  string
	ArtifactProvider, ArtifactRef            string
	ArtifactSHA256, ImageRef                 string
	ServedModelName, EngineRuntime           string
	RuntimeProvider                          string
	CommandArgv, Resources, SpecJSON         []byte
	GPURequest, GPUPlan                      []byte
	GPUPlanDigest                            string
	Replicas, WorkerReplicas                 int32
	RuntimeMode                              string
	RequestHash                              string
	Method, IdempotencyKey                   string
	QuotaReservationID                       string
	Actor, RequestID                         string
	Generation                               int64
	EndpointContainerPort                    int32
	EndpointServicePort                      int32
	EndpointTargetPort, EndpointProtocol     string
	EndpointEnabled                          bool
	ManagedGPU                               *inferencebiz.ManagedGPUCommand
	ResponseSnapshot                         []byte
}

func endpointParams(in *CreateAggregateInput) (pgtype.Int4, pgtype.Int4, pgtype.Text, pgtype.Text) {
	if in == nil || !in.EndpointEnabled {
		return pgtype.Int4{}, pgtype.Int4{}, pgtype.Text{}, pgtype.Text{}
	}
	return pgtype.Int4{Int32: in.EndpointContainerPort, Valid: true}, pgtype.Int4{Int32: in.EndpointServicePort, Valid: true}, pgtype.Text{String: in.EndpointTargetPort, Valid: true}, pgtype.Text{String: in.EndpointProtocol, Valid: true}
}

type CreateAggregateResult struct {
	ServiceID, OperationID string
	Replayed               bool
	ResponseSnapshot       []byte
}

// Repository owns local persistence for the Inference domain. It never writes
// tables outside this schema and executes acceptance as one local transaction.
type Repository struct{ pool *pgxpool.Pool }

func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

func parseUUID(label, value string, allowEmpty bool) (pgtype.UUID, error) {
	if value == "" && allowEmpty {
		return pgtype.UUID{}, nil
	}
	u, err := uuid.Parse(value)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("%w: %s", ErrInvalidUUID, label)
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

func jsonOrEmpty(v []byte, empty []byte) []byte {
	if len(v) == 0 {
		return empty
	}
	return v
}

// CreateService atomically records the service, immutable generation-1 spec,
// operation, durable work item, runtime observation row, idempotency key, and
// audit event. A repeated request with the same tenant/method/key and hash is
// replayed without creating a second operation.
func (r *Repository) CreateService(ctx context.Context, in CreateAggregateInput) (CreateAggregateResult, error) {
	if r == nil || r.pool == nil {
		return CreateAggregateResult{}, errors.New("nil postgres repository")
	}
	tenant, err := parseUUID("tenant_id", in.TenantID, false)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	serviceID, err := parseUUID("service_id", in.ServiceID, true)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	specID, err := parseUUID("spec_id", in.SpecID, true)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	opID, err := parseUUID("operation_id", in.OperationID, true)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	modelID, err := parseUUID("model_id", in.ModelID, true)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	modelVersionID, err := parseUUID("model_version_id", in.ModelVersionID, false)
	if err != nil {
		return CreateAggregateResult{}, err
	}
	if !serviceID.Valid {
		serviceID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	}
	if !specID.Valid {
		specID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	}
	if !opID.Valid {
		opID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	}
	generation := in.Generation
	if generation == 0 {
		generation = 1
	}
	replicas := in.Replicas
	if replicas == 0 {
		replicas = 1
	}
	runtimeMode := in.RuntimeMode
	if runtimeMode == "" {
		runtimeMode = "deployment"
	}
	runtimeProvider := in.RuntimeProvider
	if runtimeProvider == "" {
		runtimeProvider = "deployment"
	}
	workerReplicas := in.WorkerReplicas
	if workerReplicas == 0 {
		workerReplicas = 1
	}
	method := in.Method
	if method == "" {
		method = "CreateInferenceService"
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CreateAggregateResult{}, err
	}
	defer tx.Rollback(ctx)
	q := New(tx)
	if in.ManagedGPU != nil {
		original := in.ManagedGPU.Context
		if original.TenantID != in.TenantID || original.ResourceID != serviceID.String() || original.OriginalCreateOperationID != opID.String() || len(in.ResponseSnapshot) == 0 || len(in.GPUPlan) == 0 {
			return CreateAggregateResult{}, inferencebiz.ErrInvalidState
		}
		accepted, e := prepareManagedGPU(ctx, tx, in.ManagedGPU, "create", opID.String())
		if e != nil {
			return CreateAggregateResult{}, e
		}
		if len(accepted.replay) != 0 {
			return CreateAggregateResult{ServiceID: serviceID.String(), OperationID: opID.String(), Replayed: true, ResponseSnapshot: accepted.replay}, nil
		}
		if accepted.closing {
			response := managedIntentResponse(original, opID.String(), "create", "blocked_by_delete")
			snapshot, e := protojson.Marshal(response)
			if e != nil {
				return CreateAggregateResult{}, e
			}
			if e := persistManagedGPUCommand(ctx, tx, in.ManagedGPU, "create", opID.String(), "blocked_by_delete", snapshot); e != nil {
				return CreateAggregateResult{}, e
			}
			if e := tx.Commit(ctx); e != nil {
				return CreateAggregateResult{}, e
			}
			return CreateAggregateResult{ServiceID: serviceID.String(), OperationID: opID.String(), ResponseSnapshot: snapshot}, nil
		}
	}
	if in.IdempotencyKey != "" {
		if err := q.LockIdempotency(ctx, LockIdempotencyParams{TenantID: in.TenantID, Method: method, IdempotencyKey: in.IdempotencyKey}); err != nil {
			return CreateAggregateResult{}, err
		}
		existing, e := q.GetIdempotency(ctx, GetIdempotencyParams{TenantID: tenant, Method: method, IdempotencyKey: in.IdempotencyKey})
		if e == nil {
			if existing.PayloadHash != in.RequestHash {
				return CreateAggregateResult{}, ErrIdempotencyConflict
			}
			op, opErr := q.GetOperation(ctx, GetOperationParams{TenantID: tenant, ID: existing.OperationID})
			if opErr != nil {
				return CreateAggregateResult{}, opErr
			}
			if err = tx.Commit(ctx); err != nil {
				return CreateAggregateResult{}, err
			}
			return CreateAggregateResult{ServiceID: op.ServiceID.String(), OperationID: existing.OperationID.String(), Replayed: true}, nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return CreateAggregateResult{}, e
		}
	}

	if err = q.InsertService(ctx, InsertServiceParams{TenantID: tenant, ID: serviceID, Name: in.Name, DesiredState: "running", DesiredGeneration: generation}); err != nil {
		return CreateAggregateResult{}, err
	}
	if err = q.InsertOperation(ctx, InsertOperationParams{TenantID: tenant, ID: opID, ServiceID: serviceID, Kind: "create", Phase: "pending", Step: "admission", TargetGeneration: generation, RequestHash: in.RequestHash}); err != nil {
		return CreateAggregateResult{}, err
	}
	if err = q.SetCurrentOperation(ctx, SetCurrentOperationParams{TenantID: tenant, ID: serviceID, CurrentOperationID: opID}); err != nil {
		return CreateAggregateResult{}, err
	}
	endpointContainerPort, endpointServicePort, endpointTargetPort, endpointProtocol := endpointParams(&in)
	if err = q.InsertSpec(ctx, InsertSpecParams{TenantID: tenant, ID: specID, ServiceID: serviceID, Generation: generation, ModelID: modelID, ModelVersionID: modelVersionID, ArtifactProvider: in.ArtifactProvider, ArtifactRef: in.ArtifactRef, ArtifactSha256: in.ArtifactSHA256, ImageRef: in.ImageRef, ServedModelName: in.ServedModelName, EngineRuntime: in.EngineRuntime, CommandArgv: jsonOrEmpty(in.CommandArgv, []byte("[]")), Resources: jsonOrEmpty(in.Resources, []byte("{}")), Replicas: replicas, RuntimeMode: runtimeMode, WorkerReplicas: workerReplicas, RuntimeProvider: runtimeProvider, SpecJson: jsonOrEmpty(in.SpecJSON, []byte("{}")), EndpointContainerPort: endpointContainerPort, EndpointServicePort: endpointServicePort, EndpointTargetPort: endpointTargetPort, EndpointProtocol: endpointProtocol, GpuRequest: in.GPURequest, GpuPlan: in.GPUPlan, GpuPlanDigest: pgtype.Text{String: in.GPUPlanDigest, Valid: in.GPUPlanDigest != ""}}); err != nil {
		return CreateAggregateResult{}, err
	}
	if err = q.InsertRuntime(ctx, InsertRuntimeParams{TenantID: tenant, ServiceID: serviceID, Generation: generation, RuntimeMode: runtimeMode}); err != nil {
		return CreateAggregateResult{}, err
	}
	reservationID := in.QuotaReservationID
	if reservationID == "" {
		reservationID = "pending-" + opID.String()
	}
	if in.ManagedGPU == nil {
		if err = q.InsertQuotaReservation(ctx, InsertQuotaReservationParams{TenantID: tenant, ServiceID: serviceID, OperationID: opID, Generation: generation, ReservationID: reservationID, RequestedResources: jsonOrEmpty(in.Resources, []byte("{}"))}); err != nil {
			return CreateAggregateResult{}, err
		}
	}
	if err = q.UpsertResourceWork(ctx, UpsertResourceWorkParams{TenantID: tenant, ServiceID: serviceID, DirtyVersion: generation}); err != nil {
		return CreateAggregateResult{}, err
	}
	if in.IdempotencyKey != "" {
		rows, e := q.PutIdempotency(ctx, PutIdempotencyParams{TenantID: tenant, Method: method, IdempotencyKey: in.IdempotencyKey, PayloadHash: in.RequestHash, OperationID: opID, ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}})
		if e != nil {
			return CreateAggregateResult{}, e
		}
		if rows == 0 {
			existing, e := q.GetIdempotency(ctx, GetIdempotencyParams{TenantID: tenant, Method: method, IdempotencyKey: in.IdempotencyKey})
			if e != nil {
				return CreateAggregateResult{}, e
			}
			if existing.PayloadHash != in.RequestHash {
				return CreateAggregateResult{}, ErrIdempotencyConflict
			}
			op, e := q.GetOperation(ctx, GetOperationParams{TenantID: tenant, ID: existing.OperationID})
			if e != nil {
				return CreateAggregateResult{}, e
			}
			if err = tx.Commit(ctx); err != nil {
				return CreateAggregateResult{}, err
			}
			return CreateAggregateResult{ServiceID: op.ServiceID.String(), OperationID: existing.OperationID.String(), Replayed: true}, nil
		}
	}
	eventID := uuid.New()
	if err = q.AppendAuditEvent(ctx, AppendAuditEventParams{TenantID: tenant, EventID: pgtype.UUID{Bytes: eventID, Valid: true}, ServiceID: serviceID, OperationID: opID, Generation: generation, EventType: "inference.create.accepted", Actor: in.Actor, RequestID: in.RequestID, Payload: []byte(`{"operation_id":"` + opID.String() + `"}`)}); err != nil {
		return CreateAggregateResult{}, err
	}
	if in.ManagedGPU != nil {
		if err := persistManagedGPUCommand(ctx, tx, in.ManagedGPU, "create", opID.String(), "create_pending", in.ResponseSnapshot); err != nil {
			return CreateAggregateResult{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return CreateAggregateResult{}, err
	}
	return CreateAggregateResult{ServiceID: serviceID.String(), OperationID: opID.String()}, nil
}

// RequestHash computes a stable SHA-256 hash for idempotency storage.
func RequestHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
