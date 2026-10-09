package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"google.golang.org/protobuf/encoding/protojson"
)

var _ gpu.RefundContextStore = (*Repository)(nil)

type managedIntake struct {
	closing bool
	replay  []byte
}

// prepareManagedGPU serializes CREATE/DELETE on the tenant/resource key and
// validates the original immutable context before any business write. The
// caller uses this same transaction for service/spec/work and the durable ACK.
func prepareManagedGPU(ctx context.Context, tx pgx.Tx, command *inferencebiz.ManagedGPUCommand, kind, operationID string) (managedIntake, error) {
	if command == nil {
		return managedIntake{}, fmt.Errorf("managed GPU command is required")
	}
	original := command.Context
	tenant, err := parseUUID("tenant_id", original.TenantID, false)
	if err != nil {
		return managedIntake{}, err
	}
	resourceID, err := parseUUID("resource_id", original.ResourceID, false)
	if err != nil {
		return managedIntake{}, err
	}
	operation, err := parseUUID("operation_id", operationID, false)
	if err != nil {
		return managedIntake{}, err
	}
	createID, err := parseUUID("create_operation_id", original.OriginalCreateOperationID, false)
	if err != nil {
		return managedIntake{}, err
	}
	q := New(tx)
	if err := q.LockManagedGPUKey(ctx, original.TenantID+":command:"+operationID); err != nil {
		return managedIntake{}, err
	}
	if err := q.LockManagedGPUKey(ctx, original.TenantID+":"+original.ResourceID); err != nil {
		return managedIntake{}, err
	}
	existing, err := q.GetManagedGPUCommand(ctx, GetManagedGPUCommandParams{TenantID: tenant, OperationID: operation})
	if err == nil {
		if existing.PayloadHash != RequestHash(command.Payload) {
			return managedIntake{}, inferencebiz.ErrIdempotencyConflict
		}
		return managedIntake{replay: existing.ResponseSnapshot}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return managedIntake{}, err
	}
	savedResource, err := q.GetManagedGPUResourceForUpdate(ctx, GetManagedGPUResourceForUpdateParams{TenantID: tenant, ResourceID: resourceID})
	if err == nil {
		var saved gpu.RefundContext
		if err := json.Unmarshal(savedResource.OriginalContext, &saved); err != nil {
			return managedIntake{}, err
		}
		saved.DeleteOperationID, original.DeleteOperationID = "", ""
		if savedResource.CreateOperationID != createID || !reflect.DeepEqual(saved, original) {
			return managedIntake{}, inferencebiz.ErrIdempotencyConflict
		}
		if kind == "delete" && savedResource.DeleteOperationID.Valid && savedResource.DeleteOperationID != operation {
			return managedIntake{}, inferencebiz.ErrIdempotencyConflict
		}
		if kind == "create" && savedResource.BusinessPayloadDigest != "" && savedResource.BusinessPayloadDigest != command.BusinessDigest {
			return managedIntake{}, inferencebiz.ErrIdempotencyConflict
		}
		return managedIntake{closing: savedResource.DeleteOperationID.Valid}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return managedIntake{}, err
	}
	// A managed command may not adopt an old direct record or fabricate charges
	// for it. DELETE-first rows deliberately have no inference_services FK.
	directExists, err := q.ManagedGPUServiceExists(ctx, ManagedGPUServiceExistsParams{TenantID: tenant, ID: resourceID})
	if err != nil {
		return managedIntake{}, err
	}
	if directExists {
		return managedIntake{}, fmt.Errorf("%w: existing direct resource is not Governance managed", inferencebiz.ErrInvalidState)
	}
	original.DeleteOperationID = ""
	payload, err := json.Marshal(original)
	if err != nil {
		return managedIntake{}, err
	}
	err = q.InsertManagedGPUResource(ctx, InsertManagedGPUResourceParams{TenantID: tenant, ResourceID: resourceID, CreateOperationID: createID, OwnerService: original.OwnerService, MeteringVersion: original.MeteringVersion, OriginalContext: payload})
	return managedIntake{}, err
}

func persistManagedGPUCommand(ctx context.Context, tx pgx.Tx, command *inferencebiz.ManagedGPUCommand, kind, operationID, intent string, response []byte) error {
	original := command.Context
	tenant, err := parseUUID("tenant_id", original.TenantID, false)
	if err != nil {
		return err
	}
	resourceID, err := parseUUID("resource_id", original.ResourceID, false)
	if err != nil {
		return err
	}
	operation, err := parseUUID("operation_id", operationID, false)
	if err != nil {
		return err
	}
	createID, err := parseUUID("create_operation_id", original.OriginalCreateOperationID, false)
	if err != nil {
		return err
	}
	q := New(tx)
	if kind == "delete" {
		err := q.SetManagedGPUClosing(ctx, SetManagedGPUClosingParams{TenantID: tenant, ResourceID: resourceID, DeleteOperationID: operation})
		if err != nil {
			return err
		}
	} else {
		err := q.SaveManagedGPUCreate(ctx, SaveManagedGPUCreateParams{TenantID: tenant, ResourceID: resourceID, BusinessPayload: command.Business, BusinessPayloadDigest: command.BusinessDigest, CreateRequestHash: command.RequestHash, CreatePayload: command.Payload})
		if err != nil {
			return err
		}
	}
	ack, err := json.Marshal(map[string]any{"operation_id": operationID, "resource_id": original.ResourceID, "accepted": true})
	if err != nil {
		return err
	}
	err = q.InsertManagedGPUCommand(ctx, InsertManagedGPUCommandParams{TenantID: tenant, OperationID: operation, ResourceID: resourceID, CreateOperationID: createID, Kind: kind, PayloadHash: RequestHash(command.Payload), RequestHash: command.RequestHash, CommandPayload: command.Payload, ResponseSnapshot: response, DurableAck: ack, ExecutionIntent: intent})
	return err
}

func decodeManagedResponse(raw []byte) (*inferencev1.OperationResponse, error) {
	out := new(inferencev1.OperationResponse)
	if err := protojson.Unmarshal(raw, out); err != nil {
		return nil, err
	}
	return out, nil
}

func managedIntentResponse(original gpu.RefundContext, operationID, kind, intent string) *inferencev1.OperationResponse {
	return &inferencev1.OperationResponse{
		Resource:        &inferencev1.InferenceService{Id: original.ResourceID, DesiredState: "deleted"},
		Operation:       &inferencev1.Operation{Id: operationID, ServiceId: original.ResourceID, Kind: kind, Phase: "pending", Step: intent},
		DurableOwnerAck: &integrationv1.DurableOwnerAck{OperationId: operationID, ResourceId: original.ResourceID, Accepted: true},
	}
}

func (u *CommandUseCase) acceptManagedGPUDelete(ctx context.Context, in inferencebiz.CommandInput) (*inferencev1.OperationResponse, error) {
	original := in.ManagedGPU.Context
	if in.Kind != "delete" || original.TenantID != in.TenantID || original.ResourceID != in.ServiceID || original.DeleteOperationID == "" {
		return nil, inferencebiz.ErrInvalidState
	}
	tx, err := u.repo.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	accepted, err := prepareManagedGPU(ctx, tx, in.ManagedGPU, "delete", original.DeleteOperationID)
	if err != nil {
		return nil, err
	}
	if len(accepted.replay) != 0 {
		return decodeManagedResponse(accepted.replay)
	}
	response := managedIntentResponse(original, original.DeleteOperationID, "delete", "closing_intent")
	snapshot, err := protojson.Marshal(response)
	if err != nil {
		return nil, err
	}
	if err := persistManagedGPUCommand(ctx, tx, in.ManagedGPU, "delete", original.DeleteOperationID, "closing_intent", snapshot); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return response, nil
}

// LoadRefundContext uses the original CREATE, never the current generation or
// most recent local operation. It requires the separately persisted DELETE.
func (r *Repository) LoadRefundContext(ctx context.Context, tenantID, resourceID, originalCreateID string) (gpu.RefundContext, error) {
	if r == nil || r.pool == nil {
		return gpu.RefundContext{}, errors.New("nil managed GPU repository")
	}
	tenant, err := parseUUID("tenant_id", tenantID, false)
	if err != nil {
		return gpu.RefundContext{}, err
	}
	resource, err := parseUUID("resource_id", resourceID, false)
	if err != nil {
		return gpu.RefundContext{}, err
	}
	createID, err := parseUUID("create_operation_id", originalCreateID, false)
	if err != nil {
		return gpu.RefundContext{}, err
	}
	row, err := New(r.pool).GetManagedGPURefundContext(ctx, GetManagedGPURefundContextParams{TenantID: tenant, ResourceID: resource, CreateOperationID: createID})
	if err != nil {
		return gpu.RefundContext{}, err
	}
	var original gpu.RefundContext
	if err := json.Unmarshal(row.OriginalContext, &original); err != nil {
		return gpu.RefundContext{}, err
	}
	if row.DeleteOperationID.Valid {
		original.DeleteOperationID = row.DeleteOperationID.String()
	}
	return original, nil
}
