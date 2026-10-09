-- name: GetManagedGPUResource :one
SELECT * FROM inference_managed_gpu_resources
WHERE tenant_id = $1 AND resource_id = $2;

-- name: GetManagedGPUCommand :one
SELECT * FROM inference_managed_gpu_commands
WHERE tenant_id = $1 AND operation_id = $2;

-- name: LockManagedGPUKey :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(lock_key)::text, 0));

-- name: GetManagedGPUResourceForUpdate :one
SELECT * FROM inference_managed_gpu_resources
WHERE tenant_id = $1 AND resource_id = $2 FOR UPDATE;

-- name: GetManagedGPUState :one
SELECT EXISTS(SELECT 1 FROM inference_managed_gpu_resources AS managed_resource WHERE managed_resource.tenant_id=sqlc.arg(tenant_id) AND managed_resource.resource_id=sqlc.arg(resource_id)) AS managed,
       EXISTS(SELECT 1 FROM inference_managed_gpu_resources AS closing_resource WHERE closing_resource.tenant_id=sqlc.arg(tenant_id) AND closing_resource.resource_id=sqlc.arg(resource_id) AND closing_resource.closing) AS closing;

-- name: ManagedGPUServiceExists :one
SELECT EXISTS(SELECT 1 FROM inference_services WHERE tenant_id=$1 AND id=$2);

-- name: InsertManagedGPUResource :exec
INSERT INTO inference_managed_gpu_resources(tenant_id,resource_id,create_operation_id,owner_service,metering_version,original_context)
VALUES($1,$2,$3,$4,$5,$6);

-- name: SetManagedGPUClosing :exec
UPDATE inference_managed_gpu_resources
SET closing=TRUE,delete_operation_id=COALESCE(delete_operation_id,sqlc.arg(delete_operation_id)::uuid),updated_at=now()
WHERE tenant_id=sqlc.arg(tenant_id) AND resource_id=sqlc.arg(resource_id);

-- name: SaveManagedGPUCreate :exec
UPDATE inference_managed_gpu_resources
SET business_payload=sqlc.arg(business_payload),business_payload_digest=sqlc.arg(business_payload_digest),
    create_request_hash=sqlc.arg(create_request_hash),create_payload=sqlc.arg(create_payload),updated_at=now()
WHERE tenant_id=sqlc.arg(tenant_id) AND resource_id=sqlc.arg(resource_id) AND create_payload IS NULL;

-- name: InsertManagedGPUCommand :exec
INSERT INTO inference_managed_gpu_commands(tenant_id,operation_id,resource_id,create_operation_id,kind,payload_hash,request_hash,command_payload,response_snapshot,durable_ack,execution_intent)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);

-- name: GetManagedGPURefundContext :one
SELECT original_context,delete_operation_id FROM inference_managed_gpu_resources
WHERE tenant_id=$1 AND resource_id=$2 AND create_operation_id=$3;
