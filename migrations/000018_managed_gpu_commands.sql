-- Explicit v1.2 owner intake migration; never executed at service startup.
-- A resource row may precede inference_services when DELETE arrives first.
-- This closing intent and ACK do not assert workload cleanup or GPU release.
CREATE TABLE inference_managed_gpu_resources (
 tenant_id UUID NOT NULL,
 resource_id UUID NOT NULL,
 create_operation_id UUID NOT NULL,
 owner_service TEXT NOT NULL CHECK (owner_service = 'ani-inference'),
 metering_version TEXT NOT NULL CHECK (metering_version = 'gpu-metering-v1'),
 original_context JSONB NOT NULL,
 business_payload JSONB,
 business_payload_digest TEXT NOT NULL DEFAULT '',
 create_request_hash TEXT NOT NULL DEFAULT '',
 create_payload BYTEA,
 delete_operation_id UUID,
 closing BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id, resource_id),
 UNIQUE (tenant_id, create_operation_id),
 UNIQUE (tenant_id, resource_id, create_operation_id),
 CHECK (closing = (delete_operation_id IS NOT NULL))
);

CREATE TABLE inference_managed_gpu_commands (
 tenant_id UUID NOT NULL,
 operation_id UUID NOT NULL,
 resource_id UUID NOT NULL,
 create_operation_id UUID NOT NULL,
 kind TEXT NOT NULL CHECK (kind IN ('create', 'delete')),
 payload_hash TEXT NOT NULL,
 request_hash TEXT NOT NULL,
 command_payload BYTEA NOT NULL,
 response_snapshot BYTEA NOT NULL,
 durable_ack JSONB NOT NULL,
 execution_intent TEXT NOT NULL CHECK (execution_intent IN ('create_pending', 'blocked_by_delete', 'closing_intent')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id, operation_id),
 FOREIGN KEY (tenant_id, resource_id, create_operation_id)
  REFERENCES inference_managed_gpu_resources (tenant_id, resource_id, create_operation_id)
);

-- Existing GPU records have no Governance charges. Keep the historical
-- snapshots and reservation cleanup paths; do not manufacture ledger refs.
COMMENT ON COLUMN inference_specs.gpu_request IS
 'Frozen GPU request; a record absent from inference_managed_gpu_resources is legacy direct, not eligible for Governance refund';
