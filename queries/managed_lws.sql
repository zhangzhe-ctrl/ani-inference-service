-- name: ListManagedLWSOwnerBindings :many
-- Namespace/name/UID is an observed Kubernetes object identity. Tenant and
-- service scope come from this binding, never from admission object labels.
SELECT DISTINCT ON (b.tenant_id,b.service_id)
       b.tenant_id,b.service_id,b.generation,b.object_kind,b.object_namespace,
       b.object_name,b.object_uid,b.resource_version,b.role,
       s.desired_generation,m.original_context,m.closing
FROM inference_runtime_bindings b
JOIN inference_services s ON s.tenant_id=b.tenant_id AND s.id=b.service_id
JOIN inference_specs sp ON sp.tenant_id=s.tenant_id AND sp.service_id=s.id AND sp.generation=s.desired_generation
JOIN inference_managed_gpu_resources m ON m.tenant_id=s.tenant_id AND m.resource_id=s.id
WHERE b.object_namespace=sqlc.arg(object_namespace) AND b.object_name=sqlc.arg(object_name)
  AND b.object_uid=sqlc.arg(object_uid) AND b.object_kind='KServeLLMInferenceService' AND b.role='runtime'
  AND b.generation<=s.desired_generation AND s.deleted_at IS NULL
  AND sp.runtime_provider='kserve' AND sp.runtime_mode='leader_worker_set'
ORDER BY b.tenant_id,b.service_id,b.generation DESC;

-- name: HasManagedLWSOwnerCandidate :one
-- A matching accepted name without the observed UID binding is pending, not
-- permission to let an unprojected first CREATE through. Closing tombstones
-- with saved CREATE business also remain protected.
SELECT EXISTS(
  SELECT 1 FROM inference_managed_gpu_resources m
  LEFT JOIN inference_services s ON s.tenant_id=m.tenant_id AND s.id=m.resource_id
  WHERE s.name=sqlc.arg(object_name) OR m.business_payload->>'name'=sqlc.arg(object_name)
);
