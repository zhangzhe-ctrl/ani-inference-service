# Inference managed GPU command and refund handoff

Batch: `GOV-ACC-INF-V12-WIRING-01`. Software wiring does not establish model
readiness, physical GPU scheduling, cleanup completion or production readiness.
The three-party API authority remains Accelerator's
`docs/contracts/accelerator-consumer-contract.md` and Governance's
`docs/contracts/gpu-owner-integration-guide.md`.

## Trusted command acceptance

Governance sends the existing `CreateInferenceService` business message with
the public `GpuOwnerCreateAttachment` and full `original_charges`. Delete uses
`DeleteInferenceServiceRequest` with `GpuOwnerDeleteAttachment` and the full
original charge vector. The command resource/create/delete IDs come from
Governance; local generations and restart/update operation IDs never replace
the original refund identity.

The managed gRPC listener requires TLS 1.3 and a verified client certificate.
Its unique URI SAN must exactly equal the registered
`spiffe://ani.internal/service/ani-governance`. A second URI, a different URI,
CN, owner metadata or an unverified same-name certificate cannot authorize a
command. After authentication the receiver binds the attachment's tenant;
incoming tenant metadata, when present, must match it. The service separately
requires the trusted marker and the validated plan/business/charge contract.

An ACK means only that the command, original context and execution or closing
intent committed in one PostgreSQL transaction. DELETE may precede CREATE;
its ACK never means resources are released. Managed records preserve the
original plan and ledger vector for retry/recovery. Historical direct records
cannot manufacture those fields to become eligible for this reporter.

The explicit migration is `migrations/000018_managed_gpu_commands.sql`.
`inference_managed_gpu_resources` uses `(tenant_id, resource_id)` as its key
and stores the immutable original CREATE, plan, full charge vector and GPU
subset in `original_context`. It also retains canonical business payload,
business digest, CREATE request hash/raw message and DELETE/closing association.
`inference_managed_gpu_commands` uses `(tenant_id, operation_id)` with a
tenant-preserving resource/CREATE foreign key, normalized full command,
payload/request hashes, durable ACK and exact response snapshot. Its execution
intent is `create_pending`, `closing_intent` or `blocked_by_delete`.
Normal CREATE writes local service/spec/operation/work records in the same
transaction; CREATE after a closing tombstone only preserves the command and
ACK. Refund lookup matches tenant, resource and original CREATE exactly.

## KServe v0.16 LWS projection blocker (C02)

For the pinned KServe v0.16.0 LLMI API, `spec.template` and `spec.worker`
are raw `PodSpec` fields. `WorkloadSpec` has no `annotations`, queue or
PodGroup field. `router.scheduler` selects HTTP inference backends through
the endpoint picker; it is not the Kubernetes workload scheduler.
See the official [WorkloadSpec](https://github.com/kserve/kserve/blob/v0.16.0/pkg/apis/serving/v1alpha1/llm_inference_service_types.go#L81-L103)
and [SchedulerSpec](https://github.com/kserve/kserve/blob/v0.16.0/pkg/apis/serving/v1alpha1/llm_inference_service_types.go#L187-L205).

KServe copies the raw leader/worker PodSpecs into LWS templates, so
`schedulerName`, node selectors, runtime class and container resource limits
have a supported input path. For LWS and template metadata it propagates
only LLMI metadata annotations under `leaderworkerset.sigs.k8s.io`,
`k8s.v1.cni.cncf.io` and the Kueue API prefix. It does not propagate
`scheduling.volcano.sh/queue-name` or `volcano.sh/` GPU annotations.
`spec.annotations` therefore cannot establish their handoff.
See [PodSpec copying](https://github.com/kserve/kserve/blob/v0.16.0/pkg/controller/v1alpha1/llmisvc/workload_multi_node.go#L156-L190)
and [the exact metadata whitelist](https://github.com/kserve/kserve/blob/v0.16.0/pkg/controller/v1alpha1/llmisvc/workload_multi_node.go#L477-L501).

LWS v0.10.0 reads `scheduling.volcano.sh/queue-name` from LWS top-level
metadata when it creates a new Volcano PodGroup. Pod annotations alone are
insufficient, and an existing PodGroup is not updated by that function.
See [the official Volcano provider](https://github.com/kubernetes-sigs/lws/blob/v0.10.0/pkg/schedulerprovider/volcano_provider.go#L46-L94).

An Inference patch after KServe creates LWS cannot guarantee the first
PodGroup uses the intended queue. Added metadata may survive a no-op
comparison, but KServe performs a full expected-object update when desired
state changes; it does not merge the current object's metadata. See
[the comparator](https://github.com/kserve/kserve/blob/v0.16.0/pkg/controller/v1alpha1/llmisvc/workload_multi_node.go#L517-L537)
and [dry-run/full update](https://github.com/kserve/kserve/blob/v0.16.0/pkg/controller/v1alpha1/llmisvc/lifecycle_crud.go#L131-L145).

A deterministic synchronous LWS CREATE/UPDATE admission projection could
populate the top-level queue and both template GPU annotations before
controllers observe the object, including KServe's dry-run request. This is
a possible integration seam, not an installed or verified capability in this
batch. No such capability has been demonstrated, and a post-creation patch
would leave a scheduling race. Consequently LWS queue/template metadata
delivery and **C02 remain blocked**; a renderer assertion on unsupported
LLMI fields must not be reported as a pass. This conclusion comes from pinned
source inspection and does not claim a cluster or GPU execution result.

## Managed runtime configuration

`Bootstrap.managed_gpu` is the actual typed configuration read by the
composition root. An entirely empty block leaves managed GPU wiring disabled.
Any configured managed identity requires both receiver and refund settings.
CPU/direct compatibility never authorizes an untrusted GPU attachment.

```yaml
managed_gpu:
  receiver:
    address: <registered-inference-managed-host:port>
    ca_file: /managed/inbound-ca.pem
    cert_file: /managed/inference-server.pem
    key_file: /managed/inference-server.key
  refund:
    address: <registered-governance-quota-host:port>
    ca_file: /managed/refund-ca.pem
    cert_file: /managed/inference-refund-client.pem
    key_file: /managed/inference-refund-client.key
    server_name: <registered-governance-quota-DNS-name>
    timeout: 5s
```

These paths are placeholders, not issued production identities. The shipped
configuration expands the existing `ANI` environment source keys:
`ANI_GOVERNANCE_RECEIVER_GRPC_ADDR`, `ANI_GOVERNANCE_RECEIVER_{CA,CERT,KEY}_FILE` and
`ANI_GOVERNANCE_QUOTA_GRPC_ADDR`, `ANI_GOVERNANCE_QUOTA_{CA,CERT,KEY}_FILE`,
`ANI_GOVERNANCE_QUOTA_SERVER_NAME`, `ANI_GOVERNANCE_QUOTA_TIMEOUT`.
Equivalent explicit YAML values use the same typed fields. No plaintext,
`skipVerify`, arbitrary owner override or public refund HTTP endpoint exists.
The dedicated managed listener registers the same Inference service instance;
the existing CPU/model-reference listener keeps its transport configuration.
Managed GPU attachments on that ordinary listener still fail the service's
trusted Governance identity check.

The inbound Governance certificate's URI rule and the reverse refund client's
DNS rule are separate contracts. Governance must register the exact issued
Inference refund DNS SAN as fixed owner `ani-inference`; Inference cannot set
the owner or tenant in a refund request. Production address/SAN/certificate
issuance remains deployment input; isolated test identities do not enable it.

## Reporting a persisted completion notice

The composition root installs the real
`internal/data/governance.Client` as `gpu.RefundReporter` on the actual owner
service. The lifecycle owner can call its internal
`ReportManagedGPUCompletion(ctx, gpu.ReleaseNotification)` method once its
completion fact and stable notification are durable. This method has no gRPC
or HTTP route. The reporter does not create notifications or inspect K8s to
decide when a resource is free.

```go
notice := gpu.ReleaseNotification{
    TenantID: persistedTenant,
    ResourceID: persistedResource,
    OriginalCreateOperationID: persistedOriginalCreate,
    ReleaseEventID: persistedReleaseEvent,
    Reason: gpu.ResourceReleased, // or gpu.AbortedCleaned
    Items: []gpu.ReleaseItem{{
        ChargeID: originalGPUCharge,
        QuotaCode: "gpu.shared_memory_mib",
        ReleasedTotal: 1024,
    }},
    ResourceRefs: []string{persistedResource}, // audit only
}
receipt, err := owner.ReportManagedGPUCompletion(ctx, notice)
```

The adapter loads `gpu.RefundContext` through
`Repository.LoadRefundContext(ctx, tenant, resource, originalCreate)`. This is
the stored managed command snapshot, not caller-supplied replacement data.
It requires the matching original CREATE, fixed `ani-inference` owner,
`gpu-metering-v1`, valid original plan/digest, full original charge vector and
a durable DELETE association. DELETE proves intent only; the caller owns the
separate complete cleanup fact. Every GPU item is mandatory and must use its
full original cumulative total. Existing non-GPU charges can also be included
with cumulative totals within their saved original units.

For a one-Pod shared plan with `F=256`, `q=4`, the refund is **1024 MiB**,
not 4 blocks. For two whole GPUs it is `gpu.physical.count=2`. LWS shared totals
use all original GPU Pods (`groups × (leader + workers)`) already frozen in
the plan; the reporter never recalculates the amount from current Pods.

The larger shared contract example uses two Pods, each with 6144 MiB and
`core_limit_percent=25`: `F=1024`, `q=6`, `memory_percentage=0`,
`logical_device_count=2`, `exclusive_device_count=0` and total/refund
`gpu.shared_memory_mib=12288`. Per-Pod limits are number=1, cores=25 and
memory=6. The corresponding whole example uses two Pods with number=1,
cores=100, memory-percentage=100, no absolute memory limit, zero shared/logical
totals, exclusive count=2 and original/refund `gpu.physical.count=2`.
These amounts illustrate the unit contract; changed containers, replica counts
or topology require the actual matching resolved plan and digest, not reuse of
a historical plan hash.

The wire request uses the published
`quota.service.v1.QuotaReleaseService/ReportQuotaRelease`, original CREATE
`operation_id`, stable `release_event_id`, original charge IDs/codes and
cumulative `released_total`. Audit references provide neither authorization
nor release proof. The response must contain exactly one result per sent
charge, valid nonnegative applied deltas and bounded authoritative cumulative
totals. A malformed or incomplete successful response is an error.

The configured timeout bounds both context lookup and RPC. Persistence errors,
transport/gRPC errors and invalid responses propagate to the caller. After a
timeout, unavailable receiver or lost response, retry the **same persisted
event and content**; do not invent a new ID. Governance owns receipt
idempotency and cumulative accounting. `PermissionDenied`, invalid-context or
conflict errors require correction of identity/state/input, not blind changes
to original IDs or charges. A client return error does not prove that no refund
committed; durable retries must tolerate that uncertainty.

## Remaining lifecycle work and acceptance boundary

The inference owner still owns model loading/inference, real scheduling and
hardware restrictions, trusted Pod signing and complete Pod history, fencing
all create execution points, reconciling in-flight writes, full workload/GPU
cleanup and `ObserveRelease`, the persistent closed fact, reliable notification
outbox, notification triggering/retry scheduling and real environment recovery.
These remain `not_verified` by software command/refund wiring.

Stop/start/restart/update, exit, failure, timeout, a missing CR/Binding, partial
cleanup and DELETE ACK do not call this reporter. Managed GPU commands do not
reserve a second quota or use the legacy reservation release step. Controlled
software tests explicitly inject an already-completed notification after a
persistent DELETE and exercise the actual receiver and ledger; they do not
claim that real deletion occurred. Adapter-only TLS tests use an isolated
remote protocol fixture and are distinct from those cross-service PG tests.
