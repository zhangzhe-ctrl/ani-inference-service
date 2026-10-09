# Pinned KServe / Inference admission software contract

This independent **test-only** module pins official KServe `v0.16.0` and calls
its public `LLMISVCReconciler.Reconcile`. It adds no production dependency and
uses no `replace`, copied private KServe builder or cross-module internal import.
The Inference process is built from
`../../managed_lws_kserve_joint_test.go` in the parent service module.

The official KServe module source is tag commit
`5ea59135a957f5398a11bb54adf2e7a79f03fe1b`, module sum
`h1:kDkcAU5VMiZf4zhdDXH+UdAIgzXhs10GMv0d9r4qvTs=` and go.mod sum
`h1:thwpLFuDWJ+e1uCEQgPb8xjFby4/y/eoANBrHbvyz3k=`. Its normal MVS graph
uses LWS `v0.6.2` types; the actual Inference process uses production LWS
`v0.10.0` types. Their public v1 wire objects meet through HTTPS. The LWS
v0.10 PodGroup queue contract is separately grounded in pinned source; this
test does not execute an LWS scheduler or GPU workload.

The external Kubernetes `client.Client` boundary is a controlled fixture. Its
LWS CREATE and UPDATE (including dry-run) send AdmissionReview over verified
TLS 1.3 to the real Inference admission handler, apply the returned JSON Patch,
and store only non-dry-run results. The actual service accepts external Acc
resolver-result fixtures into PostgreSQL; production PG runtime/owner-binding
lookup, parameter conversion, renderer and admission remain real. The model
artifact/PVC and live LLMI APIReader are external fixtures; execution is held.

Shared and whole cases use two groups, each with a leader and one worker.
Tests compare the first admitted CREATE, dry-run and full UPDATE with the
actual accepted PG plan, including queue, both templates' GPU metadata,
scheduler, selectors, runtime class, limits and caller argv. Forged owner UID,
old-object UID, controller username, same-CA wrong DNS identity and missing
client certificate must be rejected. This proves software projection, not
cluster scheduling, model readiness, physical GPU limits or resource release.

The empty pipeline preset is explicit external cluster configuration; it
supplies no engine flags. Admission production installation/identity and
failure-policy rollout must be separately verified before production use.

Run only on the authorized Fedora host under the existing exclusive heavy
lock. The root execution task prepares `WIRING_RUN/env.sh` and the isolated
`WIRING_RUN/private/pg.env`; never print the DSN or key contents. After copying
this module and the current Inference source to Fedora:

```sh
flock -n /home/chabking/workspace/ani-network-service-runs/net05a-heavy.lock \
  bash /path/to/testdata/kserve_lws_contract/run.sh
```

`WIRING_RUN` defaults to the authorized task run directory and
`INFERENCE_SOURCE_DIR` defaults to its `source/ani-inference-service`.
Private PKI/results stay under `private/c02-admission`, source under this
testdata module, and logs under `evidence/c02-*`. The script builds the actual
Inference helper, starts it, runs this pinned public-Reconcile test, then stops
its own process. Its trap does not touch shared services or databases.
Each attempt has a distinct `C02_RUN_LABEL` (default `c02-joint-r1`); choose
a new label on retry so failed/interrupt logs remain available.

To run against an already-started helper instead, set the exported admission
environment used in `run.sh`, including its ready file, then use
`GOWORK=off go test -count=1 -v .` from this module. Missing ready-file opt-in causes skip
and cannot be reported as PASS.
