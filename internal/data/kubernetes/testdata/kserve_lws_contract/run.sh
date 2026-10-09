#!/usr/bin/env bash
set -euo pipefail
umask 077
export WIRING_RUN="${WIRING_RUN:-/home/chabking/gov-acc-inf-v12-wiring-01-20261009}"
source "$WIRING_RUN/env.sh"
source "$WIRING_RUN/private/pg.env"
inference_source="${INFERENCE_SOURCE_DIR:-$WIRING_RUN/source/ani-inference-service}"
module="$inference_source/internal/data/kubernetes/testdata/kserve_lws_contract"
private="$WIRING_RUN/private/c02-admission"
run_label="${C02_RUN_LABEL:-c02-joint-r1}"
test ! -e "$WIRING_RUN/evidence/$run_label-inference-helper-build.log"
mkdir -p "$private/results"
if test ! -s "$private/ca.pem"; then bash "$module/prepare-pki.sh"; fi
export C02_PKI_DIR="$private" C02_RESULT_DIR="$private/results"
export INFERENCE_LWS_ADMISSION_PROCESS=1
export INFERENCE_LWS_ADMISSION_NAMESPACE=c02-managed-wiring
export INFERENCE_LWS_ADMISSION_CLIENT_DNS=c02-apiserver.test
export INFERENCE_LWS_ADMISSION_SERVER_DNS=c02-inference.test
export INFERENCE_LWS_ADMISSION_CONTROLLER_USER=system:serviceaccount:kserve:kserve-controller-manager
export INFERENCE_LWS_ADMISSION_CA_FILE="$private/ca.pem"
export INFERENCE_LWS_ADMISSION_CERT_FILE="$private/server.pem"
export INFERENCE_LWS_ADMISSION_KEY_FILE="$private/server.key"
export INFERENCE_LWS_ADMISSION_READY_FILE="$private/ready.json"
rm -f "$INFERENCE_LWS_ADMISSION_READY_FILE"
cd "$inference_source"
gofmt -w internal/data/kubernetes/managed_lws_kserve_joint_test.go
go test -mod=readonly -c -o "$WIRING_RUN/bin/c02-inference-kubernetes.test" ./internal/data/kubernetes > "$WIRING_RUN/evidence/$run_label-inference-helper-build.log" 2>&1
owner_pid=
cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if test -n "$owner_pid"; then
    kill -TERM "$owner_pid" 2>/dev/null || true
    wait "$owner_pid" || rc=1
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$WIRING_RUN/bin/c02-inference-kubernetes.test" -test.run='^TestManagedLWSAdmissionProcess$' -test.timeout=0 -test.v > "$WIRING_RUN/evidence/$run_label-inference-admission-process.log" 2>&1 &
owner_pid=$!
printf '%s\n' "$owner_pid" > "$private/process.pid"
for attempt in $(seq 1 150); do
  if test -s "$INFERENCE_LWS_ADMISSION_READY_FILE"; then break; fi
  kill -0 "$owner_pid" || { echo 'Actual Inference admission stopped before ready' >&2; exit 1; }
  sleep 0.2
done
test -s "$INFERENCE_LWS_ADMISSION_READY_FILE"
cd "$module"
gofmt -w reconcile_test.go
go test -mod=readonly -count=1 -timeout=120s -v . > "$WIRING_RUN/evidence/$run_label-actual-kserve-admission.log" 2>&1
