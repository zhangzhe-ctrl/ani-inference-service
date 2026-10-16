#!/usr/bin/env bash
set -euo pipefail
# Deterministic API validation/generation; dependencies are temporary inputs only.
# Accelerator Proto and Go packages must come from the same released module.
# Use this script for lint/generate so the locked Accelerator module is mounted
# as a second input without copying its proto into this repository.
cd "$(dirname "$0")/.."
export GOWORK=off
BUF=${BUF:-buf}
action=${1:-generate}
case "$action" in
  lint|generate) ;;
  *) echo "usage: $0 [lint|generate]" >&2; exit 2 ;;
esac
test "$("$BUF" --version)" = 1.60.0
go version -m "$(command -v "$BUF")" | grep -Eq 'mod[[:space:]]+github.com/bufbuild/buf[[:space:]]+v1.60.0'
acc=github.com/zhangzhe-ctrl/ani-accelerator-service
version=$(go list -m -f '{{.Version}}' "$acc")
test "$version" = v0.0.0-20261016163330-c1242866d987
go mod download "$acc@$version"
acc_dir=$(go list -m -f '{{.Dir}}' "$acc")
work=$(mktemp -d "${TMPDIR:-/tmp}/inference-api.XXXXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/upstream"
cp -R api "$work/api"
cp go.mod go.sum buf.gen.yaml "$work/"
cp -R "$acc_dir/api/protos" "$work/upstream/accelerator"
chmod -R u+w "$work/upstream"
printf '%s\n' \
  'version: v2' \
  'modules:' \
  '  - path: api' \
  '  - path: upstream/accelerator' \
  'lint:' \
  '  use:' \
  '    - MINIMAL' > "$work/buf.yaml"
before=$(find api -type f -print0 | sort -z | xargs -0 -r sha256sum)
if [ "$action" = lint ]; then
  (cd "$work" && "$BUF" lint)
  exit 0
fi
(cd "$work" && "$BUF" lint && "$BUF" generate --template buf.gen.yaml)
test "$before" = "$(find api -type f -print0 | sort -z | xargs -0 -r sha256sum)" || {
  echo 'API input/output changed during generation; no output copied' >&2
  exit 1
}
while IFS= read -r -d '' file; do
  relative=${file#"$work/"}
  cp "$file" "$relative"
done < <(find "$work/api" -type f -name '*pb.go' -print0)
