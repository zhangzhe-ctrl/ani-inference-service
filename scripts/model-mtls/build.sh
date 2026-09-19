#!/usr/bin/env bash
set -euo pipefail
R=${1:?Fedora task directory}
export GOWORK=off GOMODCACHE="$R/gomodcache" GOCACHE="$R/gocache" GOMAXPROCS=4 GOFLAGS=-p=4 CGO_ENABLED=0
export GOPROXY="file://$R/inputs/proxy,https://proxy.golang.org" GONOSUMDB=github.com/zhangzhe-ctrl/ani-model-service
mkdir -p "$R/bin"
python3 - "$R" <<'PY'
from pathlib import Path
import zipfile,sys,hashlib
r=Path(sys.argv[1]);version='v0.0.0-20260916025225-474f37a1df63'
p=r/'inputs/proxy/github.com/zhangzhe-ctrl/ani-model-service/@v'
for f in sorted(p.iterdir()):print(hashlib.sha256(f.read_bytes()).hexdigest(),f.name)
with zipfile.ZipFile(p/(version+'.zip')) as z:
 prefix='github.com/zhangzhe-ctrl/ani-model-service@'+version+'/'
 for f in sorted((r/'work/model/api/model/v1').glob('*')):
  if f.is_file():
   assert z.read(prefix+'api/model/v1/'+f.name)==f.read_bytes(),f'API drift: {f}'
print('PASS fixed Model public API byte comparison')
PY
cd "$R/work/model"
gofmt -w internal/server/governance{,_test}.go cmd/ani-model-service/catalog{,_test}.go
go test ./internal/server -run 'Test(GovernanceMTLSBoundary|InferenceReceiverMatrix|CatalogReadinessLiveProbe)$' -count=1 -v
go test ./internal/service -run 'Test(GetModelVersion.*|CatalogListContract)$' -count=1 -v
go test ./cmd/ani-model-service -run 'TestCatalog(StartupRequiresIdentityConfiguration|WiresVersionReader)$' -count=1 -v
go build -o "$R/bin/model" ./cmd/ani-model-service
cd "$R/work/inference"
gofmt -w internal/data/model/*.go cmd/model-contract-probe/*.go
go mod tidy
go test ./internal/data/model -run 'TestClient(MetadataAndDeadline|CancellationAndError)$' -count=1 -v
go build -o "$R/bin/probe" ./cmd/model-contract-probe
go build -o "$R/bin/inference" ./cmd/ani-inference-service
sha256sum "$R/bin/"*
