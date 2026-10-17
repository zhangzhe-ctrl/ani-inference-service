# Inference API module

`github.com/zhangzhe-ctrl/ani-inference-service/api` publishes protobuf contracts,
Kubernetes API types, and request validation helpers independently of the runtime
module. Consumers import `github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1`.

Create/Delete no longer accept the retired GPU owner attachment. Its field
numbers and name are reserved. Durable ACK uses ACC's public command protocol.
`EngineSpec.command` retains the complete command string from the current API.

This protocol cutover does not implement or enable the new Inference GPU
lifecycle. The repository's runtime module still requires a separate adaptation;
this API branch is not a runtime deployment candidate. Governance must keep
Inference GPU dispatch disabled until that implementation is complete.

From the repository root, use the pinned `scripts/generate-api.sh lint` and
`scripts/generate-api.sh generate` workflows. Run module checks with `GOWORK=off`
from `api`: `go mod tidy -diff`, `go test ./...`, `go vet ./...`, and
`go mod verify`.
