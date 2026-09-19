# Inference → Model.GetModelVersion：真实 mTLS 联调交付

日期：2026-09-19。最终状态：**C1–C8 全部 pass**。这是 Inference 正式出站客户端在
Kubernetes 内以服务身份调用真实 Model、读取真实 PostgreSQL 的验证。
Inference 用户入口、创建和推理运行流程仍不属于本批完成范围。

## 源码与交付位置

- Inference 基线：`main` / `c45ab4f55d1cd315c2f177451e7e5b2102d11253`，开始时干净。
- Model 基线：`codex/governance-model-list` / `a7437c954d9ed92367471b0f2cae49031482ca7e`，开始时干净。
- Fedora：`/home/chabking/ani-inference-runs/mtls-20260919-01`。
- 集群：`172.16.101.10/.11/.12`，ani-01/02/03，Kubernetes v1.35.8。
- namespace：`inf-model-mtls-20260919`；Deployment/Service：`model`、`postgres`；PVC：`postgres`。
- 源码：Fedora `work/inference`、`work/model`；镜像与官方清单：`inputs`；完整证据：`evidence`。
- 本仓库保留[脱敏证据快照](evidence/model-mtls-20260919/acceptance-summary.json)及[复现命令](../scripts/model-mtls/README.md)。
- Governance 业务代码及部署均未改动；既有 `gov-model-20260919-01` 等实验保留。
- 两仓库完成本地提交，未推送、合并、打 tag 或切生产。最终提交号见交付回复和 Fedora `evidence/source-commits.txt`。

Inference 新增 `internal/data/model` 单方法适配器和 `cmd/model-contract-probe` 独立入口。
客户端校验固定 Model 服务身份、规范非零租户 UUID，重新构造唯一 tenant/request ID
metadata；不转发外部 Actor，支持取消及 3 秒调用预算。正常 Job 复用正式客户端，
协议负例局限于 probe，不给客户端加入绕过开关。

固定消费 `github.com/zhangzhe-ctrl/ani-model-service v0.0.0-20260916025225-474f37a1df63`。
既有 file-GOPROXY 交付的公开 API 与当前 Model `api/model/v1` 逐文件一致。
仅增加该固定依赖及两项 go.sum；无 Proto 复制、目录 replace、go.work 或新发布任务。

Model 在现有 catalog-read 入口装配 PostgreSQL VersionStore，复用 Principal/租户 SQL。
固定矩阵为 Governance → ListModels、Inference → GetModelVersion；其他业务方法与流式
RPC 拒绝。Inference Actor 固定为 `service:ani-inference-service`。同时修正版本读取将
数据库故障误报 NotFound 的问题：资源不存在仍为 NotFound，依赖故障为 Unavailable，
原始错误留在服务日志，响应不暴露连接详情。只读入口不启动 Storage、MinIO 或 worker。

## 实际验收

| 编号 | 最终状态 | 已取得的证据 |
| --- | --- | --- |
| C1 | pass | cert-manager 实际签发 CA 和两端叶证书；SAN、EKU、有效期、链校验正确；正式客户端握手读取成功。见 `c1-certificate-chain.log`、`initial-certificates.json` |
| C2 | pass | A→A 约 15 ms、B→B 约 12 ms；响应与数据库导出的 protobuf JSON 全字段一致，含版本 ID、制品路径、校验和及默认启动参数。见 `c2-final-*.log`、`postgres-*.json` |
| C3 | pass | 跨租户版本及不存在版本 NotFound/空响应；请求租户不一致或为空 PermissionDenied。见 `c3-final-matrix.log` |
| C4 | pass | 无证书、不受信 CA、同 CA 错误服务身份分别 Unavailable；不受信 CA 负例强制呈交外部 CA 证书。见 `c4-c5-{no-cert,untrusted,wrong-service}.log` |
| C5 | pass | Inference ListModels/DeleteModel 拒绝；独立 cert-manager 测试 Governance 证书可 ListModels、不能 GetModelVersion。见矩阵及 `c4-c5-governance.log` |
| C6 | pass | tenant/request ID 缺失、重复、非法、零 UUID、非规范格式均拒绝；伪造 Governance Actor 仍无 ListModels 权限，读取仍为 Inference；单测核对构造的 Actor/Workload。见矩阵及 `TestInferenceReceiverMatrix` |
| C7 | pass | 本片 Model 暂停约 3.003 s 内 DeadlineExceeded；本片 PG 暂停约 1.013 s 内 Unavailable；恢复后同一 PVC 的原始记录再次完整匹配。见 `c7-model-*.log`、`c7-postgres-final-*.log` |
| C8 | pass | 两端 duration 168h→192h 触发重签，Secret resourceVersion、序列号及指纹变化；Model RollingUpdate 后新 Job 实际 TLS 1.3 握手看到新序列号，并通过正式客户端读取。见 `rotation-result.json`、`c8-rolling-confirm.log`、`c8-final-loaded.log` |

证据核查确认 13 个必需成功 Job 的 `status.succeeded=1`，同时保留其他成功和失败 Job；
不是以 Pod Ready 或 HTTP 200 代替上述结果。`final-snapshot.log` 记录了证据中无私钥及
任务数据库密码的扫描结果。数据库查询账号只获得三表 SELECT，见 `database-reader-grants.txt`。

换证后的实际握手序列号：

- Model：`30a690e7d9aa15c9c43e3c1a59d17c802a0b872d`。
- Inference：`2d5948ae8a364d174c7fd0e4cb84b0c1b15cd27c`。

这是重签、应用加载及重新握手验证。自然到期续期、CA 轮换：**not_verified**。

## 材料摘要

cert-manager 固定版本 v1.21.2；[官方支持表](https://cert-manager.io/docs/releases/)覆盖
Kubernetes 1.35。官方清单从 Fedora 下载，SHA-256：
`e03b668ec8675214af6b0a671699d088f2601fa3878e0dbe1b41d3feafd1879f`。
三个官方镜像拉取 digest 见 `cert-manager-images.txt`；归档转换后的实际导入 manifest digest
另保留在 Fedora `materials-repair.log`，不将两种摘要混为一谈。

| 二进制 | SHA-256 |
| --- | --- |
| Model | `d1aefc27048b813d3e0515946ea88f511d1dbd7cd3ad465e2046f8237e9c7b52` |
| probe | `5a30185f3c1cc24213d1a323c421dbd15611e2d2543a4e192e67686211761e2c` |
| Inference 原入口 | `d713921d9fc2981387698334245b2cf4ae08769d351b86ad3f2aeb482a262578` |

最终应用镜像摘要：

- Model：`sha256:daadd73c899b444c8b21be50e3da4c9b58f6ac323b298d709e8ef8d91953e2b1`。
- probe：`sha256:a88bcccc21e151995e6ac06351e984aca4228f62409b600061c68e04c4ebec6c`。
- PostgreSQL 17.11：`sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0`。

`application-images-final.json` 为最终摘要，早期 probe 摘要保留于原始构建日志，不能作为最终交付。
源码逐文件摘要、源码归档摘要及 Git 提交另保留在 Fedora `evidence/`。

## 必要测试和已修复失败

所有格式化、依赖解析、测试、编译、镜像及 Kubernetes 操作均在 Fedora 执行。
`GOWORK=off`，缓存为任务私有 `gomodcache`、`gocache`。

- Inference `./internal/data/model`：`TestClientMetadataAndDeadline`、`TestClientCancellationAndError`，pass。
- Model `./internal/server`：`TestGovernanceMTLSBoundary`、`TestInferenceReceiverMatrix`、`TestCatalogReadinessLiveProbe`，pass。
- Model `./internal/service`：`TestGetModelVersion*`、`TestCatalogListContract`，pass；新增 `TestGetModelVersionDistinguishesStoreFailure`，pass。
- Model `./cmd/ani-model-service`：`TestCatalogStartupRequiresIdentityConfiguration`、`TestCatalogWiresVersionReader`，pass。
- Model 实际入口、probe 入口、Inference 原入口编译，pass；Inference 完整运行时未启动。
- 流式拒绝 interceptor 有定向测试；公开 Model API 无流式业务接口，没有将其计作额外真实流式链路验证。
- make verify、make test、go test ./...、全量 race/lint、前端门禁：按本次明确指令未运行，**not_verified**。

中间失败全部保留：catalog 装配构造器类型错误已修正；Fedora 首次 `.11/.12` SSH 主机密钥
提示已处理；Podman 缺少 `--multi-image-archive` 造成标签指错内容，修正导入并重新启动后
实际签发成功；短服务名首次调用超时，完整域名验收成功；PG 故障误报 NotFound 已修正
并只重跑受影响测试/验收。首次失败读取 Job `c2-tenant-a` 保留，未删除来改变验收结果。
最终 C1–C8 无遗留 fail。

## 信任与保留边界

本实验使用独立 SelfSigned 引导 CA 与 namespaced CA Issuer，不使用控制面 CA。
独立公共信任 ConfigMap，与各自 Secret 分开挂载。业务 Pod 无签证/读取其他私钥权限，
无 ServiceAccount token。负例和 Governance 兼容性使用本片新签的独立测试 Secret。

Model 信任平台 Inference 断言租户，不独立复核用户 Membership。测试租户来自受控 Job。
跨租户查询负例不能宣称抵抗持有合法 Inference 凭据的恶意服务。模型制品仅验证持久化
引用与校验和，没有下载或加载模型文件。

保留 namespace、PVC、证书、数据库及证据供复核。回退只作用于本片应用；不卸载共享
cert-manager、不删旧实验、不恢复整集群快照。未做用户入口、创建、运行流程、HA、压测、
长时间观察、生产切换或全平台回归。
