# Inference → Model mTLS 单接口实验

范围：正式 `internal/data/model.Client.GetModelVersion` → Model 只读入口 → PostgreSQL。
`cmd/model-contract-probe` 是独立装配与验收入口，正常读取必须通过该 Client。
用户入口、创建/运行流程、IAM、下载、Storage、worker 不属于本片。

所有下列命令只在 Fedora 执行。本地只编辑与传输。实际任务目录：
`/home/chabking/ani-inference-runs/mtls-20260919-01`。
源码布局为 `work/inference`、`work/model`，安装材料为 `inputs`，证据为 `evidence`，
密码仅位于权限 0700 的 `private` 目录。私钥留在 Kubernetes cert-manager Secret。

## 从固定输入复现

1. 核对两仓库基线、当前 diff 及集群，使用新的任务目录和独立 namespace。
   `lab.py` 的 `NS` 固定为本片；本片已部署，不要重复执行 seed 或覆盖他人 namespace。
2. 将两个仓库源码传到 `work/`。复制已核验的 Model 固定 file-GOPROXY 至 `inputs/proxy`。
   `build.sh` 会再次逐文件对照公开 API，不使用目录 replace/go.work。
3. 先核查集群 cert-manager。已安装且适用则复用，不重新安装或覆盖共享组件。
   本次缺失，Fedora 获取官方材料的命令为：

```bash
R=/home/chabking/ani-inference-runs/mtls-20260919-01
curl -fL --connect-timeout 15 --max-time 50 \
  https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml \
  -o "$R/inputs/cert-manager.yaml"
sha256sum "$R/inputs/cert-manager.yaml"
bash "$R/work/inference/scripts/model-mtls/materials.sh" "$R"
bash "$R/work/inference/scripts/model-mtls/build.sh" "$R"
bash "$R/work/inference/scripts/model-mtls/images.sh" "$R"
for phase in init seed deploy accept recovery rotate; do
  python3 "$R/work/inference/scripts/model-mtls/lab.py" "$R" "$phase" \
    > "$R/evidence/$phase.log" 2>&1 || break
done
python3 "$R/work/inference/scripts/model-mtls/snapshot.py" "$R"
```

`build.sh` 固定 `GOWORK=off`、任务私有 `GOMODCACHE/GOCACHE`，仅执行列出的定向包/测试。
`images.sh` 使用 Fedora 已有的 `postgres:17.11-bookworm`，构建两份 scratch 应用镜像，
记录摘要并导入 `.10/.11/.12` 的 containerd `k8s.io`。`node.sh` 复用既有受控 SSH helper，
实际 kubectl 全部显式指定 `/etc/kubernetes/admin.conf`。不使用 `.20–.22` 集群。

`init` 使用 cert-manager SelfSigned 引导实验 CA，再由 namespaced CA Issuer 签叶证书。
Inference SAN 为 `ani-inference-service`，EKU clientAuth；Model SAN 为 `ani-model-service`，
EKU serverAuth。叶证书 `rotationPolicy: Always`。业务 Pod 只挂自己的 Secret 和公共 CA
ConfigMap，不挂 ServiceAccount token、不授予签证/读取其他私钥权限。

`seed` 对独立 PostgreSQL 执行 Model 的六份迁移，写入 A/B model/version/artifact，
授予 `model_reader` 三张表的 SELECT。期望响应由数据库 SQL 导出到 ConfigMap，
probe 通过 protobuf 全字段比较核对版本 ID、引用、校验和及启动参数。

`accept` 运行 C2–C6。`recovery` 只暂停本片 Model/PostgreSQL，在 finally 中恢复副本，
调用限时 3 秒，恢复后使用同一数据库记录。`rotate` 修改两份叶证书 duration 从 168h
到 192h，核对 Secret 版本、证书属性，滚动重启 Model 并用新 Job 读取；同时独立核对
实际 TLS 握手的服务端序列号。应用启动时加载证书，不提供热重载。

## 重放已部署的读取

Job 名不可复用已完成对象。保留旧 Job，通过原清单改名创建新 Job；不要删除证据。
例如在 Fedora 使用保存的 `evidence/c2-final-a-job.json`：

```bash
python3 - "$R/evidence/c2-final-a-job.json" <<'PY' | \
  bash "$R/work/inference/scripts/model-mtls/node.sh" 172.16.101.10 \
  'sudo -n kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f -'
import json,sys,time
x=json.load(open(sys.argv[1])); x['metadata']['name']='read-replay-'+str(int(time.time()))
print(json.dumps(x))
PY
```

正常 Job 使用服务完整域名 `model.inf-model-mtls-20260919.svc.cluster.local.:19090`，
证书仍校验固定身份 `ani-model-service`。短 DNS 名在本片初次调用遇到解析/连接等待超时，
完整域名验收通过；不以增大超时掩盖它。

## 验收修复附加步骤

本次保留 `materials-repair.log`（Podman 多镜像归档选项修复）、
`build-repair.log`（catalog 构造器类型修正）、`model-outage-repair.log`（PG 故障不能误报 404）。
`error-recheck` 只复核受后者影响的 A/B、资源负例、PG 暂停/恢复及新证书读取。
`rolling-confirm` 保留明确采用 RollingUpdate 的复核 Job。
`snapshot.py` 是本次最终证据核查，要求这些附加复核 Job 也成功；基础步骤之外的命令：

```bash
python3 "$R/work/inference/scripts/model-mtls/lab.py" "$R" rolling-confirm
python3 "$R/work/inference/scripts/model-mtls/lab.py" "$R" error-recheck
python3 "$R/work/inference/scripts/model-mtls/snapshot.py" "$R"
```

不运行 make verify/make test/go test ./...、全量 race/lint、前端门禁。
不删除 namespace/PVC，不卸载共享 cert-manager，不动旧 Governance 实验。
回退只缩容本片应用：`kubectl ... -n inf-model-mtls-20260919 scale deploy/model --replicas=0`。

信任边界：Model 信任平台 Inference 断言租户，测试租户来自受控 Job；跨租户负例
只证明按租户查询，不能证明可抵御持有合法 Inference 凭据的恶意服务。
