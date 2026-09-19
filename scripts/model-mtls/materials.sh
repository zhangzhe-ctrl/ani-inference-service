#!/usr/bin/env bash
set -euo pipefail
R=${1:?Fedora task directory}
cd "$R"
python3 - <<'PY' > inputs/cert-manager-images.txt
import yaml
for doc in yaml.safe_load_all(open('inputs/cert-manager.yaml')):
 if doc and doc.get('kind')=='Deployment':
  for c in doc['spec']['template']['spec']['containers']: print(c['image'])
PY
while read -r img; do
 podman pull "$img"
 podman image inspect "$img" --format '{{.Digest}} {{.RepoTags}}' >> evidence/cert-manager-images.txt
done < inputs/cert-manager-images.txt
mapfile -t images < inputs/cert-manager-images.txt
podman save --multi-image-archive --format docker-archive -o inputs/cert-manager-images.tar "${images[@]}"
sha256sum inputs/cert-manager-images.tar > evidence/cert-manager-archive.sha256
for node in 172.16.101.10 172.16.101.11 172.16.101.12; do
 bash work/inference/scripts/model-mtls/node.sh "$node" 'mkdir -p /home/ubuntu/inf-model-mtls-20260919'
 cat inputs/cert-manager-images.tar | bash work/inference/scripts/model-mtls/node.sh "$node" 'cat > /home/ubuntu/inf-model-mtls-20260919/cert-manager-images.tar'
 bash work/inference/scripts/model-mtls/node.sh "$node" 'sudo -n ctr -n k8s.io images import /home/ubuntu/inf-model-mtls-20260919/cert-manager-images.tar'
done
cat inputs/cert-manager.yaml | bash work/inference/scripts/model-mtls/node.sh 172.16.101.10 'sudo -n kubectl --kubeconfig=/etc/kubernetes/admin.conf apply -f -'
