#!/usr/bin/env bash
set -euo pipefail
R=${1:?Fedora task directory}
cd "$R"
for app in model probe; do
 mkdir -p "images/$app"
 cp "bin/$app" "images/$app/server"
 cat > "images/$app/Dockerfile" <<'DOCKER'
FROM scratch
COPY server /server
USER 65532:65532
ENTRYPOINT ["/server"]
DOCKER
 digest=$(sha256sum "bin/$app" | cut -c1-12)
 docker build --network=none -t "ani-$app:inf-model-mtls-20260919-$digest" "images/$app"
done
python3 - "$R" <<'PY'
import hashlib,json,subprocess,sys
from pathlib import Path
r=Path(sys.argv[1]);images={}
for app in ['model','probe']:
 digest=hashlib.sha256((r/'bin'/app).read_bytes()).hexdigest()[:12]
 images[app]=f'ani-{app}:inf-model-mtls-20260919-{digest}'
images['postgres']='postgres:17.11-bookworm'
(r/'inputs/images.json').write_text(json.dumps(images))
info=json.loads(subprocess.check_output(['docker','image','inspect',*images.values()]))
(r/'evidence/application-images.json').write_text(json.dumps([{k:d.get(k) for k in ['Id','RepoTags','RepoDigests','Architecture','Os']} for d in info],indent=2))
subprocess.run(['docker','save','-o',str(r/'inputs/application-images.tar'),*images.values()],check=True)
PY
sha256sum inputs/application-images.tar > evidence/application-archive.sha256
for node in 172.16.101.10 172.16.101.11 172.16.101.12; do
 bash work/inference/scripts/model-mtls/node.sh "$node" 'mkdir -p /home/ubuntu/inf-model-mtls-20260919'
 cat inputs/application-images.tar | bash work/inference/scripts/model-mtls/node.sh "$node" 'cat > /home/ubuntu/inf-model-mtls-20260919/application-images.tar'
 bash work/inference/scripts/model-mtls/node.sh "$node" 'sudo -n ctr -n k8s.io images import /home/ubuntu/inf-model-mtls-20260919/application-images.tar'
done
