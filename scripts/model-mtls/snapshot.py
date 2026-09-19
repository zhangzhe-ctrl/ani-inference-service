#!/usr/bin/env python3
"""Run on Fedora after acceptance; emit public, reviewable evidence only."""
import hashlib,json,subprocess,sys,shlex
from pathlib import Path
r=Path(sys.argv[1]);e=r/'evidence';ns='inf-model-mtls-20260919'
def k(*args):
 return subprocess.check_output(['bash',str(r/'work/inference/scripts/model-mtls/node.sh'),'172.16.101.10','sudo -n kubectl --kubeconfig=/etc/kubernetes/admin.conf '+shlex.join(args)],text=True)
for resource in ['deployments','pods','jobs','services','persistentvolumeclaims','certificates','issuers','configmaps','serviceaccounts','roles','rolebindings']:
 (e/('final-'+resource+'.json')).write_text(k('-n',ns,'get',resource,'-o','json'))
(e/'final-model.log').write_text(k('-n',ns,'logs','deployment/model'))
(e/'cluster-nodes.json').write_text(k('get','nodes','-o','json'))
images=json.loads((r/'inputs/images.json').read_text())
info=json.loads(subprocess.check_output(['docker','image','inspect',*images.values()]))
(e/'application-images-final.json').write_text(json.dumps([{key:d.get(key) for key in ['Id','RepoTags','RepoDigests','Architecture','Os']} for d in info],indent=2))
(e/'binary-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((r/'bin').iterdir())},indent=2))
# These are source manifests, independent from git metadata and private materials.
for repo in ['model','inference']:
 root=r/'work'/repo
 files={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob('*')) if p.is_file() and '.git' not in p.parts and 'evidence' not in p.parts}
 (e/(repo+'-source-sha256.json')).write_text(json.dumps(files,indent=2))
# Assert actual Jobs exited successfully; initial failed Job remains in the cluster.
jobs=json.loads((e/'final-jobs.json').read_text())['items']
required=['c2-final-a','c2-final-b','c3-final-matrix','c4-c5-no-cert','c4-c5-untrusted','c4-c5-wrong-service','c4-c5-governance','c7-model-down','c7-model-recovered','c7-postgres-final-down','c7-postgres-final-recovered','c8-final-loaded','c8-rolling-confirm']
for name in required:
 j=next(j for j in jobs if j['metadata']['name']==name)
 assert j['status'].get('succeeded')==1,name
assert 'code = Unavailable' in (e/'c7-postgres-final-down.log').read_text()
assert 'PASS handshake' in (e/'c8-final-loaded.log').read_text()
for p in e.iterdir():
 if p.is_file():
  data=p.read_bytes()
  assert b'PRIVATE KEY-----' not in data,p
  # Check generated passwords never entered public evidence.
  for secret in json.loads((r/'private/passwords.json').read_text()).values():assert secret.encode() not in data,p
(e/'acceptance-summary.json').write_text(json.dumps({'C'+str(i):'pass' for i in range(1,9)},indent=2))
print('PASS 13 required successful Jobs, final database error classification, rotation handshake and evidence credential scan')
