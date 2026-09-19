#!/usr/bin/env python3
"""Fedora-only isolated C1-C8 runner; private data is never printed."""
import base64, hashlib, json, os, secrets, shlex, subprocess, sys, time
from pathlib import Path
import yaml
R=Path(sys.argv[1]); phase=sys.argv[2]; NS='inf-model-mtls-20260919'
NODE=R/'work/inference/scripts/model-mtls/node.sh'
A='11111111-1111-4111-8111-111111111111'; B='22222222-2222-4222-8222-222222222222'
VA='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2'; VB='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2'
def k(*args,data=None):
 cmd='sudo -n kubectl --kubeconfig=/etc/kubernetes/admin.conf '+shlex.join(list(args))
 p=subprocess.run(['bash',str(NODE),'172.16.101.10',cmd],input=data,text=True,capture_output=True)
 if p.returncode: raise RuntimeError(f'kubectl {args[:3]} failed: {p.stderr}')
 return p.stdout

def save(name,value): (R/'evidence'/name).write_text(value if isinstance(value,str) else json.dumps(value,indent=2)+'\n')
def obj(kind,name,**fields):return dict(apiVersion='v1',kind=kind,metadata=dict(name=name,namespace=NS),**fields)
def apply(items):
 if not isinstance(items,list):items=[items]
 print(k('apply','-f','-',data=json.dumps(dict(apiVersion='v1',kind='List',items=items))),flush=True)
def wait(kind,name,condition='Ready',timeout=120):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  d=json.loads(k('-n',NS,'get',kind,name,'-o','json'))
  if any(c['type']==condition and c['status']=='True' for c in d.get('status',{}).get('conditions',[])):return d
  if any(c['type']=='Failed' and c['status']=='True' for c in d.get('status',{}).get('conditions',[])):raise RuntimeError(f'{kind}/{name} failed')
  time.sleep(2)
 raise RuntimeError(f'{kind}/{name} did not become {condition}')
def rollout(name): print(k('-n',NS,'rollout','status','deployment/'+name,'--timeout=120s'),flush=True)
def cert(name,san,usage,issuer='lab-ca',isCA=False):
 spec=dict(secretName=name,issuerRef=dict(name=issuer,kind='Issuer'),duration='168h',renewBefore='24h',privateKey=dict(algorithm='ECDSA',size=256,rotationPolicy='Always'))
 if isCA: spec.update(isCA=True,commonName=name,usages=['cert sign','crl sign'])
 else: spec.update(dnsNames=[san],usages=['digital signature',usage])
 return dict(apiVersion='cert-manager.io/v1',kind='Certificate',metadata=dict(name=name,namespace=NS),spec=spec)
def issuer(name,spec):return dict(apiVersion='cert-manager.io/v1',kind='Issuer',metadata=dict(name=name,namespace=NS),spec=spec)
def public_certs(label):
 result={}
 for name in ['model-tls','inference-tls','governance-tls','wrong-tls','untrusted-tls']:
  d=json.loads(k('-n',NS,'get','secret',name,'-o','json'))
  pem=base64.b64decode(d['data']['tls.crt'])
  path=R/'evidence'/f'{label}-{name}.pem';path.write_bytes(pem)
  p=subprocess.run(['openssl','x509','-in',str(path),'-noout','-serial','-fingerprint','-sha256','-dates','-ext','subjectAltName,extendedKeyUsage'],capture_output=True,text=True,check=True)
  result[name]=dict(resourceVersion=d['metadata']['resourceVersion'],attributes=p.stdout)
 save(label+'-certificates.json',result);return result

def dbsql(sql):return k('-n',NS,'exec','-i','deployment/postgres','--','psql','-U','postgres','-d','model','-v','ON_ERROR_STOP=1','-At',data=sql)
def deployment(name,image,ports,env,volumes,mounts,args=None,probe=None):
 c=dict(name=name,image=image,imagePullPolicy='Never',env=env,volumeMounts=mounts,resources=dict(requests=dict(cpu='100m',memory='128Mi'),limits=dict(cpu='2',memory='768Mi')))
 if args:c['args']=args
 if probe:c['readinessProbe']=dict(**probe,periodSeconds=3,timeoutSeconds=2)
 spec=dict(automountServiceAccountToken=False,containers=[c],volumes=volumes)
 # Keep database on the same node while testing recovery of its existing PVC.
 if name=='postgres':spec['nodeSelector']={'kubernetes.io/hostname':'ani-01'}
 d=dict(apiVersion='apps/v1',kind='Deployment',metadata=dict(name=name,namespace=NS),spec=dict(replicas=1,strategy=(dict(type='Recreate') if name=='postgres' else dict(type='RollingUpdate',rollingUpdate=dict(maxUnavailable=0,maxSurge=1))),selector=dict(matchLabels=dict(app=name)),template=dict(metadata=dict(labels=dict(app=name)),spec=spec)))
 svc=obj('Service',name,spec=dict(selector=dict(app=name),ports=[dict(name='p'+str(p),port=p,targetPort=p) for p in ports]))
 return [d,svc]
def envsecret(name,key):return dict(name=name,valueFrom=dict(secretKeyRef=dict(name='database',key=key)))
def job(name,mode='read',tenant=A,version=VA,secret='inference-tls',expected='a.json',serial=''):
 images=json.loads((R/'inputs/images.json').read_text())
 args=['-address','model.'+NS+'.svc.cluster.local.:19090','-mode',mode,'-tenant',tenant,'-version',version,'-timeout','3s']
 if mode=='read':args+=['-expected','/expected/'+expected]
 if serial:args+=['-server-serial',serial]
 volumes=[dict(name='identity',secret=dict(secretName=secret)),dict(name='trust',configMap=dict(name='trust')),dict(name='expected',configMap=dict(name='expected'))]
 c=dict(name='probe',image=images['probe'],imagePullPolicy='Never',args=args,volumeMounts=[dict(name=n,mountPath='/'+n,readOnly=True) for n in ['identity','trust','expected']],securityContext=dict(runAsNonRoot=True,runAsUser=65532,allowPrivilegeEscalation=False,capabilities=dict(drop=['ALL'])),resources=dict(requests=dict(cpu='50m',memory='32Mi'),limits=dict(cpu='1',memory='128Mi')))
 j=dict(apiVersion='batch/v1',kind='Job',metadata=dict(name=name,namespace=NS),spec=dict(backoffLimit=0,activeDeadlineSeconds=60,template=dict(spec=dict(restartPolicy='Never',automountServiceAccountToken=False,containers=[c],volumes=volumes))))
 save(name+'-job.json',j);apply(j)
 try:wait('job',name,'Complete',75)
 finally:
  log=k('-n',NS,'logs','job/'+name);save(name+'.log',log);print(log,flush=True)
 save(name+'-status.json',json.loads(k('-n',NS,'get','job',name,'-o','json')))

if phase=='init':
 for name in ['cert-manager','cert-manager-cainjector','cert-manager-webhook']:
  print(k('-n','cert-manager','rollout','status','deployment/'+name,'--timeout=120s'),flush=True)
 apply(dict(apiVersion='v1',kind='Namespace',metadata=dict(name=NS,labels={'ani-task':'inference-model-mtls-20260919'})))
 apply(issuer('selfsigned',dict(selfSigned={})))
 apply([cert('lab-root','','','selfsigned',True),cert('untrusted-root','','','selfsigned',True)])
 for name in ['lab-root','untrusted-root']:wait('certificate',name)
 apply([issuer('lab-ca',dict(ca=dict(secretName='lab-root'))),issuer('untrusted-ca',dict(ca=dict(secretName='untrusted-root')))])
 certs=[cert('model-tls','ani-model-service','server auth'),cert('inference-tls','ani-inference-service','client auth'),cert('governance-tls','ani-governance','client auth'),cert('wrong-tls','wrong-service','client auth'),cert('untrusted-tls','ani-inference-service','client auth','untrusted-ca')]
 save('certificates-manifest.json',certs);apply(certs)
 for c in certs:wait('certificate',c['metadata']['name'])
 root=json.loads(k('-n',NS,'get','secret','lab-root','-o','json'))
 ca=base64.b64decode(root['data']['tls.crt']).decode();apply(obj('ConfigMap','trust',data={'ca.crt':ca}));save('ca.crt',ca)
 public_certs('initial')
 images=json.loads((R/'inputs/images.json').read_text())
 path=R/'private/passwords.json'
 if path.exists():passwords=json.loads(path.read_text())
 else:passwords=dict(admin=secrets.token_hex(24),reader=secrets.token_hex(24));path.write_text(json.dumps(passwords));path.chmod(0o600)
 apply(obj('Secret','database',stringData={'admin':passwords['admin'],'dsn':f"postgres://model_reader:{passwords['reader']}@postgres:5432/model?sslmode=disable"}))
 apply(obj('PersistentVolumeClaim','postgres',spec=dict(accessModes=['ReadWriteOnce'],storageClassName='rook-ceph-block',resources=dict(requests=dict(storage='2Gi')))))
 apply(deployment('postgres',images['postgres'],[5432],[envsecret('POSTGRES_PASSWORD','admin'),dict(name='POSTGRES_DB',value='model'),dict(name='PGDATA',value='/var/lib/postgresql/task/data')],[dict(name='data',persistentVolumeClaim=dict(claimName='postgres'))],[dict(name='data',mountPath='/var/lib/postgresql/task')],probe=dict(exec=dict(command=['pg_isready','-U','postgres','-d','model']))))
 rollout('postgres')
elif phase=='seed':
 for p in sorted((R/'work/model/migrations').glob('*.sql')): print(p.name,dbsql(p.read_text()),flush=True)
 password=json.loads((R/'private/passwords.json').read_text())['reader']
 sql=f"CREATE ROLE model_reader LOGIN PASSWORD '{password}'; GRANT CONNECT ON DATABASE model TO model_reader; GRANT USAGE ON SCHEMA public TO model_reader; GRANT SELECT ON models, model_versions, model_artifacts TO model_reader;\n"
 for tenant,prefix in [(A,'a'),(B,'b')]:
  mid=f'{prefix*8}-{prefix*4}-4{prefix*3}-8{prefix*3}-{prefix*11}1';vid=mid[:-1]+'2';aid=mid[:-1]+'3';checksum=prefix*64
  sql+=f"INSERT INTO models(tenant_id,id,model_id,name,source,status) VALUES('{tenant}','{mid}','fixture-{prefix}','fixture-{prefix}','upload','ready');\n"
  sql+=f"INSERT INTO model_versions(tenant_id,id,model_id,version,format,status,checksum_sha256,engine_type,startup_command,startup_args) VALUES('{tenant}','{vid}','{mid}','v1','gguf','ready','{checksum}','vllm','vllm serve','[\"--dtype\",\"auto\"]');\n"
  sql+=f"INSERT INTO model_artifacts(tenant_id,id,model_version_id,provider,reference,format,size_bytes,sha256) VALUES('{tenant}','{aid}','{vid}','upload','{tenant}/fixture-{prefix}/v1/model.gguf','gguf',1024,'{checksum}');\n"
 print(dbsql(sql),flush=True)
 expected={}
 for tenant,file in [(A,'a.json'),(B,'b.json')]:
  q=f"""SELECT json_build_object('model',json_build_object('tenantId',m.tenant_id,'id',m.id,'modelId',m.model_id),'version',json_build_object('id',v.id,'modelId',m.model_id,'version',v.version,'format',v.format,'status',v.status,'checksumSha256',a.sha256,'storagePath',a.reference,'engineType',v.engine_type,'startupCommand',v.startup_command,'startupArgs',v.startup_args)) FROM models m JOIN model_versions v ON (m.tenant_id,m.id)=(v.tenant_id,v.model_id) JOIN model_artifacts a ON (a.tenant_id,a.model_version_id)=(v.tenant_id,v.id) WHERE m.tenant_id='{tenant}';"""
  expected[file]=json.dumps(json.loads(dbsql(q)));save('postgres-'+file,expected[file])
 apply(obj('ConfigMap','expected',data=expected))
 save('database-reader-grants.txt',dbsql("SELECT grantee,table_name,privilege_type FROM information_schema.role_table_grants WHERE grantee='model_reader' ORDER BY table_name,privilege_type;"))
elif phase=='deploy':
 images=json.loads((R/'inputs/images.json').read_text())
 config=dict(server=dict(grpc=dict(network='tcp',addr='0.0.0.0:19090',timeout='2s'),admin=dict(network='tcp',addr='0.0.0.0:19091',timeout='1s'),shutdown_timeout='5s'))
 apply(obj('ConfigMap','model-config',data={'config.yaml':yaml.safe_dump(config)}))
 env=[envsecret('ANI_DATABASE_DSN','dsn')]+[dict(name=k,value=v) for k,v in {'ANI_MODEL_MODE':'catalog-read','ANI_MODEL_CLIENT_CA':'/trust/ca.crt','ANI_MODEL_TLS_CERT':'/identity/tls.crt','ANI_MODEL_TLS_KEY':'/identity/tls.key'}.items()]
 vols=[dict(name='identity',secret=dict(secretName='model-tls')),dict(name='trust',configMap=dict(name='trust')),dict(name='config',configMap=dict(name='model-config'))]
 items=deployment('model',images['model'],[19090,19091],env,vols,[dict(name=n,mountPath='/'+n,readOnly=True) for n in ['identity','trust','config']],['-conf','/config/config.yaml'],dict(httpGet=dict(path='/readyz',port=19091)))
 save('model-manifest.json',items);apply(items);rollout('model')
elif phase=='accept':
 job('c2-tenant-a-fqdn');job('c2-tenant-b',tenant=B,version=VB,expected='b.json')
 job('c3-c5-c6-matrix',mode='matrix')
 for mode,secret in [('no-cert','inference-tls'),('untrusted','untrusted-tls'),('wrong-service','wrong-tls'),('governance','governance-tls')]:job('c4-c5-'+mode,mode=mode,secret=secret)
elif phase=='recovery':
 for dep in ['model','postgres']:
  print(k('-n',NS,'scale','deployment/'+dep,'--replicas=0'),flush=True)
  try:
   print(k('-n',NS,'wait','pod','-l','app='+dep,'--for=delete','--timeout=90s'),flush=True)
   job('c7-'+dep+'-down',mode='outage')
  finally:
   print(k('-n',NS,'scale','deployment/'+dep,'--replicas=1'),flush=True);rollout(dep)
  job('c7-'+dep+'-recovered')
elif phase=='rotate':
 before=public_certs('before-rotation')
 for name in ['model-tls','inference-tls']:print(k('-n',NS,'patch','certificate',name,'--type=merge','-p','{"spec":{"duration":"192h"}}'),flush=True)
 end=time.monotonic()+120
 while time.monotonic()<end:
  changed=True
  for name in ['model-tls','inference-tls']:
   d=json.loads(k('-n',NS,'get','secret',name,'-o','json'))
   changed &= d['metadata']['resourceVersion']!=before[name]['resourceVersion']
  if changed:break
  time.sleep(2)
 else:raise RuntimeError('certificate Secrets did not rotate')
 after=public_certs('after-rotation')
 for name in ['model-tls','inference-tls']:
  assert before[name]['attributes']!=after[name]['attributes'],name
 print(k('-n',NS,'rollout','restart','deployment/model'),flush=True);rollout('model')
 serial=after['model-tls']['attributes'].split('serial=')[1].splitlines()[0]
 job('c8-new-certificates',serial=serial)
 save('rotation-result.json',dict(status='pass',before=before,after=after))
elif phase=='rolling-confirm':
 after=public_certs('rolling-confirm')
 print(k('-n',NS,'rollout','restart','deployment/model'),flush=True);rollout('model')
 serial=after['model-tls']['attributes'].split('serial=')[1].splitlines()[0]
 job('c8-rolling-confirm',serial=serial)
elif phase=='error-recheck':
 job('c2-final-a');job('c2-final-b',tenant=B,version=VB,expected='b.json')
 job('c3-final-matrix',mode='matrix')
 print(k('-n',NS,'scale','deployment/postgres','--replicas=0'),flush=True)
 try:
  print(k('-n',NS,'wait','pod','-l','app=postgres','--for=delete','--timeout=90s'),flush=True)
  job('c7-postgres-final-down',mode='outage')
 finally:
  print(k('-n',NS,'scale','deployment/postgres','--replicas=1'),flush=True);rollout('postgres')
 job('c7-postgres-final-recovered')
 after=public_certs('final')
 serial=after['model-tls']['attributes'].split('serial=')[1].splitlines()[0]
 job('c8-final-loaded',serial=serial)
else:raise SystemExit('unknown phase')
print('PASS phase '+phase,flush=True)
