"""Execute the fixed Journal patch outside the repository, in a fresh cache."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MODULE = 'github.com/Leeminjing/go-forest/extensions/topology'
VERSION = 'v0.2.1'
TAG = 'extensions/topology/' + VERSION


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--go',default=shutil.which('go'))
    p.add_argument('--race',action='store_true');p.add_argument('--count',type=int,default=1)
    p.add_argument('--wait-seconds',type=int,default=0,help='bounded public index readiness wait; tests are not retried')
    args=p.parse_args()
    if not args.go or args.count<1 or not 0<=args.wait_seconds<=1800:p.error('Go, positive count and wait in [0,1800] required')
    helpers=runpy.run_path(str(ROOT/'scripts/verify-core-release.py'))
    git,objects=helpers['git'],helpers['objects']
    target=git('rev-parse',TAG+'^{}',text=True).strip()
    prefix='extensions/topology/'
    expected={f[len(prefix):]:hashlib.sha256(git('show',TAG+':'+f)).hexdigest()
              for f in git('ls-tree','-r','--name-only',TAG,prefix,text=True).splitlines()
              if (f.endswith('.go') and not f.endswith('_test.go') and '/' not in f[len(prefix):]) or f in [prefix+'go.mod',prefix+'go.sum']}
    out=ROOT/'.scratch/topology-patch-release'/f'{time.strftime("%Y%m%d-%H%M%S")}-{os.urandom(3).hex()}'
    out.mkdir(parents=True);commands=[]
    if args.wait_seconds:
        deadline=time.monotonic()+args.wait_seconds
        urls=['https://proxy.golang.org/github.com/!leeminjing/go-forest/extensions/topology/@v/v0.2.1.info',
              'https://sum.golang.org/lookup/github.com/!leeminjing/go-forest/extensions/topology@v0.2.1']
        while True:
            checks=[]
            for url in urls:
                try:
                    with urllib.request.urlopen(url,timeout=30) as response:
                        checks.append(dict(url=url,status=response.status))
                except urllib.error.HTTPError as e:
                    checks.append(dict(url=url,status=e.code,expires=e.headers.get('Expires')))
                except urllib.error.URLError as e:
                    checks.append(dict(url=url,error=str(e.reason)))
            with (out/'availability.jsonl').open('a',encoding='utf-8') as log:
                log.write(json.dumps(dict(timestamp=time.time(),checks=checks))+'\n')
            if all(c.get('status')==200 for c in checks):break
            if time.monotonic()>=deadline:raise RuntimeError('public index unavailable; no consumer tests executed: '+str(out))
            time.sleep(max(0,min(30,deadline-time.monotonic())))
    with tempfile.TemporaryDirectory(prefix='forest-topology-published-') as temporary:
        task=Path(temporary).resolve();project=task/'consumer';cache=task/'cache';cache.mkdir()
        if task.is_relative_to(ROOT.resolve()):raise RuntimeError('consumer must be outside repository')
        shutil.copytree(ROOT/'tests/topology-patch-consumer',project)
        env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOFLAGS='',GOMODCACHE=str(cache),
                 GOPROXY='https://proxy.golang.org,direct',GOSUMDB='sum.golang.org',GONOPROXY='',GONOSUMDB='',GOPRIVATE='')
        def run(name,*command):
            commands.append(dict(name=name,command=command))
            (out/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
            with (out/(name+'.txt')).open('x',encoding='utf-8') as log:
                result=subprocess.run(command,cwd=project,env=env,stdout=log,stderr=subprocess.STDOUT,text=True,timeout=600)
            if result.returncode:raise RuntimeError(name+' failed; evidence: '+str(out))
            return (out/(name+'.txt')).read_text(encoding='utf-8')
        try:
            go_version=run('go-version',args.go,'version').strip()
            download=json.loads(run('download',args.go,'mod','download','-json',MODULE+'@'+VERSION))
            if download.get('Error') or download.get('Version')!=VERSION or download.get('Replace'):raise RuntimeError('wrong fixed download')
            origin=download.get('Origin',{})
            if origin.get('Hash') and origin['Hash']!=target:raise RuntimeError('download origin differs from tag')
            directory=Path(download['Dir']).resolve()
            if not directory.is_relative_to(cache) or directory.is_relative_to(ROOT.resolve()):raise RuntimeError('not a fresh outside download')
            actual={f:hashlib.sha256((directory/f).read_bytes()).hexdigest() for f in expected}
            if actual!=expected:raise RuntimeError('downloaded production bytes differ from optimized patch tag')
            run('tidy',args.go,'mod','tidy');run('verify',args.go,'mod','verify')
            modules=list(objects(run('modules',args.go,'list','-m','-json','all')))
            if any(m.get('Replace') for m in modules):raise RuntimeError('replacement selected')
            for path,version in [(MODULE,VERSION),('github.com/Leeminjing/go-forest','v0.3.1')]:
                m=next(m for m in modules if m['Path']==path)
                if m['Version']!=version or not Path(m['Dir']).resolve().is_relative_to(cache):raise RuntimeError('wrong selected dependency')
            for package in objects(run('packages',args.go,'list','-test','-deps','-json','./...')):
                if package.get('Module',{}).get('Path')==MODULE and not Path(package['Dir']).resolve().is_relative_to(directory):raise RuntimeError('package outside downloaded patch')
            run('vet',args.go,'vet','./...')
            run('test',args.go,'test',*(['-race'] if args.race else []),'./...','-v','-count='+str(args.count),'-timeout=120s')
            shutil.copy2(project/'go.sum',out/'consumer.go.sum')
            metadata=dict(module=MODULE,version=VERSION,tag=TAG,target=target,go=go_version,
                          module_sum=download['Sum'],go_mod_sum=download['GoModSum'],origin=origin,
                          source_sha256=actual,GOWORK='off',replace=False,fresh_cache=True,race=args.race,count=args.count,public_index_wait=args.wait_seconds)
            (out/'metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8')
            (out/'COMPLETE').write_text('fixed download, original legacy receipts, incremental retention and cold recovery passed\n',encoding='utf-8')
            print(json.dumps(metadata),flush=True);print('PASS: '+str(out),flush=True)
        finally:
            if cache.resolve().is_relative_to(task):
                subprocess.run([args.go,'clean','-modcache'],cwd=project,env=env,check=True,timeout=120)


if __name__=='__main__':main()
