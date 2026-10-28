"""Verify the fixed Core release from an external project and an empty module cache."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
MODULE='github.com/Leeminjing/go-forest'


def git(*args,**kwargs):
    return subprocess.check_output(['git','-c','safe.directory='+str(ROOT),*args],cwd=ROOT,**kwargs)


def objects(raw):
    decoder=json.JSONDecoder()
    while raw.strip():
        raw=raw.lstrip(); value,length=decoder.raw_decode(raw);raw=raw[length:];yield value


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--version',default='v0.3.1')
    parser.add_argument('--go',default=shutil.which('go'))
    parser.add_argument('--race',action='store_true')
    parser.add_argument('--count',type=int,default=1)
    args=parser.parse_args()
    if not re.fullmatch(r'v\d+\.\d+\.\d+',args.version) or args.count<1 or not args.go:
        parser.error('a fixed semantic version, Go executable and positive count are required')
    tag_target=git('rev-parse',args.version+'^{}',text=True).strip()
    files=git('ls-tree','-r','--name-only',args.version,'forest',text=True).splitlines()
    expected={file:hashlib.sha256(git('show',args.version+':'+file)).hexdigest() for file in files if file.endswith('.go') and not file.endswith('_test.go')}
    evidence=ROOT/'.scratch/core-release'/f'{time.strftime("%Y%m%d-%H%M%S")}-{args.version}-{os.urandom(3).hex()}'
    evidence.mkdir(parents=True)
    commands=[]
    with tempfile.TemporaryDirectory(prefix='forest-core-published-') as temporary:
        directory=Path(temporary).resolve(); project=directory/'consumer';cache=directory/'modules'
        project.mkdir();cache.mkdir()
        if directory.is_relative_to(ROOT.resolve()):raise RuntimeError('consumer must be outside repository')
        for file in (ROOT/'tests/core-release-consumer').iterdir():
            if file.is_file():shutil.copy2(file,project/file.name)
        # Use the selected executable locally: no hidden toolchain download into
        # the empty module cache and no inherited workspace/vendor replacement.
        env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOFLAGS='',GOMODCACHE=str(cache),
                 GOPROXY='https://proxy.golang.org,direct',GOSUMDB='sum.golang.org',GONOSUMDB='',GONOPROXY='',GOPRIVATE='')
        def run(name,*command):
            commands.append({'name':name,'command':command})
            (evidence/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
            with (evidence/(name+'.txt')).open('x',encoding='utf-8') as output:
                result=subprocess.run(command,cwd=project,env=env,stdout=output,stderr=subprocess.STDOUT,text=True,timeout=600)
            if result.returncode:raise RuntimeError(name+' failed; inspect '+str(evidence))
            return (evidence/(name+'.txt')).read_text(encoding='utf-8')
        try:
            version=run('go-version',args.go,'version').strip()
            run('pin',args.go,'mod','edit','-require='+MODULE+'@'+args.version)
            download=json.loads(run('download',args.go,'mod','download','-json',MODULE+'@'+args.version))
            if download.get('Version')!=args.version or download.get('Replace') or download.get('Error'):raise RuntimeError('wrong downloaded release')
            origin=download.get('Origin',{})
            if origin.get('Hash') and origin['Hash']!=tag_target:raise RuntimeError('release origin is not the immutable tag target')
            downloaded=Path(download['Dir']).resolve()
            if not downloaded.is_relative_to(cache) or downloaded.is_relative_to(ROOT.resolve()):raise RuntimeError('release was not downloaded into fresh external cache')
            actual={file:hashlib.sha256((downloaded/file).read_bytes()).hexdigest() for file in expected}
            if actual!=expected:raise RuntimeError('downloaded Core source differs from tagged optimized implementation')
            run('tidy',args.go,'mod','tidy')
            run('verify',args.go,'mod','verify')
            modules=list(objects(run('modules',args.go,'list','-m','-json','all')))
            if any(m.get('Replace') for m in modules):raise RuntimeError('published verification cannot use replacements')
            selected=next(m for m in modules if m['Path']==MODULE)
            if selected['Version']!=args.version or Path(selected['Dir']).resolve()!=downloaded:raise RuntimeError('build selected a different Core')
            for package in objects(run('packages',args.go,'list','-deps','-json','./...')):
                if package.get('Module',{}).get('Path')==MODULE and not Path(package['Dir']).resolve().is_relative_to(downloaded):raise RuntimeError('Core package compiled outside release cache')
            run('vet',args.go,'vet','./...')
            run('test',args.go,'test',*(['-race'] if args.race else []),'./...','-v','-count='+str(args.count),'-timeout=120s')
            shutil.copy2(project/'go.sum',evidence/'consumer.go.sum')
            metadata={'version':args.version,'tag_target':tag_target,'module_sum':download['Sum'],'go_mod_sum':download['GoModSum'],'go':version,'GOWORK':'off','replace':False,'fresh_cache':True,'source_sha256':actual,'origin':origin,'race':args.race,'count':args.count}
            (evidence/'metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8')
            (evidence/'COMPLETE').write_text('external download, source identity, execution, old scope and allocation regression passed\n',encoding='utf-8')
            print(json.dumps(metadata),flush=True)
            print('PASS: fixed Core release; evidence '+str(evidence),flush=True)
        finally:
            # Only this new, explicitly bounded cache is cleaned. Leave the
            # developer's own module and build caches untouched.
            if cache.resolve().is_relative_to(directory):
                subprocess.run([args.go,'clean','-modcache'],cwd=project,env=env,check=True,timeout=120)

if __name__=='__main__':main()
