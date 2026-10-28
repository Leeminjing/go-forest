"""Frozen topology journal benchmarks against identical API-produced log bytes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--topology-ref',required=True);p.add_argument('--harness-ref',required=True)
    p.add_argument('--label',required=True);p.add_argument('--inputs')
    p.add_argument('--matrix',choices=['smoke','study'],default='smoke')
    p.add_argument('--count',type=int,default=10);p.add_argument('--profiles',action='store_true');p.add_argument('--management',action='store_true')
    args=p.parse_args()
    if args.count<1 or not args.label.replace('-','').isalnum():p.error('invalid count/label')
    shared=runpy.run_path(str(ROOT/'scripts/measure-core.py'))
    out=ROOT/'.scratch/topology-journal'/f'{time.strftime("%Y%m%d-%H%M%S")}-{args.label}'
    out.mkdir(parents=True)
    source,tests=out/'source',out/'tests';revision=shared['archive'](args.topology_ref,source);harness_ref=shared['archive'](args.harness_ref,tests)
    harness=out/'harness';shutil.copytree(tests/'tests/performance',harness)
    env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='4',GOFLAGS='')
    commands=[]
    def run(name,*cmd,dimensions=None):
        print(args.label+': '+name,flush=True);commands.append(dict(name=name,command=cmd,dimensions=dimensions or {}))
        (out/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
        with (out/(name+'.txt')).open('x',encoding='utf-8') as log:
            result=subprocess.run(cmd,cwd=harness,env=dict(env,**(dimensions or {})),stdout=log,stderr=subprocess.STDOUT,text=True,timeout=1800)
        if result.returncode:raise RuntimeError(name+' failed: '+str(out))
        return (out/(name+'.txt')).read_text(encoding='utf-8')
    run('configure','go','mod','edit','-replace=github.com/Leeminjing/go-forest/extensions/topology='+str(source/'extensions/topology'))
    run('tidy','go','mod','tidy');modules=run('modules','go','list','-m','-json','all')
    decoder=json.JSONDecoder();selected={}
    while modules.strip():
        modules=modules.lstrip();m,length=decoder.raw_decode(modules);modules=modules[length:];selected[m['Path']]=m
    core=selected['github.com/Leeminjing/go-forest'];topology=selected['github.com/Leeminjing/go-forest/extensions/topology']
    if core.get('Replace') or core['Version']!='v0.3.1':raise RuntimeError('both sides require fixed published Core v0.3.1 without replacement')
    if Path(topology['Replace']['Dir']).resolve()!=(source/'extensions/topology').resolve():raise RuntimeError('wrong topology source')
    run('go-version','go','version');run('vet','go','vet','./...');run('fixture-tests','go','test','./...','-count=1','-timeout=120s')
    binary=out/('journal.exe' if os.name=='nt' else 'journal');run('build','go','build','-pgo=off','-o',str(binary),'./cmd/journal');run('binary-info','go','version','-m',str(binary))
    inputs=Path(args.inputs).resolve() if args.inputs else out/'inputs'
    if not args.inputs:inputs.mkdir()
    cases=[(8,0),(8,16)] if args.matrix=='smoke' else [(8,h) for h in [0,16,64,256,1024]]+[(128,h) for h in [0,16,64]]+[(1024,h) for h in [0,16]]+[(10000,h) for h in [0,4]]
    input_metadata={}
    for n,h in cases:
        name=f'n{n}-h{h}';path=inputs/(name+'.journal')
        if not args.inputs:run('create-'+name,str(binary),'-mode=fixture','-n='+str(n),'-history='+str(h),'-out='+str(path))
        if not path.exists():raise RuntimeError('missing frozen input '+name)
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        info=json.loads(run('input-'+name,str(binary),'-mode=info','-input='+str(path)))
        if info['Revisions']!=h+1 or info['Entries']!=h+2:raise RuntimeError('input does not match real record/history dimensions')
        input_metadata[name]=dict(sha256=digest,n=n,additional_history=h,**info)
    manifest=inputs/'manifest.json'
    if args.inputs:
        if input_metadata!=json.loads(manifest.read_text(encoding='utf-8')):raise RuntimeError('frozen input metadata/hash mismatch')
    else:manifest.write_text(json.dumps(input_metadata,indent=2),encoding='utf-8')
    metadata=dict(topology=revision,harness=harness_ref,arguments=vars(args),inputs=input_metadata,input_directory=str(inputs),environment={k:env.get(k) for k in ['GOWORK','GOTOOLCHAIN','GOMAXPROCS','GOFLAGS','GOGC','GOMEMLIMIT']},source_files=shared['manifest'](source/'extensions/topology',['*.go','go.mod','go.sum']))
    for path in ['/sys/fs/cgroup/cpu.max','/sys/fs/cgroup/memory.max','/proc/cpuinfo']:
        if Path(path).exists():metadata[path]=Path(path).read_text()
    (out/'metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8');(out/'runner.py').write_bytes(Path(__file__).read_bytes())
    for n,h in cases:
        name=f'n{n}-h{h}';path=inputs/(name+'.journal')
        run(name,'go','test','-pgo=off','-run=^$','-bench=^BenchmarkJournal$','-benchmem','-benchtime=1x','-count='+str(args.count),'-cpu=1','-timeout=25m',dimensions={'FOREST_JOURNAL_INPUT':str(path)})
        for mode in ['open','append']:
            run('memory-'+name+'-'+mode,str(binary),'-mode=memory','-operation='+mode,'-input='+str(path),'-out='+str(out/(name+'-'+mode+'.pprof')))
    if args.profiles:
        path=inputs/'n128-h64.journal' if args.matrix=='study' else inputs/'n8-h16.journal'
        for mode in ['open','append']:
            for kind in ['cpu','allocs']:
                file=out/(mode+'-'+kind+'.pprof');run('profile-'+mode+'-'+kind,str(binary),'-mode=profile','-operation='+mode,'-kind='+kind,'-input='+str(path),'-out='+str(file))
                run('profile-top-'+mode+'-'+kind,'go','tool','pprof','-top',str(binary),str(file))
                if mode=='append':run('profile-focus-'+kind,'go','tool','pprof','-top','-focus=appendLocked',str(binary),str(file))
    if args.management:
        for n,e in [(32,16),(1024,256),(10000,256)]:
            run('management-'+str(n),'go','test','-pgo=off','-run=^$','-bench=^BenchmarkManagement$','-benchmem','-benchtime=1x','-count='+str(args.count),'-cpu=1','-timeout=25m',dimensions={'FOREST_PERF_N':str(n),'FOREST_PERF_E':str(e)})
    (out/'COMPLETE').write_text('all frozen-input journal and management cases passed\n',encoding='utf-8');print('saved '+str(out),flush=True)

if __name__=='__main__':main()
