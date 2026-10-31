"""Freeze paired management benchmarks; instrument a separate owned archive."""
import argparse,hashlib,json,os,runpy,shutil,subprocess,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def main():
 p=argparse.ArgumentParser();p.add_argument('--source-ref',required=True);p.add_argument('--harness-ref',required=True);p.add_argument('--label',required=True);p.add_argument('--inputs');p.add_argument('--count',type=int,default=10);p.add_argument('--matrix',choices=['smoke','study'],default='smoke');p.add_argument('--profiles',action='store_true');a=p.parse_args()
 if a.count<1 or not a.label.replace('-','').isalnum():p.error('invalid label/count')
 shared=runpy.run_path(str(ROOT/'scripts/measure-core.py'));out=ROOT/'.scratch/topology-planning'/f'{time.strftime("%Y%m%d-%H%M%S")}-{a.label}';out.mkdir(parents=True)
 source,tools=out/'source',out/'tools';ref=shared['archive'](a.source_ref,source);harness_ref=shared['archive'](a.harness_ref,tools);harness=out/'harness';shutil.copytree(tools/'tests/performance',harness)
 env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='4',GOFLAGS='');commands=[]
 def run(name,*command,dimensions=None,cwd=harness):
  print(a.label+': '+name,flush=True);commands.append(dict(name=name,command=command,dimensions=dimensions or {},cwd=str(cwd)));(out/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
  with (out/(name+'.txt')).open('x',encoding='utf-8') as f:r=subprocess.run(command,cwd=cwd,env=dict(env,**(dimensions or {})),stdout=f,stderr=subprocess.STDOUT,text=True,timeout=1800)
  if r.returncode:raise RuntimeError(name+' failed: '+str(out))
  return (out/(name+'.txt')).read_text(encoding='utf-8')
 run('configure','go','mod','edit','-replace=github.com/Leeminjing/go-forest/extensions/topology='+str(source/'extensions/topology'));run('tidy','go','mod','tidy')
 modules=run('modules','go','list','-m','-json','all');decoder=json.JSONDecoder();selected={}
 while modules.strip():
  modules=modules.lstrip();m,n=decoder.raw_decode(modules);modules=modules[n:];selected[m['Path']]=m
 core=selected['github.com/Leeminjing/go-forest'];topo=selected['github.com/Leeminjing/go-forest/extensions/topology']
 if core['Version']!='v0.3.1' or core.get('Replace') or Path(topo['Replace']['Dir']).resolve()!=(source/'extensions/topology').resolve():raise RuntimeError('wrong selected sources')
 run('version','go','version');run('vet','go','vet','./...');run('fixture-tests','go','test','./...','-count=1','-timeout=120s')
 binary=out/('planning.exe' if os.name=='nt' else 'planning');run('build','go','build','-pgo=off','-buildvcs=false','-o',str(binary),'./cmd/planning');run('binary-info','go','version','-m',str(binary))
 cases=[('small',32,16,0,1,'replace')]
 if a.matrix=='study':cases += [('medium',1024,256,0,1,'replace'),('large',10000,256,0,1,'replace'),('history',32,16,16,1,'replace'),('rebind',32,16,0,1,'rebind'),('noop',32,16,0,1,'noop'),('many',32,16,0,8,'reorder'),('open-history',32,16,64,1,'replace'),('open-large',10000,256,4,1,'replace')]
 inputs=Path(a.inputs).resolve() if a.inputs else out/'inputs'
 if not a.inputs:inputs.mkdir()
 inventory={};dims={}
 for name,n,e,h,d,change in cases:
  dimensions=dict(FOREST_PERF_N=str(n),FOREST_PERF_E=str(e),FOREST_PERF_D=str(d),FOREST_PERF_PORTS='1',FOREST_PERF_MANY='1' if d>1 else '0',FOREST_PERF_HISTORY=str(h),FOREST_PLANNING_CHANGE=change)
  data,journal=inputs/(name+'.json'),inputs/(name+'.journal')
  if not a.inputs:run('input-'+name,str(binary),'-mode=input','-out='+str(data),'-journal='+str(journal),dimensions=dimensions)
  inventory[name]=dict(dimensions=dimensions,files={f.name:dict(bytes=f.stat().st_size,sha256=hashlib.sha256(f.read_bytes()).hexdigest()) for f in [data,journal]})
  dims[name]=dict(dimensions,FOREST_PLANNING_INPUT=str(data),FOREST_PLANNING_JOURNAL=str(journal))
 if a.inputs:
  if inventory!=json.loads((inputs/'manifest.json').read_text(encoding='utf-8')):raise RuntimeError('frozen input differs')
 else:(inputs/'manifest.json').write_text(json.dumps(inventory,indent=2),encoding='utf-8')
 metadata=dict(source=ref,harness=harness_ref,arguments=vars(a),inputs=inventory,environment={k:env.get(k) for k in ['GOWORK','GOTOOLCHAIN','GOMAXPROCS','GOFLAGS','GOGC','GOMEMLIMIT']},source_files=shared['manifest'](source/'extensions/topology',['*.go','go.mod','go.sum']))
 for path in ['/proc/cpuinfo','/sys/fs/cgroup/cpu.max','/sys/fs/cgroup/memory.max']:
  if Path(path).exists():metadata[path]=Path(path).read_text()
 (out/'metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8');(out/'runner.py').write_bytes(Path(__file__).read_bytes())
 for name,*_ in cases:
  regex='^BenchmarkPlanning$' if not name.startswith('open-') else '^BenchmarkPlanning/Open$'
  run('bench-'+name,'go','test','-pgo=off','-buildvcs=false','-run=^$','-bench='+regex,'-benchmem','-benchtime=1x','-count='+str(a.count),'-cpu=1','-timeout=25m',dimensions=dims[name])
 if a.profiles:
  for kind in ['Preview','Commit','PreviewCommit','Open']:
   name='large' if kind!='Open' else 'open-large'
   if name not in dims:name='small'
   for profile in ['cpu','allocs']:
    path=out/(kind+'-'+profile+'.pprof');run('profile-'+kind+'-'+profile,str(binary),'-mode=profile','-operation='+kind,'-profile='+profile,'-out='+str(path),dimensions=dims[name]);run('top-'+kind+'-'+profile,'go','tool','pprof','-top',str(binary),str(path))
 instrumented=out/'instrumented';shutil.copytree(source/'extensions/topology',instrumented)
 tool=out/('instrument.exe' if os.name=='nt' else 'instrument');run('build-instrument','go','build','-pgo=off','-buildvcs=false','-o',str(tool),'./scripts/planning-instrument',cwd=tools)
 run('instrument',str(tool),'-source='+str(instrumented));metadata['instrumented_files']=shared['manifest'](instrumented,['*.go'])
 run('probe-configure','go','mod','edit','-replace=github.com/Leeminjing/go-forest/extensions/topology='+str(instrumented));run('probe-tidy','go','mod','tidy')
 probe=out/('probe.exe' if os.name=='nt' else 'probe');run('probe-build','go','build','-pgo=off','-buildvcs=false','-tags=planningprobe','-o',str(probe),'./cmd/planningprobe')
 for name,*_ in cases:
  kinds=['Open'] if name.startswith('open-') else ['Capture','Normalize','Encode','Compare','Digest','Preview','Commit','PreviewCommit','Open']
  for kind in kinds:run('count-'+name+'-'+kind,str(probe),'-operation='+kind,dimensions=dims[name])
 (out/'metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8');(out/'COMPLETE').write_text('all uninstrumented benchmarks, profiles and separate real attribution completed\n',encoding='utf-8');print('saved '+str(out),flush=True)

if __name__=='__main__':main()
