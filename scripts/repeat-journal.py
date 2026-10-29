"""Interleaved controls using the already-frozen primary run workspaces."""
import argparse,hashlib,json,os,subprocess
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--before',required=True);p.add_argument('--after',required=True);p.add_argument('--inputs',required=True);p.add_argument('--out',required=True);p.add_argument('--count',type=int,default=5);a=p.parse_args()
 if a.count<1:p.error('positive count required')
 origins={side:Path(getattr(a,side)).resolve() for side in ['before','after']}
 metadata={side:json.loads((d/'metadata.json').read_text()) for side,d in origins.items()}
 for key in ['harness','inputs','environment','/sys/fs/cgroup/cpu.max','/sys/fs/cgroup/memory.max']:
  if metadata['before'][key]!=metadata['after'][key]:raise RuntimeError('primary pair differs: '+key)
 selected=Path(a.inputs).resolve()/'n8-h1024.journal'
 actual_digest=hashlib.sha256(selected.read_bytes()).hexdigest()
 if actual_digest!=metadata['before']['inputs']['n8-h1024']['sha256']:
  raise RuntimeError('interleaved input differs from frozen primary corpus')
 out=Path(a.out).resolve();out.mkdir(parents=True);commands=[]
 env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOFLAGS='',GOMAXPROCS='4')
 for i in range(a.count):
  for side in (['before','after'] if i%2==0 else ['after','before']):
   for family in ['journal','management']:
    command=['go','test','-pgo=off','-run=^$','-bench='+('^BenchmarkJournal/(Append|Open)$' if family=='journal' else '^BenchmarkManagement/(Preview|ApplyRebind|CommitFresh)$'),'-benchmem','-benchtime=1x','-count=1','-cpu=1','-timeout=5m']
    dimensions={'FOREST_JOURNAL_INPUT':str(Path(a.inputs).resolve()/'n8-h1024.journal')} if family=='journal' else {'FOREST_PERF_N':'10000','FOREST_PERF_E':'256'}
    name=f'{i:02}-{side}-{family}';print(name,flush=True)
    commands.append(dict(name=name,command=command,dimensions=dimensions,cwd=str(origins[side]/'harness')))
    with (out/(name+'.txt')).open('x') as f:subprocess.run(command,cwd=origins[side]/'harness',env=dict(env,**dimensions),stdout=f,stderr=subprocess.STDOUT,check=True,timeout=600)
 (out/'metadata.json').write_text(json.dumps(dict(primary=metadata,input_sha256=actual_digest,environment={k:env.get(k) for k in ['GOWORK','GOTOOLCHAIN','GOFLAGS','GOMAXPROCS']},commands=commands),indent=2),encoding='utf-8')
 (out/'runner.py').write_bytes(Path(__file__).read_bytes());(out/'COMPLETE').write_text('interleaved controls complete\n')

if __name__=='__main__':main()
