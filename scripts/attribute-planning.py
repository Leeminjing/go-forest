"""Paired attribution tool versioned separately from frozen ordinary timing."""
import argparse,hashlib,json,os,runpy,shutil,subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def main():
 p=argparse.ArgumentParser();p.add_argument('--run',required=True);p.add_argument('--tool-ref',required=True);p.add_argument('--out',required=True);a=p.parse_args()
 primary=Path(a.run).resolve();out=Path(a.out).resolve();out.mkdir(parents=True)
 shared=runpy.run_path(str(ROOT/'scripts/measure-core.py'));tools=out/'tools';toolref=shared['archive'](a.tool_ref,tools)
 source=out/'topology';shutil.copytree(primary/'source/extensions/topology',source);harness=out/'harness';shutil.copytree(primary/'harness',harness)
 env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='4',GOFLAGS='');commands=[]
 def run(name,*command,dimensions=None,cwd=harness):
  print(name,flush=True);commands.append(dict(name=name,command=command,dimensions=dimensions or {}))
  (out/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
  with (out/(name+'.txt')).open('x',encoding='utf-8') as f:subprocess.run(command,cwd=cwd,env=dict(env,**(dimensions or {})),stdout=f,stderr=subprocess.STDOUT,text=True,check=True,timeout=600)
 tool=out/'instrument';run('build-tool','go','build','-pgo=off','-buildvcs=false','-o',str(tool),'./scripts/planning-instrument',cwd=tools);run('instrument',str(tool),'-source='+str(source))
 run('configure','go','mod','edit','-replace=github.com/Leeminjing/go-forest/extensions/topology='+str(source));run('tidy','go','mod','tidy');probe=out/'probe';run('build-probe','go','build','-pgo=off','-buildvcs=false','-tags=planningprobe','-o',str(probe),'./cmd/planningprobe')
 prior=json.loads((primary/'metadata.json').read_text());count=0
 for item in json.loads((primary/'commands.json').read_text()):
  if item['name'].startswith('count-'):
   run(item['name'],str(probe),*item['command'][1:],dimensions=item['dimensions']);count+=1
 (out/'metadata.json').write_text(json.dumps(dict(source=prior['source'],harness=prior['harness'],tool=toolref,inputs=prior['inputs'],environment=prior['environment'],count=count,instrumented_files=shared['manifest'](source,['*.go'])),indent=2),encoding='utf-8')
 (out/'COMPLETE').write_text('all paired semantic attribution cases completed\n',encoding='utf-8')

if __name__=='__main__':main()
