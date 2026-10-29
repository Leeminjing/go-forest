"""Supplementary full-public-projection comparison; no timed measurements."""
import argparse, hashlib, json, os, shutil, subprocess, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--before',required=True);p.add_argument('--after',required=True)
    p.add_argument('--inputs',required=True);p.add_argument('--out',required=True)
    args=p.parse_args();out=Path(args.out).resolve();out.mkdir(parents=True)
    env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='go1.23.12',GOFLAGS='')
    reports={}
    with tempfile.TemporaryDirectory(prefix='forest-replay-audit-') as temporary:
        task=Path(temporary);module=task/'module';module.mkdir()
        for name in ['go.mod','go.sum']:shutil.copy2(ROOT/'tests/performance'/name,module/name)
        shutil.copytree(ROOT/'tests/performance/cmd/journal-audit',module/'cmd/audit')
        inputs=sorted(Path(args.inputs).resolve().glob('*.journal'))
        inventories=[json.loads((Path(getattr(args,side))/'metadata.json').read_text())['inputs'] for side in ['before','after']]
        if inventories[0]!=inventories[1] or {f.stem for f in inputs}!=set(inventories[0]):
            raise RuntimeError('frozen input inventory differs')
        for file in inputs:
            if hashlib.sha256(file.read_bytes()).hexdigest()!=inventories[0][file.stem]['sha256']:
                raise RuntimeError('frozen input checksum differs: '+file.name)
        for kind in ['history','pending']:
            dest=task/('released-v0.1-'+kind+'.journal')
            shutil.copy2(ROOT/'extensions/topology/testdata/legacy-v0.1'/kind/'journal',dest)
            inputs.append(dest)
        commands=[]
        def run(*command):
            commands.append(list(map(str,command)))
            return subprocess.run(list(map(str,command)),cwd=module,env=env,check=True,
                                  text=True,encoding='utf-8',capture_output=True,timeout=900).stdout
        for side in ['before','after']:
            origin=Path(getattr(args,side)).resolve()
            run('go','mod','edit','-replace=github.com/Leeminjing/go-forest/extensions/topology='+str(origin/'source/extensions/topology'))
            run('go','mod','tidy')
            (out/(side+'-modules.json')).write_text(run('go','list','-m','-json','all'),encoding='utf-8')
            binary=task/('audit.exe' if os.name=='nt' else 'audit')
            run('go','build','-o',binary,'./cmd/audit')
            (out/(side+'-binary.txt')).write_text(run('go','version','-m',binary),encoding='utf-8')
            reports[side]=json.loads(run(binary,*inputs))
            (out/(side+'.json')).write_text(json.dumps(reports[side],indent=2),encoding='utf-8')
        if reports['before']!=reports['after']:raise RuntimeError('public replay projection differs')
        (out/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
        (out/'COMPLETE').write_text(f'{len(reports["before"])} identical public projections; original offline receipts do not append or alter evidence\n',encoding='utf-8')
        print('identical public projections:',len(reports['before']))

if __name__=='__main__':main()
