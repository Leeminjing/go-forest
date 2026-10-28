"""Pair successful frozen-input Journal observations without mixing environments."""
import argparse,csv,hashlib,json,re,shutil,statistics,zipfile,subprocess,sys
from pathlib import Path

PATTERN=re.compile(r'^(Benchmark\S+)\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op$',re.M)

def main():
 p=argparse.ArgumentParser();p.add_argument('--before',required=True);p.add_argument('--after',required=True);p.add_argument('--out',required=True);p.add_argument('--inputs');args=p.parse_args()
 before,after=Path(args.before).resolve(),Path(args.after).resolve();out=Path(args.out).resolve();out.mkdir(parents=True);(out/'.gitattributes').write_bytes(b'* -text -whitespace\n')
 a,b=[json.loads((d/'metadata.json').read_text()) for d in [before,after]]
 model=lambda m:next((line.split(':',1)[1].strip() for line in m.get('/proc/cpuinfo','').splitlines() if line.startswith('model name')),None)
 if model(a)!=model(b):raise RuntimeError('CPU model differs')
 for key in ['harness','inputs','environment','/sys/fs/cgroup/cpu.max','/sys/fs/cgroup/memory.max']:
  if a.get(key)!=b.get(key):raise RuntimeError('incompatible pair: '+key)
 for name in ['go-version.txt']:
  if (before/name).read_bytes()!=(after/name).read_bytes():raise RuntimeError('Go version differs')
 def flags(d):return sorted(line for line in (d/'binary-info.txt').read_text().splitlines() if line.startswith('\tbuild\t') and not line.startswith('\tbuild\tvcs'))
 if flags(before)!=flags(after):raise RuntimeError('compiler settings differ')
 rows=[];groups={}
 for side,d in [('before',before),('after',after)]:
  dest=out/side;dest.mkdir()
  for file in d.iterdir():
   if file.suffix in ['.txt','.json','.pprof','.py']:shutil.copy2(file,dest/file.name)
   if file.suffix!='.txt':continue
   raw=file.read_text(encoding='utf-8');values=PATTERN.findall(raw)
   if not values:continue
   if '\nPASS\n' not in raw:raise RuntimeError('failed benchmark input: '+str(file))
   for benchmark,iterations,ns,allocated,allocs in values:
    row=dict(side=side,case=file.stem,benchmark=benchmark,iterations=int(iterations),ns_op=float(ns),bytes_op=int(allocated),allocs_op=int(allocs));rows.append(row);groups.setdefault((side,file.stem,benchmark),[]).append(row)
 comparisons=[]
 for (side,case,benchmark),x in sorted(groups.items()):
  if side!='before':continue
  y=groups.get(('after',case,benchmark))
  if not y:raise RuntimeError('missing candidate case '+case+'/'+benchmark)
  record=dict(case=case,benchmark=benchmark,before_samples=len(x),after_samples=len(y))
  for field in ['ns_op','bytes_op','allocs_op']:
   for label,values in [('before',x),('after',y)]:
    numbers=[v[field] for v in values];record[label+'_'+field]=dict(median=statistics.median(numbers),min=min(numbers),max=max(numbers))
  comparisons.append(record)
 with (out/'samples.csv').open('w',newline='',encoding='utf-8') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
 (out/'comparison.json').write_text(json.dumps(comparisons,indent=2),encoding='utf-8')
 # Preserve exact synthetic corpus bytes, compressed instead of duplicating
 # multi-megabyte journals in every raw result directory.
 inputs=Path(args.inputs or a['input_directory'])
 if not inputs.exists():raise RuntimeError('run on the originating host or relocate input_directory explicitly')
 for name,meta in a['inputs'].items():
  if hashlib.sha256((inputs/(name+'.journal')).read_bytes()).hexdigest()!=meta['sha256']:raise RuntimeError('relocated input bytes differ')
 subprocess.run([sys.executable,str(Path(__file__).with_name('inspect-journal-inputs.py')),'--inputs',str(inputs),'--out',str(out/'corpus-details.json')],check=True)
 with zipfile.ZipFile(out/'inputs.zip','x',zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
  for file in sorted(inputs.iterdir()):
   if file.is_file():archive.write(file,file.name)
 (out/'sha256.json').write_text(json.dumps({f.relative_to(out).as_posix():hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(out.rglob('*')) if f.is_file()},indent=2),encoding='utf-8')
 print(json.dumps(dict(observations=len(rows),pairs=len(comparisons))))

if __name__=='__main__':main()
