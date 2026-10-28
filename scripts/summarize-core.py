"""Summarize immutable paired runs; raw observations remain authoritative."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
import statistics

PATTERN = re.compile(r"^(Benchmark\S+)\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op$", re.M)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--before', action='append', required=True)
    parser.add_argument('--after', action='append', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    output = Path(args.out).resolve()
    output.mkdir(parents=True)  # preserve an earlier analysis instead of overwriting it
    rows, sources, samples = [], [], {}
    for side, directories in [('before', args.before), ('after', args.after)]:
        for name in directories:
            directory = Path(name).resolve()
            metadata = json.loads((directory/'metadata.json').read_text(encoding='utf-8'))
            destination = output/side/directory.name
            destination.mkdir(parents=True)
            for file in sorted(directory.glob('*.txt')):
                raw = file.read_text(encoding='utf-8')
                measurements = PATTERN.findall(raw)
                if not measurements:
                    if file.name.startswith(('load-', 'longevity-', 'profile-', 'binary-info', 'go-version')):
                        shutil.copy2(file, destination/file.name)
                    continue
                if '\nPASS\n' not in raw:
                    # A valid earlier case may live in an incomplete matrix,
                    # but never count a partial or failed individual benchmark.
                    continue
                shutil.copy2(file, destination/file.name)
                case = file.stem
                for benchmark, iterations, ns, allocated, allocs in measurements:
                    record = dict(side=side, case=case, benchmark=benchmark, iterations=int(iterations), ns_op=float(ns), bytes_op=int(allocated), allocs_op=int(allocs), core=metadata['core'], harness=metadata['harness'], run=directory.name)
                    rows.append(record)
                    samples.setdefault((side, case, benchmark), []).append(record)
            for file in directory.glob('*.pprof'):
                shutil.copy2(file, destination/file.name)
            for name in ['metadata.json','commands.json','runner.py','modules.txt','binary-info.txt','go-version.txt']:
                shutil.copy2(directory/name, destination/name)
            sources.append(dict(side=side, run=directory.name, core=metadata['core'], harness=metadata['harness'], matrix_complete=(directory/'COMPLETE').exists()))
    if not rows:
        raise RuntimeError('no successful benchmark cases')
    with (output/'samples.csv').open('w',newline='',encoding='utf-8') as target:
        writer=csv.DictWriter(target,fieldnames=list(rows[0])); writer.writeheader();writer.writerows(rows)
    summary=[]
    for (side,case,benchmark), values in sorted(samples.items()):
        record=dict(side=side,case=case,benchmark=benchmark,samples=len(values))
        for field in ['ns_op','bytes_op','allocs_op']:
            data=[v[field] for v in values]
            record[field+'_median']=statistics.median(data)
            record[field+'_min']=min(data);record[field+'_max']=max(data)
        summary.append(record)
    comparison=[]
    for (_,case,benchmark), before in sorted(samples.items()):
        if before[0]['side'] != 'before': continue
        after=samples.get(('after',case,benchmark))
        if after is None: raise RuntimeError('missing after case: '+case+'/'+benchmark)
        if {v['harness'] for v in before} != {v['harness'] for v in after}:
            raise RuntimeError('paired harness mismatch: '+case+'/'+benchmark)
        a=statistics.median(v['ns_op'] for v in before); b=statistics.median(v['ns_op'] for v in after)
        comparison.append(dict(case=case,benchmark=benchmark,before_samples=len(before),after_samples=len(after),before_ns=a,after_ns=b,ratio_after_before=b/a,before_bytes=statistics.median(v['bytes_op'] for v in before),after_bytes=statistics.median(v['bytes_op'] for v in after)))
    (output/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    (output/'comparison.json').write_text(json.dumps(comparison,indent=2),encoding='utf-8')
    (output/'sources.json').write_text(json.dumps(sources,indent=2),encoding='utf-8')
    hashes={p.relative_to(output).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.rglob('*')) if p.is_file()}
    (output/'sha256.json').write_text(json.dumps(hashes,indent=2),encoding='utf-8')
    print(json.dumps({'samples':len(rows),'paired_cases':len(comparison),'output':str(output)}))


if __name__ == '__main__':
    main()
