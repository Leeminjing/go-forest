"""Publish raw paired planning evidence and summarize, without timing assertions."""
import argparse
from collections import Counter
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import statistics
import struct

from planning_evidence import verify_attribution, verify_pair, verify_primary


def main():
    p = argparse.ArgumentParser()
    for key in ['before', 'after', 'attribute-before', 'attribute-after', 'audit', 'interleaved', 'out']:
        p.add_argument('--' + key, required=True)
    a = p.parse_args()
    method = verify_pair(a.before, a.after)
    attribution_metadata = {side: verify_attribution(getattr(a, 'attribute_' + side), verify_primary(getattr(a, side))) for side in ['before', 'after']}
    if attribution_metadata['before']['tool'] != attribution_metadata['after']['tool']:
        raise RuntimeError('different attribution tools')
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'paired-method.json').write_text(json.dumps(dict(fingerprint=method, sha256=hashlib.sha256(json.dumps(method, sort_keys=True).encode()).hexdigest()), indent=2))
    rows, counts, metadata = [], {}, {}

    def copy(path, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.stat().st_size > 300_000 and path.suffix in ['.json', '.txt', '.journal']:
            with path.open('rb') as source, gzip.open(str(destination) + '.gz', 'wb') as target:
                shutil.copyfileobj(source, target)
        else:
            shutil.copyfile(path, destination)

    def evidence_file(path):
        return path.is_file() and (path.suffix in ['.json', '.txt', '.pprof', '.py', '.gz'] or path.name == 'COMPLETE')

    pattern = re.compile(r'^BenchmarkPlanning/(\w+)\s+1\s+(\d+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op$')
    for side in ['before', 'after']:
        directory = Path(getattr(a, side))
        if not (directory / 'COMPLETE').exists():
            raise RuntimeError('incomplete timing run')
        metadata[side] = json.loads((directory / 'metadata.json').read_text())
        total = 0
        for path in directory.glob('bench-*.txt'):
            case = path.stem[6:]
            observations = Counter()
            for line in path.read_text().splitlines():
                match = pattern.match(line.strip())
                if match:
                    operation, ns, allocation, allocations = match.groups()
                    observations[operation] += 1
                    rows.append(dict(side=side, case=case, operation=operation, sample=observations[operation], ns=int(ns), bytes=int(allocation), allocations=int(allocations)))
                    total += 1
            if not observations or any(n != 10 for n in observations.values()):
                raise RuntimeError('wrong sample count: ' + str(path))
        if total != 650:
            raise RuntimeError('expected 650 ordinary observations per side')
        for path in directory.iterdir():
            if evidence_file(path):
                copy(path, out / side / path.name)
        directory = Path(getattr(a, 'attribute_' + side))
        if not (directory / 'COMPLETE').exists():
            raise RuntimeError('incomplete attribution run')
        cases = {}
        for path in directory.glob('count-*.txt'):
            cases[path.stem[6:]] = json.loads(path.read_text())
        if len(cases) != 65:
            raise RuntimeError('expected 65 attribution cases per side')
        counts[side] = cases
        for path in directory.iterdir():
            if evidence_file(path):
                copy(path, out / ('attribute-' + side) / path.name)
    if metadata['before']['inputs'] != metadata['after']['inputs'] or metadata['before']['harness'] != metadata['after']['harness']:
        raise RuntimeError('timing inputs or harness differ')
    with (out / 'samples.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['side', 'case', 'operation', 'sample', 'ns', 'bytes', 'allocations'])
        writer.writeheader()
        writer.writerows(rows)
    summaries = {}
    for row in rows:
        key = row['case'] + '/' + row['operation']
        summaries.setdefault(key, {})
        for side in ['before', 'after']:
            if side in summaries[key]:
                continue
            samples = [v for v in rows if v['case'] == row['case'] and v['operation'] == row['operation'] and v['side'] == side]
            summaries[key][side] = {field: dict(median=statistics.median(v[field] for v in samples), minimum=min(v[field] for v in samples), maximum=max(v[field] for v in samples)) for field in ['ns', 'bytes', 'allocations']}
    (out / 'summary.json').write_text(json.dumps(summaries, indent=2))
    (out / 'attribution.json').write_text(json.dumps(counts, indent=2))
    inputs = Path(a.before) / 'inputs'
    inventory = {}
    for path in inputs.iterdir():
        if path.is_file():
            copy(path, out / 'inputs' / path.name)
            if path.suffix == '.journal':
                raw, offset, kinds = path.read_bytes(), 0, Counter()
                while offset < len(raw):
                    if raw[offset:offset + 8] != b'FRTOP001':
                        raise RuntimeError('invalid frozen input frame')
                    length = struct.unpack('>Q', raw[offset + 8:offset + 16])[0]
                    frame = raw[offset:offset + 16 + length]
                    if hashlib.sha256(b'forest-topology/record/v1\0' + frame).digest() != raw[offset + 16 + length:offset + 48 + length]:
                        raise RuntimeError('invalid frozen frame checksum')
                    kinds[json.loads(frame[16:])['kind']] += 1
                    offset += length + 48
                if offset != len(raw):
                    raise RuntimeError('truncated frozen input')
                inventory[path.stem] = dict(bytes=len(raw), records=sum(kinds.values()), kinds=dict(kinds))
    (out / 'journal-inventory.json').write_text(json.dumps(inventory, indent=2))
    audit = Path(a.audit)
    if not (audit / 'COMPLETE').exists():
        raise RuntimeError('incomplete result/profile audit')
    for path in audit.iterdir():
        if path.is_file():
            copy(path, out / 'audit' / path.name)
    controls = Path(a.interleaved)
    control_metadata = json.loads((controls / 'metadata.json').read_text())
    if not (controls / 'COMPLETE').exists():
        raise RuntimeError('incomplete interleaved controls')
    for side in ['before', 'after']:
        for key in ['source', 'harness', 'inputs']:
            if control_metadata[side][key] != metadata[side][key]:
                raise RuntimeError('interleaved source/inputs differ')
    for path in controls.iterdir():
        if path.is_file():
            copy(path, out / 'interleaved' / path.name)
    manifest = {p.relative_to(out).as_posix(): dict(bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in out.rglob('*') if p.is_file() and p.name not in ['sha256.json', 'planning', 'probe', 'instrument']}
    (out / 'sha256.json').write_text(json.dumps(manifest, indent=2))
    print(json.dumps(dict(observations=len(rows), operation_pairs=len(summaries), attribution_pairs=len(counts['before']), artifact_files=len(manifest))))


if __name__ == '__main__':
    main()
