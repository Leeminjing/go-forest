"""Representative ABBA controls using the unchanged ordinary timing harness."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--before', required=True)
    p.add_argument('--after', required=True)
    p.add_argument('--out', required=True)
    a = p.parse_args()
    out = Path(a.out).resolve()
    out.mkdir(parents=True)
    env = dict(os.environ, GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='', GOMAXPROCS='4')
    commands, inputs, binaries, metadata = [], {}, {}, {}

    def run(name, command, cwd, dimensions=None):
        print(name, flush=True)
        commands.append(dict(name=name, command=command, cwd=str(cwd), dimensions=dimensions or {}))
        (out / 'commands.json').write_text(json.dumps(commands, indent=2))
        with (out / (name + '.txt')).open('xb') as log:
            subprocess.run(command, cwd=cwd, env=dict(env, **(dimensions or {})), stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)

    for side in ['before', 'after']:
        primary = Path(getattr(a, side)).resolve()
        if not (primary / 'COMPLETE').exists():
            raise RuntimeError('incomplete primary run')
        info = json.loads((primary / 'metadata.json').read_text())
        metadata[side] = dict(source=info['source'], harness=info['harness'], inputs=info['inputs'])
        project = out / side
        shutil.copytree(primary / 'harness', project)
        for item in json.loads((primary / 'commands.json').read_text()):
            if item['name'] in ['bench-medium', 'bench-large']:
                inputs[item['name'][6:]] = item['dimensions']
        topology = primary / 'source/extensions/topology'
        run(side + '-configure', ['go', 'mod', 'edit', '-replace=github.com/Leeminjing/go-forest/extensions/topology=' + str(topology)], project)
        run(side + '-tidy', ['go', 'mod', 'tidy'], project)
        run(side + '-modules', ['go', 'list', '-m', '-json', 'all'], project)
        binary = project / 'bench'
        run(side + '-build', ['go', 'test', '-c', '-pgo=off', '-buildvcs=false', '-o', str(binary)], project)
        run(side + '-binary', ['go', 'version', '-m', str(binary)], project)
        binaries[side] = binary
    if metadata['before']['harness'] != metadata['after']['harness'] or metadata['before']['inputs'] != metadata['after']['inputs']:
        raise RuntimeError('different controls')
    for case, operation in [('medium', 'PreviewCommit'), ('large', '(Capture|Preview)')]:
        for number, side in enumerate(['before', 'after', 'after', 'before'] * 2):
            run(f'control-{case}-{number}-{side}', [str(binaries[side]), '-test.run=^$', '-test.bench=^BenchmarkPlanning/' + operation + '$', '-test.benchtime=1x', '-test.benchmem', '-test.cpu=1', '-test.count=1'], out / side, inputs[case])
    metadata['runner_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (out / 'metadata.json').write_text(json.dumps(metadata, indent=2))
    (out / 'COMPLETE').write_text('two ABBA rounds for medium PreviewCommit and large Capture/Preview completed\n')


if __name__ == '__main__':
    main()
