"""Supplement frozen timing with result parity and GC-flushed allocation profiles."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from planning_evidence import verify_attribution, verify_pair, verify_primary

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--before', required=True)
    parser.add_argument('--after', required=True)
    parser.add_argument('--attribute-before', required=True)
    parser.add_argument('--attribute-after', required=True)
    parser.add_argument('--harness-ref', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    method = verify_pair(args.before, args.after)
    attributed = {}
    for side in ['before', 'after']:
        primary = verify_primary(getattr(args, side))
        attributed[side] = verify_attribution(getattr(args, 'attribute_' + side), primary)
    if attributed['before']['tool'] != attributed['after']['tool']:
        raise RuntimeError('different attribution tools')
    out = Path(args.out).resolve()
    out.mkdir(parents=True)
    env = dict(os.environ, GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='', GOMAXPROCS='4')
    commands, metadata, results = [], {}, {}

    def git(*argv):
        return subprocess.check_output(['git', '-c', 'safe.directory=' + str(ROOT), *argv], cwd=ROOT)

    harness_ref = git('rev-parse', args.harness_ref).decode().strip()
    overlay = ['planning.go', 'planning_audit.go', 'cmd/planningaudit/main.go']
    files = {name: git('show', harness_ref + ':tests/performance/' + name) for name in overlay}

    def run(name, command, cwd, dimensions=None):
        print(name, flush=True)
        commands.append(dict(name=name, command=command, cwd=str(cwd), dimensions=dimensions or {}))
        (out / 'commands.json').write_text(json.dumps(commands, indent=2))
        try:
            data = subprocess.check_output(command, cwd=cwd, env=dict(env, **(dimensions or {})), stderr=subprocess.STDOUT, timeout=900)
        except subprocess.CalledProcessError as error:
            (out / (name + '-error.txt')).write_bytes(error.output)
            raise
        if name.startswith('audit-'):
            with gzip.open(out / (name + '.json.gz'), 'wb') as stream:
                stream.write(data)
        else:
            (out / (name + '.txt')).write_bytes(data)
        return data

    for side, primary in [('before', Path(args.before)), ('after', Path(args.after))]:
        primary = primary.resolve()
        if not (primary / 'COMPLETE').exists():
            raise RuntimeError('primary run is incomplete')
        original = json.loads((primary / 'metadata.json').read_text())
        counts = [v for v in json.loads((primary / 'commands.json').read_text()) if v['name'].startswith('count-')]
        dimensions = {}
        for item in counts:
            name = item['name'][6:].rsplit('-', 1)[0]
            dimensions[name] = item['dimensions']
        metadata[side] = dict(source=original['source'], timing_harness=original['harness'], inputs=original['inputs'])
        actual_instrumented = Path(getattr(args, 'attribute_' + side)).resolve() / 'topology'
        for mode, topology in [('ordinary', primary / 'source/extensions/topology'), ('instrumented', actual_instrumented)]:
            project = out / (side + '-' + mode)
            shutil.copytree(primary / 'harness', project)
            for name, data in files.items():
                path = project / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            prefix = side + '-' + mode
            run(prefix + '-configure', ['go', 'mod', 'edit', '-replace=github.com/Leeminjing/go-forest/extensions/topology=' + str(topology)], project)
            run(prefix + '-tidy', ['go', 'mod', 'tidy'], project)
            modules = run(prefix + '-modules', ['go', 'list', '-m', '-json', 'all'], project).decode()
            selected, decoder = {}, json.JSONDecoder()
            while modules.strip():
                modules = modules.lstrip()
                module, end = decoder.raw_decode(modules)
                selected[module['Path']] = module
                modules = modules[end:]
            core = selected['github.com/Leeminjing/go-forest']
            target = selected['github.com/Leeminjing/go-forest/extensions/topology']
            if core.get('Replace') or core['Version'] != 'v0.3.1' or Path(target['Replace']['Dir']).resolve() != topology.resolve():
                raise RuntimeError('supplement selected wrong sources')
            binary = project / 'audit'
            run(prefix + '-build', ['go', 'build', '-tags=planningprobe', '-pgo=off', '-buildvcs=false', '-o', str(binary), './cmd/planningaudit'], project)
            run(prefix + '-binary', ['go', 'version', '-m', str(binary)], project)
            for name, dims in dimensions.items():
                data = json.loads(run('audit-' + prefix + '-' + name, [str(binary)], project, dims))
                prior = results.setdefault(name, data['Semantic'])
                if data['Semantic'] != prior:
                    raise RuntimeError('semantic audit differs: ' + prefix + '-' + name)
            if mode == 'ordinary':
                profiler = project / 'profile'
                run(prefix + '-profile-build', ['go', 'build', '-pgo=off', '-buildvcs=false', '-o', str(profiler), './cmd/planning'], project)
                for operation in ['Preview', 'Commit', 'PreviewCommit', 'Open']:
                    path = out / (side + '-' + operation + '-allocs-gc.pprof')
                    dims = dimensions['open-large' if operation == 'Open' else 'large']
                    run(prefix + '-' + operation + '-profile', [str(profiler), '-mode=profile', '-profile=allocs', '-operation=' + operation, '-out=' + str(path)], project, dims)
                    run(prefix + '-' + operation + '-top', ['go', 'tool', 'pprof', '-top', '-alloc_space', str(profiler), str(path)], project)
    metadata.update(method=method, attribution=attributed, harness=harness_ref, overlays={k: hashlib.sha256(v).hexdigest() for k, v in files.items()}, cases=len(results), builds=4,
                    notes='Raw results retained. Only independently verified random identities renamed; Position.Sequence, generation, and retained/new instance relations preserved. Open projections compare exact frozen journal. Profiles include setup and GC-flushed operation allocations; ordinary timing harness unchanged.')
    (out / 'metadata.json').write_text(json.dumps(metadata, indent=2))
    (out / 'COMPLETE').write_text(f'all four builds agree on {len(results)} result audits; eight GC-flushed allocation profiles saved\n')


if __name__ == '__main__':
    main()
