"""Strict provenance checks shared by supplementary planning tools."""
import hashlib
import json
from pathlib import Path


def read_json(path):
    return json.loads(Path(path).read_text())


def verify_files(directory, inventory):
    directory = Path(directory).resolve()
    for name, expected in inventory.items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory):
            raise RuntimeError('inventory path escapes its archive')
        raw = path.read_bytes()
        digest = expected['sha256'] if isinstance(expected, dict) else expected
        if hashlib.sha256(raw).hexdigest() != digest or (isinstance(expected, dict) and len(raw) != expected['bytes']):
            raise RuntimeError('frozen file differs: ' + str(path))


def verify_primary(directory):
    directory = Path(directory).resolve()
    if not (directory / 'COMPLETE').exists():
        raise RuntimeError('incomplete primary evidence')
    metadata = read_json(directory / 'metadata.json')
    verify_files(directory / 'source/extensions/topology', metadata['source_files'])
    cases = set()
    for command in read_json(directory / 'commands.json'):
        if not command['name'].startswith('count-'):
            continue
        name = command['name'][6:].rsplit('-', 1)[0]
        fixture = metadata['inputs'][name]
        dimensions = command['dimensions']
        if any(dimensions[k] != v for k, v in fixture['dimensions'].items()):
            raise RuntimeError('input dimensions differ')
        for key, suffix in [('FOREST_PLANNING_INPUT', '.json'), ('FOREST_PLANNING_JOURNAL', '.journal')]:
            path = Path(dimensions[key]).resolve()
            if path.name != name + suffix:
                raise RuntimeError('input name differs')
            verify_files(path.parent, {path.name: fixture['files'][path.name]})
        cases.add(name)
    if cases != set(metadata['inputs']):
        raise RuntimeError('input inventory is incomplete')
    return metadata


def verify_attribution(directory, primary):
    directory = Path(directory).resolve()
    if not (directory / 'COMPLETE').exists():
        raise RuntimeError('incomplete attribution evidence')
    metadata = read_json(directory / 'metadata.json')
    for key in ['source', 'harness', 'inputs', 'environment']:
        if metadata[key] != primary[key]:
            raise RuntimeError('attribution identity differs: ' + key)
    verify_files(directory / 'topology', metadata['instrumented_files'])
    return metadata


def method_fingerprint(directory):
    directory = Path(directory)
    metadata = read_json(directory / 'metadata.json')
    processors = []
    for block in metadata['/proc/cpuinfo'].strip().split('\n\n'):
        fields = {}
        for line in block.splitlines():
            key, value = line.split(':', 1)
            key = key.strip()
            # Instantaneous clocks are observations, not a stable CPU identity.
            if key not in ['cpu MHz', 'bogomips']:
                fields[key] = value.strip()
        processors.append(fields)
    commands = []
    for entry in read_json(directory / 'commands.json'):
        if entry['name'].startswith('bench-'):
            commands.append(dict(name=entry['name'], command=entry['command'], dimensions=entry['dimensions']))
        elif entry['name'] == 'build':
            argv = list(entry['command'])
            argv[argv.index('-o') + 1] = '<binary-output>'
            commands.append(dict(name='build', command=argv))
    builds = []
    dependencies = []
    for line in (directory / 'binary-info.txt').read_text().splitlines():
        fields = line.strip().split('\t')
        if fields[0] == 'build' and not fields[1].startswith('vcs.'):
            builds.append(fields[1:])
        elif fields[0] == 'dep' and fields[1] != 'github.com/Leeminjing/go-forest/extensions/topology':
            dependencies.append(fields[1:])
    if not builds or not dependencies:
        raise RuntimeError('missing actual binary settings/dependencies')
    return dict(version=(directory / 'version.txt').read_text(), harness=metadata['harness'], environment=metadata['environment'],
                cpu=processors, cpu_quota=metadata['/sys/fs/cgroup/cpu.max'], memory_limit=metadata['/sys/fs/cgroup/memory.max'], commands=commands,
                binary_build_settings=builds, fixed_binary_dependencies=dependencies)


def verify_pair(before, after):
    left, right = verify_primary(before), verify_primary(after)
    if left['inputs'] != right['inputs']:
        raise RuntimeError('frozen input inventories differ')
    method = method_fingerprint(before)
    if method != method_fingerprint(after):
        raise RuntimeError('paired measurement method/environment differs')
    return method
