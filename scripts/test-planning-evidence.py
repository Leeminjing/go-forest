"""Negative provenance regressions using actual released journal fixtures."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from planning_evidence import verify_attribution, verify_pair, verify_primary

ROOT = Path(__file__).resolve().parents[1]


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = self.root / 'inputs'
        self.inputs.mkdir()
        journal = ROOT / 'extensions/topology/testdata/legacy-v0.1/history/journal'
        shutil.copyfile(journal, self.inputs / 'small.journal')
        (self.inputs / 'small.json').write_text('{"Before":{"version":1,"trees":[],"bindings":[]},"Target":{"version":1,"trees":[],"bindings":[]},"Generation":0}')
        self.fixture = dict(dimensions={'FOREST_PERF_N': '32'}, files={p.name: dict(bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in self.inputs.iterdir()})
        self.before, self.after = self.primary('before'), self.primary('after')

    def primary(self, side):
        directory = self.root / side
        source = directory / 'source/extensions/topology'
        source.mkdir(parents=True)
        (source / 'fixture.go').write_text('package topology\n')
        metadata = dict(source=side, harness='frozen', inputs={'small': self.fixture}, environment={'GOWORK': 'off'}, source_files={'fixture.go': hashlib.sha256((source / 'fixture.go').read_bytes()).hexdigest()}, **{'/proc/cpuinfo': 'processor : 0\nmodel name : same CPU\ncpu MHz : 100\n', '/sys/fs/cgroup/cpu.max': '800000 100000\n', '/sys/fs/cgroup/memory.max': '8589934592\n'})
        dimensions = dict(FOREST_PERF_N='32', FOREST_PLANNING_INPUT=str(self.inputs / 'small.json'), FOREST_PLANNING_JOURNAL=str(self.inputs / 'small.journal'))
        commands = [dict(name='count-small-Open', dimensions=dimensions), dict(name='bench-small', command=['go', 'test', '-cpu=1', '-benchtime=1x'], dimensions=dimensions), dict(name='build', command=['go', 'build', '-pgo=off', '-o', str(directory / 'binary')])]
        (directory / 'metadata.json').write_text(json.dumps(metadata))
        (directory / 'commands.json').write_text(json.dumps(commands))
        (directory / 'version.txt').write_text('go version go1.23.12 linux/amd64\n')
        (directory / 'binary-info.txt').write_text('binary: go1.23.12\n\tdep\tgithub.com/Leeminjing/go-forest\tv0.3.1\tfixed-sum\n\tbuild\tGOAMD64=v1\n\tbuild\tCGO_ENABLED=1\n')
        (directory / 'COMPLETE').write_text('complete\n')
        return directory

    def test_identical_method_allows_only_expected_source_output_differences(self):
        self.assertTrue(verify_pair(self.before, self.after))

    def test_different_valid_released_journal_is_rejected_before_work(self):
        alternate = ROOT / 'extensions/topology/testdata/legacy-v0.1/pending/journal'
        self.assertNotEqual(alternate.read_bytes(), (self.inputs / 'small.journal').read_bytes())
        shutil.copyfile(alternate, self.inputs / 'small.journal')
        with self.assertRaisesRegex(RuntimeError, 'frozen file differs'):
            verify_primary(self.before)

    def test_changed_toolchain_cpu_quota_environment_and_flags_are_not_paired(self):
        for field in ['version', 'cpu', 'quota', 'memory', 'environment', 'flags', 'GOAMD64', 'CGO_ENABLED']:
            with self.subTest(field=field):
                metadata = json.loads((self.after / 'metadata.json').read_text())
                commands = json.loads((self.after / 'commands.json').read_text())
                old_metadata, old_commands = dict(metadata), json.loads(json.dumps(commands))
                version = (self.after / 'version.txt').read_text()
                binary_info = (self.after / 'binary-info.txt').read_text()
                if field == 'version': (self.after / 'version.txt').write_text('go version go1.26.0 linux/amd64\n')
                if field == 'cpu': metadata['/proc/cpuinfo'] = metadata['/proc/cpuinfo'].replace('same CPU', 'other CPU')
                if field == 'quota': metadata['/sys/fs/cgroup/cpu.max'] = '400000 100000\n'
                if field == 'memory': metadata['/sys/fs/cgroup/memory.max'] = '4294967296\n'
                if field == 'environment': metadata['environment'] = {'GOWORK': 'auto'}
                if field == 'flags': commands[1]['command'].append('-race')
                if field == 'GOAMD64': (self.after / 'binary-info.txt').write_text(binary_info.replace('GOAMD64=v1', 'GOAMD64=v3'))
                if field == 'CGO_ENABLED': (self.after / 'binary-info.txt').write_text(binary_info.replace('CGO_ENABLED=1', 'CGO_ENABLED=0'))
                (self.after / 'metadata.json').write_text(json.dumps(metadata))
                (self.after / 'commands.json').write_text(json.dumps(commands))
                with self.assertRaisesRegex(RuntimeError, 'method/environment differs'):
                    verify_pair(self.before, self.after)
                (self.after / 'metadata.json').write_text(json.dumps(old_metadata))
                (self.after / 'commands.json').write_text(json.dumps(old_commands))
                (self.after / 'version.txt').write_text(version)
                (self.after / 'binary-info.txt').write_text(binary_info)

    def test_a_different_instrumented_source_is_rejected(self):
        primary = verify_primary(self.before)
        attributed = self.root / 'attribute'
        source = attributed / 'topology'
        source.mkdir(parents=True)
        path = source / 'fixture.go'
        path.write_text('package topology\n')
        metadata = {key: primary[key] for key in ['source', 'harness', 'inputs', 'environment']}
        metadata['instrumented_files'] = {'fixture.go': hashlib.sha256(path.read_bytes()).hexdigest()}
        (attributed / 'metadata.json').write_text(json.dumps(metadata))
        (attributed / 'COMPLETE').write_text('complete\n')
        verify_attribution(attributed, primary)
        path.write_text('package different\n')
        with self.assertRaisesRegex(RuntimeError, 'frozen file differs'):
            verify_attribution(attributed, primary)


if __name__ == '__main__':
    unittest.main()
