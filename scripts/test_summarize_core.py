"""The analysis must refuse plausible-looking data with incompatible inputs."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('summarize',Path(__file__).with_name('summarize-core.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class PairingTests(unittest.TestCase):
    def test_environment_and_dimensions_are_part_of_pair_identity(self):
        with tempfile.TemporaryDirectory(prefix='forest-analysis-') as directory:
            path=Path(directory)
            (path/'go-version.txt').write_text('go version go1.23.12 linux/amd64\n')
            meta=dict(harness='frozen',platform='Linux',machine='x86_64',cpu_count=8,environment={'GOMAXPROCS':'4'})
            command=dict(dimensions={'FOREST_PERF_E':'1'},command=['go','test','-benchmem','-timeout=10m'])
            original=module.method_identity(path,meta,command)[0]
            # Timeout does not change the measured work. Resource limits do.
            self.assertEqual(original,module.method_identity(path,meta,{**command,'command':['go','test','-benchmem','-timeout=60m']})[0])
            for changed in [{**meta,'environment':{'GOMAXPROCS':'8'}},{**meta,'/sys/fs/cgroup/cpu.max':'400000 100000'},{**meta,'harness':'another'}]:
                self.assertNotEqual(original,module.method_identity(path,changed,command)[0])
            self.assertNotEqual(original,module.method_identity(path,meta,{**command,'dimensions':{'FOREST_PERF_E':'10001'}})[0])
            (path/'go-version.txt').write_text('go version go1.26.5 linux/amd64\n')
            self.assertNotEqual(original,module.method_identity(path,meta,command)[0])

if __name__=='__main__': unittest.main()
