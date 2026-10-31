"""Verify public evidence after checkout, or directly from immutable Git blobs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PREFIX = 'docs/topology/planning-performance/data/'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--git-ref')
    args = parser.parse_args()
    if args.git_ref:
        manifest = json.loads(subprocess.check_output(['git', 'show', args.git_ref + ':' + PREFIX + 'sha256.json'], cwd=ROOT))
        requests = [(name, args.git_ref + ':' + PREFIX + name) for name in manifest]
        data = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=ROOT, input=''.join(key + '\n' for _, key in requests).encode())
        payloads, offset = {}, 0
        for name, _ in requests:
            end = data.index(b'\n', offset)
            fields = data[offset:end].split()
            if len(fields) != 3 or fields[1] != b'blob':
                raise RuntimeError('missing Git evidence: ' + name)
            length = int(fields[2])
            payloads[name] = data[end + 1:end + 1 + length]
            offset = end + 2 + length
        if offset != len(data):
            raise RuntimeError('unexpected Git batch content')
    else:
        directory = ROOT / PREFIX
        manifest = json.loads((directory / 'sha256.json').read_text())
        payloads = {}
        for name in manifest:
            path = (directory / name).resolve()
            if not path.is_relative_to(directory.resolve()):
                raise RuntimeError('artifact escapes data directory')
            payloads[name] = path.read_bytes()
    for name, expected in manifest.items():
        raw = payloads[name]
        if len(raw) != expected['bytes'] or hashlib.sha256(raw).hexdigest() != expected['sha256']:
            raise RuntimeError('public evidence bytes differ: ' + name)
    print(f'{len(manifest)} public artifacts match collected sizes and SHA-256 ({args.git_ref or "checkout"})')


if __name__ == '__main__':
    main()
