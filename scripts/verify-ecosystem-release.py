"""Download published modules outside this repository and exercise their CLI.

Run with real FOREST_POSTGRES_DSN/FOREST_NATS_URL and Python Playwright installed.
Does not publish tags or modify any checkout.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest/adapters/gorm": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/nats": "v0.1.0",
    "github.com/Leeminjing/go-forest/extensions/telemetry": "v0.1.0",
    "github.com/Leeminjing/go-forest/extensions/observatory": "v0.2.0",
    "github.com/Leeminjing/go-forest/examples/ecosystem": "v0.1.0",
}


def main():
    with tempfile.TemporaryDirectory(prefix="forest-published-") as temporary:
        directory = Path(temporary)
        env = dict(os.environ, GOWORK="off", GOBIN=str(directory / "bin"))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True, text=True, encoding="utf-8", stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/forest-consumer")
        (directory / "main.go").write_text('package main\nimport "github.com/Leeminjing/go-forest/examples/ecosystem"\nfunc main(){ _ = ecosystem.Options{} }\n', encoding="utf-8")
        run("go", "get", *(module + "@" + version for module, version in VERSIONS.items()))
        run("go", "mod", "tidy")
        run("go", "mod", "verify")
        payload = run("go", "list", "-m", "-json", "all", capture=True)
        decoder, records = json.JSONDecoder(), {}
        while payload.strip():
            record, position = decoder.raw_decode(payload.lstrip())
            payload = payload.lstrip()[position:]
            assert "Replace" not in record, "published graph contains a replacement"
            records[record["Path"]] = record
        for module, version in VERSIONS.items():
            record = records[module]
            assert record["Version"] == version, module
            assert not Path(record["Dir"]).is_relative_to(ROOT), module
            print(module + "@" + version + ": downloaded, no replace", flush=True)
        run("go", "vet", "./...")
        run("go", "build", "./...")
        run("go", "install", "github.com/Leeminjing/go-forest/examples/ecosystem/cmd/demo@v0.1.0")
        binary = directory / "bin" / ("demo.exe" if os.name == "nt" else "demo")
        env["FOREST_ECOSYSTEM_BINARY"] = str(binary)
        subprocess.run([sys.executable, str(ROOT / "examples/ecosystem/tests/browser_test.py")], env=env, check=True)
        print("PASS: external published CLI + real database/message/Agent/browser roundtrip", flush=True)


if __name__ == "__main__":
    main()
