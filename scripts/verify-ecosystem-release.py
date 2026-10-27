"""Download published modules outside this repository and exercise their CLI.

Run with real FOREST_POSTGRES_DSN/FOREST_NATS_URL and Python Playwright installed.
Does not publish tags or modify any checkout.
"""
import json
import os
import secrets
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest-agent": "v0.2.0",
    "github.com/Leeminjing/go-forest/adapters/gin": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/grpc": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/gorm": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/nats": "v0.1.0",
    "github.com/Leeminjing/go-forest/extensions/telemetry": "v0.1.0",
    "github.com/Leeminjing/go-forest/extensions/observatory": "v0.2.1",
    "github.com/Leeminjing/go-forest/examples/ecosystem": "v0.1.2",
    "github.com/Leeminjing/go-forest/testkit": "v0.1.0",
}


def main():
    with tempfile.TemporaryDirectory(prefix="forest-published-") as temporary:
        directory = Path(temporary)
        # Module sources/build context are outside the checkout. Keep only the
        # installed evidence binary ignored locally; Windows AV may retain an
        # image handle after child exit, preventing tempdir cleanup.
        bindir = ROOT / ".scratch" / "ecosystem" / ("published-" + secrets.token_hex(6))
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True, text=True, encoding="utf-8", stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/forest-consumer")
        assert run("go", "env", "GOSUMDB", capture=True).strip() != "off", "checksum verification must be enabled"
        (directory / "main.go").write_text('package main\nimport "github.com/Leeminjing/go-forest/examples/ecosystem"\nimport "github.com/Leeminjing/go-forest/testkit"\nfunc main(){ _ = ecosystem.Options{}; _ = testkit.Scope{} }\n', encoding="utf-8")
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
        assert not any("temporal" in module.lower() for module in records), "unexpected Temporal module"
        dependencies = run("go", "list", "-deps", "-test", "github.com/Leeminjing/go-forest/examples/ecosystem", capture=True)
        assert not any("temporal" in package.lower() for package in dependencies.splitlines()), "unexpected Temporal test dependency"
        run("go", "vet", "./...")
        run("go", "build", "./...")
        app = "github.com/Leeminjing/go-forest/examples/ecosystem"
        run("go", "install", app + "/cmd/demo@" + VERSIONS[app])
        binary = bindir / ("demo.exe" if os.name == "nt" else "demo")
        env["FOREST_ECOSYSTEM_BINARY"] = str(binary)
        subprocess.run([sys.executable, str(ROOT / "examples/ecosystem/tests/browser_test.py")], env=env, check=True)
        print("PASS: external published CLI + real database/message/Agent/browser roundtrip", flush=True)


if __name__ == "__main__":
    main()
