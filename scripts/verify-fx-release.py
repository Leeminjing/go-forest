"""Verify fixed Fx modules outside the checkout and execute the installed demo."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest/adapters/fx": "v0.1.0",
    "github.com/Leeminjing/go-forest/examples/fx": "v0.1.0",
}


def main():
    with tempfile.TemporaryDirectory(prefix="forest-fx-published-") as temporary:
        directory = Path(temporary)
        bindir = ROOT / ".scratch" / "fx" / ("published-" + secrets.token_hex(6))
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/fx-consumer")
        (directory / "main.go").write_text('''package main
import (
    "context"
    ff "github.com/Leeminjing/go-forest/adapters/fx"
    demo "github.com/Leeminjing/go-forest/examples/fx"
    "go.uber.org/fx"
)
// The caller owns its narrow contracts; no provider package is required.
type service interface { Execute(context.Context) error }
type access interface {
    Use(context.Context, func(context.Context, service) error) error
}
var _ access = ff.Access[service](nil)
func main() { _ = demo.Options{}; _ = fx.Module("native") }
''', encoding="utf-8")
        run("go", "get", *(module + "@" + version for module, version in VERSIONS.items()))
        run("go", "mod", "tidy")
        run("go", "mod", "verify")
        payload = run("go", "list", "-m", "-json", "all", capture=True)
        decoder, records = json.JSONDecoder(), {}
        while payload.strip():
            payload = payload.lstrip()
            record, position = decoder.raw_decode(payload)
            payload = payload[position:]
            assert "Replace" not in record, "published graph contains replace"
            records[record["Path"]] = record
        for module, version in VERSIONS.items():
            record = records[module]
            assert record["Version"] == version, module
            assert not Path(record["Dir"]).is_relative_to(ROOT), module
            print(module + "@" + version + ": downloaded without replace", flush=True)
        run("go", "vet", "./...")
        run("go", "build", "./...")
        app = "github.com/Leeminjing/go-forest/examples/fx"
        run("go", "install", app + "/cmd/demo@" + VERSIONS[app])
        binary = bindir / ("demo.exe" if os.name == "nt" else "demo")
        evidence = json.loads(run(str(binary), capture=True))
        assert evidence == {
            "Before": "v1:oak/v1:oak",
            "Background": "v2:elm/v2:elm",
            "HTTP": {"text": "v2:ash/v2:ash", "generation": 2},
        }, evidence
        print(json.dumps(evidence), flush=True)
        print("PASS: fixed modules + installed native Fx/HTTP/background CLI", flush=True)


if __name__ == "__main__":
    main()
