"""Download fixed modules outside the checkout and run their installed real CLI."""
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
    "github.com/Leeminjing/go-forest/adapters/temporal": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/gorm": "v0.1.0",
    "github.com/Leeminjing/go-forest/examples/cross-adapter": "v0.1.1",
}


def main():
    if not os.environ.get("FOREST_TEMPORAL_ADDRESS") or not os.environ.get("FOREST_POSTGRES_DSN"):
        raise RuntimeError("published acceptance requires real Temporal/PostgreSQL services")
    suffix = secrets.token_hex(6)
    with tempfile.TemporaryDirectory(prefix="forest-cross-published-") as temporary:
        directory = Path(temporary)
        bindir = ROOT / ".scratch/cross-adapter" / ("published-" + suffix)
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))
        if env.get("GOSUMDB", "sum.golang.org") == "off":
            raise RuntimeError("published acceptance requires module checksum verification")

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/cross-consumer")
        (directory / "main.go").write_text('''package main
import (
    "context"
    ff "github.com/Leeminjing/go-forest/adapters/fx"
    gf "github.com/Leeminjing/go-forest/adapters/gorm"
    tf "github.com/Leeminjing/go-forest/adapters/temporal"
    demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
    "github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)
// Consumer-owned interface; the implementation's internal package is invisible.
type processor interface {
    Process(context.Context, protocol.Request) (protocol.Result, error)
    Inspect(context.Context, protocol.Request) (protocol.Observation, error)
}
type access interface {
    Use(context.Context, func(context.Context, processor) error) error
}
var _ access = ff.Access[processor](nil)
func main() { _ = demo.Options{}; _ = gf.Owned; _ = tf.ErrNestedScope }
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
            assert not Path(record["Dir"]).resolve().is_relative_to(ROOT), module
            print(module + "@" + version + ": downloaded without replace", flush=True)
        run("go", "vet", "./...")
        run("go", "build", "./...")
        app = "github.com/Leeminjing/go-forest/examples/cross-adapter"
        run("go", "install", app + "/cmd/demo@" + VERSIONS[app])
        binary = bindir / ("demo.exe" if os.name == "nt" else "demo")
        evidence = json.loads(run(str(binary), "-setup", "-queue", "published-" + suffix,
                                  "-id", "published-" + suffix, "-directory", str(directory / "native"), capture=True))
        assert evidence == {
            "FirstVersion": "v1", "LaterVersion": "v2", "ReceiptVersion": "v1",
            "Reused": True, "EffectCount": 1, "Generation": 2,
        }, evidence
        print(json.dumps(evidence), flush=True)
        print("PASS: fixed modules + installed Fx/Temporal/PostgreSQL CLI", flush=True)


if __name__ == "__main__":
    main()
