"""Download fixed modules outside the checkout and run the published real CLI.

Requires owned test FOREST_TEMPORAL_ADDRESS and FOREST_POSTGRES_DSN; normally
invoked by scripts/test-temporal.py --published after release tags exist.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest-agent": "v0.2.0",
    "github.com/Leeminjing/go-forest/adapters/gorm": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/temporal": "v0.1.0",
    "github.com/Leeminjing/go-forest/examples/temporal": "v0.1.0",
}


def main():
    address = os.environ["FOREST_TEMPORAL_ADDRESS"]
    if not os.environ.get("FOREST_POSTGRES_DSN"):
        raise RuntimeError("real test PostgreSQL configuration required")
    with tempfile.TemporaryDirectory(prefix="forest-temporal-published-") as temporary:
        directory = Path(temporary)
        suffix = secrets.token_hex(6)
        bindir = ROOT / ".scratch" / "temporal" / ("published-" + suffix)
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True, text=True,
                                  encoding="utf-8", stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/temporal-consumer")
        (directory / "main.go").write_text('''package main
import (
    "context"
    tf "github.com/Leeminjing/go-forest/adapters/temporal"
    demo "github.com/Leeminjing/go-forest/examples/temporal"
    "go.temporal.io/sdk/client"
)
type nativeClient interface { Use(context.Context,func(client.Client)error)error }
var _ nativeClient = tf.ClientAccess(nil)
func main(){ _ = demo.Options{} }
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
        app = "github.com/Leeminjing/go-forest/examples/temporal"
        run("go", "install", app + "/cmd/demo@v0.1.0")
        executable = bindir / ("demo.exe" if os.name == "nt" else "demo")
        value = json.loads(run(str(executable), "-setup", "-address", address,
                               "-queue", "published-" + suffix, "-id", "published-" + suffix,
                               "-directory", str(directory / "agent"), capture=True))
        receipt = value["Result"]["Receipt"]
        assert receipt["ID"] == "published-" + suffix and receipt["Changes"] == 1 and receipt["Plan"] == "plan:oak", value
        assert value["Run"]["RunID"] and value["Result"]["ReadVersion"] == "1", value
        print(json.dumps(value), flush=True)
        print("PASS: fixed modules + installed CLI + actual Temporal/Agent/PostgreSQL", flush=True)


if __name__ == "__main__":
    main()
