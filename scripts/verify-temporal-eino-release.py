"""Verify fixed modules outside the checkout and run their installed native CLI."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest/adapters/eino": "v0.1.0",
    "github.com/Leeminjing/go-forest/adapters/temporal": "v0.1.0",
    "github.com/Leeminjing/go-forest/examples/temporal-eino": "v0.1.0",
}


def main():
    if not os.environ.get("FOREST_TEMPORAL_ADDRESS"):
        raise RuntimeError("published acceptance requires a real Temporal server")
    suffix = secrets.token_hex(6)
    with tempfile.TemporaryDirectory(prefix="forest-temporal-eino-published-") as temporary:
        directory = Path(temporary)
        bindir = ROOT / ".scratch/temporal-eino" / ("published-" + suffix)
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))
        if env.get("GOSUMDB", "sum.golang.org") == "off":
            raise RuntimeError("published acceptance requires checksum verification")

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/temporal-eino-consumer")
        (directory / "main.go").write_text('''package main
import (
    "context"
    ef "github.com/Leeminjing/go-forest/adapters/eino"
    tf "github.com/Leeminjing/go-forest/adapters/temporal"
    demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
    "github.com/cloudwego/eino/adk"
)
// The consumer owns this narrow native API contract.
type resumable interface {
    Resume(context.Context, *adk.ResumeParams, ef.RunOptions, ef.Visitor) (ef.Result, error)
}
var _ resumable = ef.ADK(nil)
func main() { _ = demo.Options{}; _ = tf.ErrNestedScope }
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
        app = "github.com/Leeminjing/go-forest/examples/temporal-eino"
        run("go", "install", app + "/cmd/demo@" + VERSIONS[app])
        executable = bindir / ("demo.exe" if os.name == "nt" else "demo")
        evidence = json.loads(run(str(executable), "-queue", "published-" + suffix,
                                  "-id", "published-" + suffix, "-directory", str(directory / "state"), capture=True))
        assert evidence == {
            "State": "completed", "Text": "finished:v1:oak", "EffectVersion": "v1",
            "ResumeGeneration": 2, "Artifacts": 1, "Query": 1, "Resume": 1,
            "Apply": 1, "Released": True,
        }, evidence
        print(json.dumps(evidence), flush=True)
        print("PASS: fixed modules + installed native Temporal/Eino signal/resume/replace CLI", flush=True)


if __name__ == "__main__":
    main()
