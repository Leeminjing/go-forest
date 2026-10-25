"""Download fixed Eino modules outside the checkout and run the installed demo."""
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
    "github.com/Leeminjing/go-forest/adapters/eino": "v0.1.0",
    "github.com/Leeminjing/go-forest/examples/eino": "v0.1.0",
}


def main():
    with tempfile.TemporaryDirectory(prefix="forest-eino-published-") as temporary:
        directory = Path(temporary)
        bindir = ROOT / ".scratch" / "eino" / ("published-" + secrets.token_hex(6))
        bindir.mkdir(parents=True)
        env = dict(os.environ, GOWORK="off", GOBIN=str(bindir))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/eino-consumer")
        (directory / "main.go").write_text('''package main
import (
    "context"
    demo "github.com/Leeminjing/go-forest/examples/eino"
    ef "github.com/Leeminjing/go-forest/adapters/eino"
    "github.com/cloudwego/eino/adk"
)
// This consumer owns its interface; native event semantics remain available.
type native interface {
    Query(context.Context, string, ef.RunOptions, ef.Visitor) (ef.Result, error)
}
var _ native = ef.ADK(nil)
func main() { _ = demo.Options{}; _ = adk.ResumeParams{} }
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
        app = "github.com/Leeminjing/go-forest/examples/eino"
        run("go", "install", app + "/cmd/demo@" + VERSIONS[app])
        binary = bindir / ("demo.exe" if os.name == "nt" else "demo")
        evidence = json.loads(run(str(binary), "-checkpoints", str(directory / "checkpoints"), capture=True))
        assert evidence == {"eino": "eino:v1:oak/v1:oak", "forest_agent": "forest:v1:oak/v1:oak", "generation": 2, "trees": 12}, evidence
        print(json.dumps(evidence, ensure_ascii=False), flush=True)
        print("PASS: downloaded fixed versions + installed dual-engine CLI", flush=True)


if __name__ == "__main__":
    main()
