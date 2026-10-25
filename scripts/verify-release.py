"""Copy the version-pinned consumer outside this checkout and test releases."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="go-forest-consumer-") as temporary:
    target = Path(temporary)
    for source in (root / "tests" / "release-consumer").glob("*"):
        if source.is_file():
            shutil.copy2(source, target / source.name)
    environment = dict(os.environ, GOWORK="off")

    def run(*arguments, capture=False):
        return subprocess.run(arguments, cwd=target, env=environment, check=True, text=True, encoding="utf-8", stdout=subprocess.PIPE if capture else None).stdout

    modules = ["github.com/Leeminjing/go-forest/extensions/observe", "github.com/Leeminjing/go-forest/adapters/gin", "github.com/Leeminjing/go-forest/extensions/observatory"]
    run("go", "get", *(module + "@v0.1.0" for module in modules))
    run("go", "mod", "tidy")
    payload = run("go", "list", "-m", "-json", "all", capture=True)
    decoder, records = json.JSONDecoder(), {}
    while payload.strip():
        record, position = decoder.raw_decode(payload.lstrip())
        payload = payload.lstrip()[position:]
        assert "Replace" not in record, "unexpected dependency replacement"
        records[record["Path"]] = record
    for module in modules:
        assert records[module]["Version"] == "v0.1.0", records[module]
        assert not Path(records[module]["Dir"]).is_relative_to(root)
        print(module + "@v0.1.0: downloaded module, no replace", flush=True)
    assert records["github.com/Leeminjing/go-forest"]["Version"] == "v0.3.0"
    run("go", "vet", "./...")
    run("go", "test", "./...", "-count=1", "-timeout", "30s")
    print("PASS: external HTTP/SSE, topology, pinned replacement, draining, stopped, Close", flush=True)
