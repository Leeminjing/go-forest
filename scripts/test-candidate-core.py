"""Isolated candidate-Core regression; never labelled published acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--module", action="append")
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--services", choices=["ecosystem", "cross-adapter", "temporal-eino", "temporal", "topology"])
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("positive count required")
    tools = runpy.run_path(str(ROOT / "scripts/measure-core.py"))
    directory = ROOT / ".scratch/core-performance" / (time.strftime("%Y%m%d-%H%M%S") + "-candidate-" + os.urandom(3).hex())
    directory.mkdir(parents=True)
    source = directory / "source"
    revision = tools["archive"](args.ref, source)
    real_go = shutil.which("go")
    env = dict(os.environ, GOWORK="off", GOFLAGS="-mod=mod")
    hashes = tools["manifest"](source, ["forest/*.go", "go.mod", "go.sum"])
    manifest = directory / "core-files.json"
    manifest.write_text(json.dumps(hashes, indent=2), encoding="utf-8")
    binaries, logs = directory / "bin", directory / "go-invocations"
    binaries.mkdir(); logs.mkdir()
    guard = binaries / ("go.exe" if os.name == "nt" else "go")
    subprocess.run([real_go, "build", "-o", str(guard), "./scripts/go-candidate-guard"], cwd=ROOT, env=env, check=True)
    entries = sorted(p for p in source.rglob("go.mod") if ".scratch" not in p.relative_to(source).parts)
    changes = []
    for path in entries:
        module = path.parent
        before = path.read_bytes()
        if module != source:
            # Only the private archive's manifests are changed; no source
            # Adapter/API/version pin in the user's checkout is rewritten.
            subprocess.run([real_go, "mod", "edit", "-replace=github.com/Leeminjing/go-forest=" + str(source)], cwd=module, env=env, check=True)
        changes.append({"module": module.relative_to(source).as_posix(), "before": hashlib.sha256(before).hexdigest(), "after": hashlib.sha256(path.read_bytes()).hexdigest()})
    env.update(FOREST_CANDIDATE_REAL_GO=real_go, FOREST_CANDIDATE_ROOT=str(source),
               FOREST_CANDIDATE_LOGS=str(logs), FOREST_CANDIDATE_MANIFEST=str(manifest))
    env["PATH"] = str(binaries) + os.pathsep + env["PATH"]
    commands = []

    def run(name, command, cwd):
        commands.append({"name": name, "command": command, "cwd": str(cwd)})
        (directory / "commands.json").write_text(json.dumps(commands, indent=2), encoding="utf-8")
        print("CANDIDATE " + revision[:7] + ": " + name, flush=True)
        with (directory / (name + ".log")).open("x", encoding="utf-8") as output:
            result = subprocess.run(command, cwd=cwd, env=env, text=True, stdout=output, stderr=subprocess.STDOUT, timeout=1800)
        if result.returncode:
            raise RuntimeError(name + " failed: " + str(directory / (name + ".log")))

    metadata = {"revision": revision, "source": str(source), "manifest_changes": changes, "arguments": vars(args)}
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    if args.services:
        scripts = {"ecosystem": "test-ecosystem.py", "cross-adapter": "test-cross-adapter.py", "temporal-eino": "test-temporal-eino.py", "temporal": "test-temporal.py", "topology": "test-topology-incremental.py"}
        command = [sys.executable, str(source / "scripts" / scripts[args.services]), "--count", str(args.count)]
        if args.race:
            command.append("--race")
        if args.browser:
            if args.services != "ecosystem":
                raise RuntimeError("service browser mode is ecosystem-specific")
            command.append("--browser")
        run("service-" + args.services, command, source)
    elif args.browser:
        run("observatory-browser", [sys.executable, str(source / "extensions/observatory/tests/browser_test.py")], source)
        run("frameworks-browser", [sys.executable, str(source / "examples/frameworks/tests/demo_test.py")], source)
    else:
        modules = args.module or [p.parent.relative_to(source).as_posix() for p in entries]
        for name in modules:
            module = (source / name).resolve()
            if not module.is_relative_to(source.resolve()) or not (module / "go.mod").exists():
                raise RuntimeError("invalid module: " + name)
            slug = name.replace("/", "-").replace(".", "root")
            run(slug + "-tidy", [str(guard), "mod", "tidy"], module)
            run(slug + "-modules", [str(guard), "list", "-m", "-json", "all"], module)
            run(slug + "-deps", [str(guard), "list", "-deps", "-json", "./..."], module)
            raw = (directory / (slug + "-deps.log")).read_text(encoding="utf-8")
            decoder = json.JSONDecoder()
            while raw.strip():
                package, length = decoder.raw_decode(raw.lstrip()); raw = raw.lstrip()[length:]
                if package.get("ImportPath", "").startswith("github.com/Leeminjing/go-forest/") and package.get("Module", {}).get("Path") == "github.com/Leeminjing/go-forest":
                    if not Path(package["Dir"]).resolve().is_relative_to(source.resolve()):
                        raise RuntimeError("package compiled outside candidate Core: " + package["ImportPath"])
            run(slug + "-vet", [str(guard), "vet", "./..."], module)
            run(slug + "-test", [str(guard), "test", *(["-race"] if args.race else []), "./...", "-count=" + str(args.count), "-timeout=600s"], module)
    proofs = [json.loads(p.read_text(encoding="utf-8")) for p in logs.glob("*.json")]
    if not any(p.get("Checked") for p in proofs) or any(p["Error"] for p in proofs):
        raise RuntimeError("missing or failed candidate Core build provenance")
    (directory / "proof-summary.json").write_text(json.dumps({"go_invocations": len(proofs), "verified_Core_invocations": sum(p["UsesCore"] for p in proofs), "proven_no_Core_invocations": sum(p.get("Checked", False) and not p["UsesCore"] for p in proofs)}, indent=2), encoding="utf-8")
    (directory / "COMPLETE").write_text("candidate regression and dependency guard passed\n", encoding="utf-8")
    print("saved candidate evidence: " + str(directory), flush=True)


if __name__ == "__main__":
    main()
