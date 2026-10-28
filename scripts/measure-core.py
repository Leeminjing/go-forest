"""Freeze Core/harness sources and save repeatable raw measurements. No Core edits."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]


def git(*args, **kwargs):
    return subprocess.run(["git", "-c", "safe.directory=" + str(ROOT), *args],
                          cwd=ROOT, check=True, **kwargs)


def archive(ref, destination):
    sha = git("rev-parse", ref, stdout=subprocess.PIPE, text=True).stdout.strip()
    tarpath = destination.parent / (destination.name + ".tar")
    with tarpath.open("wb") as output:
        git("archive", sha, stdout=output)
    destination.mkdir()
    with tarfile.open(tarpath) as source:
        for member in source.getmembers():
            path = (destination / member.name).resolve()
            if not path.is_relative_to(destination.resolve()) or not (member.isfile() or member.isdir()):
                raise RuntimeError("unsupported archive entry: " + member.name)
        source.extractall(destination, filter="data")
    tarpath.unlink()
    return sha


def manifest(directory, patterns):
    files = sorted({p for pattern in patterns for p in directory.glob(pattern) if p.is_file()})
    return {p.relative_to(directory).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--core-ref", required=True)
    parser.add_argument("--harness-ref", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--matrix", choices=["smoke", "study"], default="smoke")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--benchtime", default="1s")
    parser.add_argument("--profiles", action="store_true")
    args = parser.parse_args()
    if args.count < 1 or not args.label.replace("-", "").replace("_", "").isalnum():
        parser.error("invalid count/label")
    parent = ROOT / ".scratch/core-performance"
    parent.mkdir(parents=True, exist_ok=True)
    directory = parent / (time.strftime("%Y%m%d-%H%M%S") + "-" + args.label)
    directory.mkdir()  # never overwrite an earlier run
    core, harness_source = directory / "core", directory / "harness-source"
    core_sha = archive(args.core_ref, core)
    harness_sha = archive(args.harness_ref, harness_source)
    harness = directory / "harness"
    shutil.copytree(harness_source / "tests/performance", harness)
    environment = dict(os.environ, GOWORK="off", GOTOOLCHAIN="local", GOMAXPROCS="4")
    commands = []

    def run(name, *command, env=None):
        commands.append({"name": name, "command": command, "dimensions": env or {}})
        config = dict(environment, **(env or {}))
        with (directory / (name + ".txt")).open("x", encoding="utf-8") as output:
            result = subprocess.run(command, cwd=harness, env=config, text=True,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=3600)
        (directory / "commands.json").write_text(json.dumps(commands, indent=2), encoding="utf-8")
        if result.returncode:
            raise RuntimeError(name + " failed; raw log: " + str(directory / (name + ".txt")))

    run("configure", "go", "mod", "edit", "-replace=github.com/Leeminjing/go-forest=" + str(core))
    run("tidy", "go", "mod", "tidy")
    run("modules", "go", "list", "-m", "-json", "all")
    payload = (directory / "modules.txt").read_text(encoding="utf-8")
    decoder = json.JSONDecoder()
    selected = None
    while payload.strip():
        payload = payload.lstrip()
        record, length = decoder.raw_decode(payload)
        payload = payload[length:]
        if record["Path"] == "github.com/Leeminjing/go-forest":
            selected = record
    if selected is None or Path(selected.get("Replace", {}).get("Dir", "")).resolve() != core.resolve():
        raise RuntimeError("measurement did not select the intended Core source")
    run("go-version", "go", "version")
    run("vet", "go", "vet", "./...")
    run("fixture-tests", "go", "test", "./...", "-count=1")
    binary = directory / ("perf.exe" if os.name == "nt" else "perf")
    run("build-info", "go", "build", "-pgo=off", "-o", str(binary), "./cmd/perf")
    run("binary-info", "go", "version", "-m", str(binary))
    (directory / "runner.py").write_bytes(Path(__file__).read_bytes())
    metadata = {"runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "core": core_sha, "harness": harness_sha, "arguments": vars(args),
                "core_files": manifest(core, ["forest/*.go", "go.mod", "go.sum"]),
                "harness_files": manifest(harness, ["*.go", "cmd/**/*.go", "go.mod", "go.sum"]),
                "platform": platform.platform(), "machine": platform.machine(), "cpu": platform.processor(),
                "cpu_count": os.cpu_count(), "environment": {k: environment.get(k) for k in ["GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOWORK", "GOTOOLCHAIN"]}}
    for path in ["/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max", "/proc/cpuinfo"]:
        if Path(path).exists():
            metadata[path] = Path(path).read_text()
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    cases = [("small", 32, 16, 1, False, 1, "BenchmarkCalls", "1", 1)]
    if args.matrix == "study":
        cases += [("large", 10004, 10001, 1, False, 1, "BenchmarkCalls", "1", 1)]
        cases += [(f"unrelated-{e}", 10004, e, 1, False, 1, "BenchmarkCalls/RefPinned$", "1", 1) for e in [1, 32, 256, 2048]]
        cases += [(f"nodes-{n}", n, 16, 1, False, 1, "BenchmarkCalls/RefPinned$", "1", 1) for n in [256, 2048, 20004]]
        cases += [(f"many-{d}", 1024, 512, d, True, 1, "BenchmarkCalls/RefPinned$", "1", 1) for d in [1, 4, 16, 64, 256]]
        cases += [("optional", 32, 0, 0, False, 1, "BenchmarkCalls/RefPinned$", "1", 1)]
        cases += [(f"ports-{p}", 1024, 512, 1, False, p, "BenchmarkCalls/RefPinned$", "1", 1) for p in [16, 256]]
        cases += [(f"parallel-{w}", 2048, 1024, 1, False, 1, "BenchmarkCalls/RefParallel$", "1,2,4,8", w) for w in [1, 4]]
    for name, n, edges, degree, many, ports, regex, cpus, parallel in cases:
        dims = {"FOREST_PERF_N": str(n), "FOREST_PERF_E": str(edges), "FOREST_PERF_D": str(degree),
                "FOREST_PERF_MANY": str(int(many)), "FOREST_PERF_PORTS": str(ports), "FOREST_PERF_PARALLEL": str(parallel)}
        print(args.label + ": " + name, flush=True)
        run(name, "go", "test", "-pgo=off", "-run=^$", "-bench=^" + regex, "-benchmem",
            "-benchtime=" + args.benchtime, "-count=" + str(args.count), "-cpu=" + cpus, env=dims)
    if args.profiles:
        for kind in ["cpu", "heap", "allocs", "block", "mutex"]:
            run("profile-" + kind, str(binary), "-mode=profile", "-n=10004", "-e=10001",
                "-workers=4", "-duration=2s", "-kind=" + kind, "-enter=" + str(kind in ["block", "mutex"]).lower(), "-out=" + str(directory / (kind + ".pprof")))
            run("profile-top-" + kind, "go", "tool", "pprof", "-top", str(binary), str(directory / (kind + ".pprof")))
    (directory / "COMPLETE").write_text("all commands succeeded\n", encoding="utf-8")
    print("saved immutable run: " + str(directory), flush=True)


if __name__ == "__main__":
    main()
