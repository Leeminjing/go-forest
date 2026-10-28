"""Run the real Gin/PostgreSQL example, with explicit source or published mode."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "examples/topology-incremental"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--published", action="store_true")
    parser.add_argument("--compile-only", action="store_true", help="explicitly skip the real PostgreSQL case")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    env = dict(os.environ, GOWORK="off", GIN_MODE="release")
    container = None

    def run(*command, capture=False):
        return subprocess.run(command, cwd=MODULE, env=env, check=True, timeout=900,
                              text=True, encoding="utf-8",
                              stdout=subprocess.PIPE if capture else None).stdout

    try:
        if args.compile_only:
            env.pop("FOREST_POSTGRES_DSN", None)
            print("COMPILE ONLY: real PostgreSQL acceptance is explicitly skipped", flush=True)
        if not args.compile_only and not env.get("FOREST_POSTGRES_DSN"):
            container = "forest-topology-" + uuid.uuid4().hex[:12]
            run("docker", "run", "--rm", "--detach", "--name", container,
                "-e", "POSTGRES_PASSWORD=forest-local-test", "-e", "POSTGRES_DB=forest",
                "-p", "127.0.0.1::5432", "postgres:17-alpine")
            port = run("docker", "port", container, "5432/tcp", capture=True).strip().rsplit(":", 1)[1]
            env["FOREST_POSTGRES_DSN"] = f"postgres://postgres:forest-local-test@127.0.0.1:{port}/forest?sslmode=disable"
            for attempt in range(60):
                ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", "forest"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                if ready.returncode == 0:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError("PostgreSQL did not become ready")
        with tempfile.TemporaryDirectory(prefix="forest-topology-source-") as temporary:
            flags = []
            if not args.published:
                modfile = Path(temporary) / "source.mod"
                modfile.write_bytes((MODULE / "go.mod").read_bytes())
                if (MODULE / "go.sum").exists():
                    modfile.with_suffix(".sum").write_bytes((MODULE / "go.sum").read_bytes())
                flags = [f"-modfile={modfile}"]
                run("go", "mod", "edit", *flags,
                    "-replace=github.com/Leeminjing/go-forest/extensions/topology=" + str(ROOT / "extensions/topology"))
                run("go", "mod", "tidy", *flags)
                print("SOURCE acceptance: only topology is replaced with the current checkout", flush=True)
            else:
                payload = run("go", "list", "-m", "-json", "github.com/Leeminjing/go-forest/extensions/topology", capture=True)
                dependency = json.loads(payload)
                if dependency.get("Replace") or dependency.get("Version") != "v0.2.0":
                    raise RuntimeError("published acceptance requires fixed topology v0.2.0 without replace")
                print("PUBLISHED acceptance: topology v0.2.0 without local replacements", flush=True)
            run("go", "vet", *flags, "./...")
            run("go", "test", *flags, *(["-race"] if args.race else []), "./...",
                f"-count={args.count}", "-timeout=180s", "-v")
    finally:
        if container is not None:
            subprocess.run(["docker", "rm", "-f", container], check=False,
                           stdout=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()
