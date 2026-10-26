"""Own isolated real Temporal/PostgreSQL services for cross-adapter acceptance."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.util.spec_from_file_location("temporal_service", ROOT / "scripts/temporal-service.py")
service = importlib.util.module_from_spec(loader)
loader.loader.exec_module(service)


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, encoding="utf-8", **kwargs)


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return str(listener.getsockname()[1])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--published", action="store_true")
    parser.add_argument("--serve", action="store_true", help="keep owned development services until a stop marker")
    parser.add_argument("--directory")
    args = parser.parse_args()
    suffix = secrets.token_hex(6)
    directory = Path(args.directory).resolve() if args.directory else ROOT / ".scratch/cross-adapter" / ("acceptance-" + suffix)
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "stop").exists():
        raise RuntimeError("existing stop marker: use a new owned directory")
    binary = service.binary()
    temporal_port, pg_port = port(), port()
    while temporal_port == pg_port:
        pg_port = port()
    container = "forest-cross-" + suffix
    server, pg_started = None, False
    with (directory / "server.log").open("wb") as log:
        try:
            run("docker", "run", "-d", "--name", container,
                "-e", "POSTGRES_PASSWORD=forest", "-e", "POSTGRES_DB=forest",
                "-p", "127.0.0.1:" + pg_port + ":5432", "postgres:18", stdout=subprocess.DEVNULL)
            pg_started = True
            address = "127.0.0.1:" + temporal_port
            server = subprocess.Popen([str(binary), "server", "start-dev", "--ip", "127.0.0.1", "--port", temporal_port,
                                       "--headless", "--db-filename", str(directory / "server.sqlite")],
                                      stdout=log, stderr=log,
                                      creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            deadline = time.monotonic() + 45
            while True:
                if server.poll() is not None:
                    raise RuntimeError("owned Temporal exited; inspect " + str(directory / "server.log"))
                healthy = subprocess.run([str(binary), "operator", "cluster", "health", "--address", address],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0
                ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
                if healthy and ready:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("real services readiness timeout")
                time.sleep(0.2)
            configuration = dict(GOWORK="off", FOREST_INTEGRATION="1", FOREST_TEMPORAL_ADDRESS=address,
                                 FOREST_TEMPORAL_BINARY=str(binary),
                                 FOREST_POSTGRES_DSN="host=127.0.0.1 port=" + pg_port + " user=postgres password=forest dbname=forest sslmode=disable")
            (directory / "environment.json").write_text(json.dumps(configuration), encoding="utf-8")
            print("READY: owned real Temporal/PostgreSQL; configuration " + str(directory / "environment.json"), flush=True)
            if args.serve:
                while not (directory / "stop").exists():
                    if server.poll() is not None:
                        raise RuntimeError("owned server exited")
                    time.sleep(0.2)
                return
            env = dict(os.environ, **configuration)
            module = ROOT / "examples/cross-adapter"
            run("go", "vet", "./...", cwd=module, env=env)
            run("go", "test", *(["-race"] if args.race else []), "./...", "-count=" + str(args.count), "-timeout=300s", cwd=module, env=env)
            run("go", "run", "./cmd/demo", "-setup", "-queue", "cli-" + suffix, "-id", "cli-" + suffix,
                "-directory", str(directory / "native"), cwd=module, env=env)
            if args.published:
                run(sys.executable, str(ROOT / "scripts/verify-cross-adapter-release.py"), env=env)
            print("PASS: real cross-adapter workflow, database, process recovery and CLI", flush=True)
        finally:
            if server is not None and server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
            if pg_started:
                subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    main()
