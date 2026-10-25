"""Own real Temporal/PostgreSQL services and verify both independent modules."""
import argparse
import importlib.util
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


def run(*args, **kw):
    return subprocess.run(args, check=True, text=True, encoding="utf-8", **kw)


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return str(listener.getsockname()[1])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--published", action="store_true")
    args = parser.parse_args()
    suffix = secrets.token_hex(6)
    directory = ROOT / ".scratch" / "temporal" / ("acceptance-" + suffix)
    directory.mkdir(parents=True)
    binary = service.binary()
    temporal_port, pg_port = port(), port()
    while pg_port == temporal_port:
        pg_port = port()
    container = "forest-temporal-" + suffix
    server = None
    pg_started = False
    with (directory / "server.log").open("wb") as log:
        try:
            run("docker", "run", "-d", "--name", container, "-e", "POSTGRES_PASSWORD=forest", "-e", "POSTGRES_DB=forest", "-p", "127.0.0.1:" + pg_port + ":5432", "postgres:18", stdout=subprocess.DEVNULL)
            pg_started = True
            address = "127.0.0.1:" + temporal_port
            server = subprocess.Popen([str(binary), "server", "start-dev", "--ip", "127.0.0.1", "--port", temporal_port, "--headless", "--db-filename", str(directory / "server.sqlite")], stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            deadline = time.monotonic() + 40
            while True:
                if server.poll() is not None:
                    raise RuntimeError("owned Temporal exited; see " + str(directory / "server.log"))
                healthy = subprocess.run([str(binary), "operator", "cluster", "health", "--address", address], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0
                pg_ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
                if healthy and pg_ready:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("real service readiness timeout")
                time.sleep(0.2)
            env = dict(os.environ, GOWORK="off", FOREST_INTEGRATION="1", FOREST_TEMPORAL_ADDRESS=address,
                       FOREST_TEMPORAL_BINARY=str(binary), FOREST_POSTGRES_DSN="host=127.0.0.1 port=" + pg_port + " user=postgres password=forest dbname=forest sslmode=disable")
            for path in ["adapters/temporal", "examples/temporal"]:
                print("CHECK " + path, flush=True)
                run("go", "vet", "./...", cwd=ROOT / path, env=env)
                run("go", "test", *(["-race"] if args.race else []), "./...", "-count=" + str(args.count), "-timeout=300s", cwd=ROOT / path, env=env)
            run("go", "run", "./cmd/demo", "-setup", "-address", address, "-queue", "cli-" + suffix, "-id", "cli-" + suffix, "-directory", str(directory / "agent"), cwd=ROOT / "examples/temporal", env=env)
            if args.published:
                run(sys.executable, str(ROOT / "scripts/verify-temporal-release.py"), env=env)
            print("PASS: real Temporal/SQL, worker crash, service restart and CLI", flush=True)
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
