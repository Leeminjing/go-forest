"""Run all new modules against isolated real PostgreSQL and JetStream servers.

Requires Go 1.26 and Docker. --race is intended for Linux with a C compiler.
No service/configuration is borrowed from the user's projects.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def run(*args, capture=False, **kw):
    return subprocess.run(args, check=True, text=True, encoding="utf-8", stdout=subprocess.PIPE if capture else None, **kw).stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    suffix = secrets.token_hex(5)
    pg, nats = "forest-test-pg-" + suffix, "forest-test-nats-" + suffix
    started = []
    try:
        def available_port():
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                return str(listener.getsockname()[1])
        pg_port, nats_port = available_port(), available_port()
        # Pin the chosen ports. Docker's random host-port assignment can change
        # when a container restarts, which would test a stale URL, not reconnect.
        # Test-only credentials for an isolated, loopback-only database.
        run("docker", "run", "-d", "--name", pg, "-e", "POSTGRES_PASSWORD=forest", "-e", "POSTGRES_DB=forest", "-p", "127.0.0.1:" + pg_port + ":5432", "postgres:18", capture=True)
        started.append(pg)
        run("docker", "run", "-d", "--name", nats, "-p", "127.0.0.1:" + nats_port + ":4222", "nats:2.12.4", "-js", capture=True)
        started.append(nats)

        def port(name, internal):
            data = json.loads(run("docker", "inspect", name, capture=True))[0]
            return data["NetworkSettings"]["Ports"][internal + "/tcp"][0]["HostPort"]

        deadline = time.monotonic() + 45
        while subprocess.run(["docker", "exec", pg, "pg_isready", "-U", "postgres"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError("PostgreSQL readiness timeout")
            time.sleep(0.2)
        env = dict(os.environ, GOWORK="off", GIN_MODE="release", FOREST_INTEGRATION="1",
                   FOREST_POSTGRES_DSN="host=127.0.0.1 port=" + port(pg, "5432") + " user=postgres password=forest dbname=forest sslmode=disable",
                   FOREST_NATS_URL="nats://127.0.0.1:" + port(nats, "4222"),
                   FOREST_NATS_RESTART_CONTAINER=nats, FOREST_POSTGRES_RESTART_CONTAINER=pg)
        for module in ["adapters/gorm", "adapters/nats", "extensions/telemetry", "extensions/observatory", "examples/ecosystem"]:
            print("CHECK " + module, flush=True)
            run("go", "vet", "./...", cwd=ROOT / module, env=env)
            run("go", "test", *( ["-race"] if args.race else [] ), "./...", "-count=" + str(args.count), "-timeout=180s", cwd=ROOT / module, env=env)
        print("PASS: real database/message services, process crash recovery, per-module checks", flush=True)
    finally:
        for name in reversed(started):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    main()
