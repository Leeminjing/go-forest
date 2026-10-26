"""Own a real Temporal server and execute native Eino composition acceptance."""
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--published", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--directory")
    args = parser.parse_args()
    if args.count < 1:
        raise ValueError("count must be positive")
    suffix = secrets.token_hex(6)
    directory = Path(args.directory).resolve() if args.directory else ROOT / ".scratch/temporal-eino" / ("acceptance-" + suffix)
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "stop").exists():
        raise RuntimeError("existing stop marker: use a fresh owned directory")
    binary = service.binary()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = str(listener.getsockname()[1])
    address = "127.0.0.1:" + port
    with (directory / "server.log").open("wb") as log:
        server = subprocess.Popen([str(binary), "server", "start-dev", "--ip", "127.0.0.1", "--port", port,
                                   "--headless", "--db-filename", str(directory / "server.sqlite")],
                                  stdout=log, stderr=log,
                                  creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            deadline = time.monotonic() + 45
            while True:
                if server.poll() is not None:
                    raise RuntimeError("owned server exited; inspect " + str(directory / "server.log"))
                ready = subprocess.run([str(binary), "operator", "cluster", "health", "--address", address],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0
                if ready:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("real server readiness timeout")
                time.sleep(0.2)
            config = dict(GOWORK="off", FOREST_INTEGRATION="1", FOREST_TEMPORAL_ADDRESS=address)
            (directory / "environment.json").write_text(json.dumps(config), encoding="utf-8")
            print("READY: owned real Temporal; " + str(directory / "environment.json"), flush=True)
            if args.serve:
                while not (directory / "stop").exists():
                    if server.poll() is not None:
                        raise RuntimeError("owned server exited")
                    time.sleep(0.2)
                return
            env = dict(os.environ, **config)
            module = ROOT / "examples/temporal-eino"
            run("go", "vet", "./...", cwd=module, env=env)
            run("go", "test", *(["-race"] if args.race else []), "./...", "-count=" + str(args.count), "-timeout=600s", cwd=module, env=env)
            run("go", "run", "./cmd/demo", "-queue", "cli-" + suffix, "-id", "cli-" + suffix,
                "-directory", str(directory / "native"), cwd=module, env=env)
            if args.published:
                run(sys.executable, str(ROOT / "scripts/verify-temporal-eino-release.py"), env=env)
            print("PASS: native Temporal/Eino, signal, scope, physical drain, process recovery and CLI", flush=True)
        finally:
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == "__main__":
    main()
