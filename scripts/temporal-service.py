"""Run an owned persistent real Temporal development server, stopped by a marker.

Usage: python scripts/temporal-service.py --directory .scratch/temporal/service --port 17233
Create DIRECTORY/stop to request shutdown; logs/state stay in this ignored directory.
The fixed CLI is downloaded from official release assets and checksum verified.
"""
import argparse
import hashlib
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tarfile
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
HASHES = {
    "windows_amd64.zip": "babb65844835045c91fb98930fe10b81de28db80c8c609180473d2c0b18c7589",
    "linux_amd64.tar.gz": "09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5",
}


def binary():
    platform = "windows_amd64.zip" if os.name == "nt" else "linux_amd64.tar.gz"
    name = "temporal_cli_1.9.1_" + platform
    directory = ROOT / ".scratch" / "temporal" / "cli" / platform.split(".")[0]
    directory.mkdir(parents=True, exist_ok=True)
    archive = directory / name
    if not archive.exists():
        temporary = directory / (name + ".download")
        with urllib.request.urlopen("https://github.com/temporalio/cli/releases/download/v1.9.1/" + name, timeout=60) as source, temporary.open("wb") as target:
            shutil.copyfileobj(source, target)
        if hashlib.sha256(temporary.read_bytes()).hexdigest() != HASHES[platform]:
            raise RuntimeError("Temporal CLI download checksum mismatch")
        temporary.replace(archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != HASHES[platform]:
        raise RuntimeError("Temporal CLI checksum mismatch")
    executable = directory / ("temporal.exe" if os.name == "nt" else "temporal")
    if not executable.exists():
        if platform.endswith("zip"):
            with zipfile.ZipFile(archive) as source:
                executable.write_bytes(source.read(executable.name))
        else:
            with tarfile.open(archive) as source:
                executable.write_bytes(source.extractfile("temporal").read())
            executable.chmod(0o700)
    return executable


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "stop").exists():
        raise RuntimeError("remove the old stop marker explicitly before restarting")
    with (directory / "server.log").open("ab") as log:
        process = subprocess.Popen([str(binary()), "server", "start-dev", "--ip", "127.0.0.1", "--port", str(args.port), "--headless", "--db-filename", str(directory / "server.sqlite")], stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        (directory / "pid").write_text(str(process.pid))
        print(f"owned Temporal pid={process.pid} address=127.0.0.1:{args.port}", flush=True)
        try:
            while not (directory / "stop").exists():
                if process.poll() is not None:
                    raise RuntimeError(f"Temporal exited: {process.returncode}; see {directory / 'server.log'}")
                time.sleep(0.2)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
