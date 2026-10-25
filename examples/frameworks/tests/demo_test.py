"""Real Go server and RPC client processes plus Chromium observation."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import queue
import secrets
import subprocess
import tempfile
import threading
import urllib.request

from playwright.sync_api import expect, sync_playwright

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=root.parents[1] / ".scratch" / "v0.5")
output = parser.parse_args().output.resolve()
output.mkdir(parents=True, exist_ok=True)

with tempfile.TemporaryDirectory() as temporary:
    suffix = ".exe" if os.name == "nt" else ""
    server_binary, client_binary = Path(temporary) / ("server" + suffix), Path(temporary) / ("client" + suffix)
    for binary, source in [(server_binary, "./cmd/demo"), (client_binary, "./cmd/client")]:
        subprocess.run(["go", "build", "-o", str(binary), source], cwd=root, check=True)
    token = secrets.token_urlsafe(24)
    server = subprocess.Popen([str(server_binary), "-http-address", "127.0.0.1:0", "-grpc-address", "127.0.0.1:0", "-address", "127.0.0.1:0"], env=dict(os.environ, FOREST_OBSERVATORY_TOKEN=token), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
    messages = queue.Queue()

    def collect():
        for line in server.stdout:
            messages.put(json.loads(line))

    threading.Thread(target=collect, daemon=True).start()
    stream = None
    try:
        ready = messages.get(timeout=15)
        assert ready["event"] == "ready", ready

        def command(value, expected):
            server.stdin.write(value + "\n")
            server.stdin.flush()
            result = messages.get(timeout=10)
            assert result["event"] == expected, result

        def http(path="/work"):
            with urllib.request.urlopen(ready["http_url"] + path, timeout=15) as response:
                return json.load(response)

        stream = subprocess.Popen([str(client_binary), "-target", ready["grpc_target"], "-stream"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
        stream_messages = queue.Queue()

        def collect_stream():
            for line in stream.stdout:
                stream_messages.put(json.loads(line))

        threading.Thread(target=collect_stream, daemon=True).start()
        assert stream_messages.get(timeout=10) == {"value": "v1", "again": "v1", "generation": "1"}
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1050})
            page.goto(ready["observatory_url"])
            page.wait_for_load_state("networkidle")
            page.locator("#token").fill(token)
            page.get_by_role("button", name="连接森林", exact=True).click()
            expect(page.locator("#tree-count")).to_have_text("5")
            expect(page.locator("#link-count")).to_have_text("2")
            with concurrent.futures.ThreadPoolExecutor() as pool:
                old_http = pool.submit(http, "/work?hold=1")
                command("wait", "blocked")
                command("replace", "replaced")
                expect(page.locator("#generation")).to_have_text("2")
                expect(page.locator("#draining-count")).to_have_text("1")
                assert http() == {"value": "v2", "again": "v2", "generation": "2"}
                rpc = subprocess.run([str(client_binary), "-target", ready["grpc_target"]], check=True, capture_output=True, text=True, encoding="utf-8", timeout=10)
                assert json.loads(rpc.stdout) == {"value": "v2", "again": "v2", "generation": "2"}
                page.locator('[data-tree-id="grpc"]').click()
                expect(page.locator("#detail-content")).to_contain_text("frameworks.work")
                page.screenshot(path=str(output / "frameworks.png"), full_page=True)
                command("release", "released")
                assert old_http.result(timeout=10) == {"value": "v1", "again": "v1", "generation": "1"}
            # The RPC remains pinned after the old HTTP request completes.
            expect(page.locator("#draining-count")).to_have_text("1")
            stream.stdin.write("\n")
            stream.stdin.flush()
            assert stream_messages.get(timeout=10) == {"value": "v1", "again": "v1", "generation": "1"}
            assert stream.wait(timeout=10) == 0
            expect(page.locator("#draining-count")).to_have_text("0")
            page.locator("#show-stopped").check()
            expect(page.locator("#instances")).to_contain_text("stopped")
            server.stdin.write("quit\n")
            server.stdin.flush()
            assert server.wait(timeout=10) == 0
            browser.close()
        print("PASS: real HTTP + RPC processes, shared provider, old stream generation, observable drain/stop, Close with SSE")
        print("Screenshot: " + str(output / "frameworks.png"))
    finally:
        if stream is not None and stream.poll() is None:
            stream.kill()
            stream.wait()
        if server.poll() is None:
            try:
                server.stdin.write("quit\n")
                server.stdin.flush()
                server.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                server.kill()
                server.wait()
