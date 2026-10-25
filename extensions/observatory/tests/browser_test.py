"""Real Chromium + real demo process, including stdin-controlled replacement.

Run from any directory: python tests/browser_test.py --output <screenshot-dir>
Requires Python Playwright and its Chromium browser. No token is printed.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import queue
import secrets
import select
import socket
import socketserver
import subprocess
import tempfile
import threading
import urllib.request
import urllib.parse

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT.parents[1] / ".scratch" / "v0.4")
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        binary = Path(temporary) / ("demo.exe" if os.name == "nt" else "demo")
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/demo"], cwd=ROOT, check=True)
        credential = secrets.token_urlsafe(24)
        environment = dict(os.environ, FOREST_OBSERVATORY_TOKEN=credential)
        with open(Path(temporary) / "stderr.txt", "w", encoding="utf-8") as log:
            process = subprocess.Popen([str(binary), "-address", "127.0.0.1:0", "-app-address", "127.0.0.1:0"], cwd=ROOT, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, encoding="utf-8")
            messages = queue.Queue()
            sockets, sockets_lock = set(), threading.Lock()
            online = threading.Event()
            online.set()
            proxy = None

            def read_messages():
                for line in process.stdout:
                    messages.put(json.loads(line))

            threading.Thread(target=read_messages, daemon=True).start()

            def command(value, event):
                process.stdin.write(value + "\n")
                process.stdin.flush()
                result = messages.get(timeout=10)
                assert result["event"] == event, result
                return result

            def work(path="/work"):
                with urllib.request.urlopen(ready["app_url"] + path, timeout=15) as response:
                    return json.load(response)

            try:
                ready = messages.get(timeout=15)
                assert ready["event"] == "ready", ready
                upstream = urllib.parse.urlparse(ready["observatory_url"])

                class Tunnel(socketserver.BaseRequestHandler):
                    def handle(self):
                        if not online.is_set():
                            return
                        try:
                            with socket.create_connection((upstream.hostname, upstream.port), timeout=3) as backend:
                                self.request.settimeout(3)
                                with sockets_lock:
                                    sockets.update((self.request, backend))
                                try:
                                    while online.is_set():
                                        readable, _, _ = select.select([self.request, backend], [], [], 0.1)
                                        for source in readable:
                                            data = source.recv(65536)
                                            if not data:
                                                return
                                            (backend if source is self.request else self.request).sendall(data)
                                finally:
                                    with sockets_lock:
                                        sockets.discard(self.request)
                                        sockets.discard(backend)
                        except (OSError, ValueError):
                            return

                proxy = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Tunnel)
                proxy.daemon_threads = True
                threading.Thread(target=proxy.serve_forever, daemon=True).start()
                browser_url = "http://127.0.0.1:" + str(proxy.server_address[1])
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(headless=True)
                    context = browser.new_context(viewport={"width": 1440, "height": 1050}, device_scale_factor=1)
                    page = context.new_page()
                    errors = []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.goto(browser_url)
                    page.wait_for_load_state("networkidle")
                    expect(page.locator("#status")).to_have_text("尚未连接")
                    page.locator("#token").fill(credential)
                    page.get_by_role("button", name="连接森林", exact=True).click()
                    expect(page.locator("#status")).to_have_text("已连接")
                    expect(page.locator("#tree-count")).to_have_text("4")
                    page.locator('[data-tree-id="business"]').click()
                    expect(page.locator(".tree-name")).to_have_text("business")
                    expect(page.locator("#detail-content")).to_contain_text("demo.work")
                    expect(page.locator("#detail-content")).to_contain_text("阶段 runtime")
                    expect(page.locator("#detail-content")).to_contain_text("需求契约 demo.work")
                    expect(page.locator(".edge.direct")).to_have_count(1)
                    expect(page.locator(".edge.nest")).to_have_count(1)
                    expect(page.locator(".edge.nest_binding")).to_have_count(1)
                    page.screenshot(path=str(output / "overview.png"), full_page=True)
                    page.locator("#relationship").select_option("link")
                    expect(page.locator(".edge.nest")).to_have_count(0)
                    expect(page.locator(".edge.link")).to_have_count(1)
                    page.locator("#relationship").select_option("all")
                    page.locator('[data-tree-id="application"]').focus()
                    page.keyboard.press("Enter")
                    expect(page.locator(".tree-name")).to_have_text("application")
                    page.locator('[data-tree-id="business"]').click()
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        old = pool.submit(work, "/work?hold=1")
                        command("wait", "blocked")
                        command("replace", "replaced")
                        expect(page.locator("#generation")).to_have_text("2")
                        expect(page.locator("#draining-count")).to_have_text("1")
                        assert work() == {"value": "v2", "again": "v2", "generation": "2"}
                        page.screenshot(path=str(output / "draining.png"), full_page=True)
                        command("release", "released")
                        assert old.result(timeout=10) == {"value": "v1", "again": "v1", "generation": "1"}
                    expect(page.locator("#draining-count")).to_have_text("0")
                    expect(page.locator("#detail-content")).to_contain_text("stopped")
                    history = page.locator("#detail-content details")
                    summary = history.locator("summary")
                    summary.click()
                    summary.focus()
                    # Ordinary sampling must preserve expansion and keyboard focus.
                    for _ in range(2):
                        sampled = page.locator("#sample-time").inner_text()
                        page.wait_for_function("previous => document.querySelector('#sample-time').textContent !== previous", arg=sampled)
                    expect(history).to_have_attribute("open", "")
                    expect(summary).to_be_focused()
                    online.clear()
                    with sockets_lock:
                        for connection in list(sockets):
                            try:
                                connection.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            connection.close()
                    expect(page.locator("#status")).to_contain_text("过期", timeout=10000)
                    for _ in range(20):
                        command("replace", "replaced")
                    online.set()
                    expect(page.locator("#status")).to_have_text("已连接", timeout=15000)
                    expect(page.locator("#notice")).to_contain_text("缺口")
                    expect(page.locator("#generation")).to_have_text("22")
                    # Even a changed instance history on resync preserves interaction.
                    expect(history).to_have_attribute("open", "")
                    expect(summary).to_be_focused()
                    summary.click()
                    sequences = page.locator("#events li").evaluate_all("items => items.map(x => x.dataset.sequence)")
                    assert len(sequences) == len(set(sequences))
                    page.set_viewport_size({"width": 390, "height": 844})
                    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                    page.screenshot(path=str(output / "mobile.png"), full_page=True)
                    # UI fixtures exercise legitimate empty and hostile-identifier states;
                    # all replacement, lifecycle, and reconnect tests above use real TCP.
                    fixture = context.new_page()
                    sample = {"schema_version": 1, "boot_id": "0" * 32, "sampled_at": "2026-10-25T00:00:00Z", "cursor": "0" * 32 + ":0", "events": [], "snapshot": {"generation": "0", "observed_event_sequence": "0", "trees": [], "nest_edges": [], "bindings": [], "instances": [], "counts": {"trees": "0", "nest_edges": "0", "links": "0", "nest_bindings": "0", "draining": "0"}}}

                    def fulfill(route):
                        route.fulfill(status=200, content_type="text/event-stream", body="event: sync\ndata: " + json.dumps(sample) + "\n\n")

                    fixture.route("**/api/v1/events", fulfill)
                    fixture.goto(browser_url)
                    fixture.wait_for_load_state("networkidle")
                    fixture.locator("#token").fill(credential)
                    fixture.get_by_role("button", name="连接森林", exact=True).click()
                    expect(fixture.locator("#tree-count")).to_have_text("0")
                    expect(fixture.locator("#graph-empty")).to_have_text("森林中暂时没有树。")
                    expect(fixture.locator("#events")).to_contain_text("暂无观察记录")
                    sample["snapshot"]["instances"] = [{"id": "retired-instance", "tree_id": "removed-tree", "type_id": "removed.type", "version": "1", "state": "draining", "diagnostic": {"code": "drain_timeout", "stage": "stop"}}, {"id": "candidate-instance", "tree_id": "unpublished-tree", "type_id": "candidate.type", "version": "1", "state": "stopped"}]
                    sample["snapshot"]["counts"]["draining"] = "1"
                    expect(fixture.locator("#instances")).to_contain_text("removed-tree", timeout=10000)
                    expect(fixture.locator("#instances")).to_contain_text("drain_timeout")
                    expect(fixture.locator("#instances")).not_to_contain_text("unpublished-tree")
                    fixture.locator("#show-stopped").check()
                    expect(fixture.locator("#instances")).to_contain_text("unpublished-tree")
                    hostile = '<img src=x onerror="window.__xss=1">'
                    sample["snapshot"]["trees"] = [{"id": hostile, "parent_id": "", "instance_id": "i", "type_id": "unsafe-name", "version": "1", "sampled_state": "active", "ports": [], "capabilities": []}]
                    sample["snapshot"]["counts"]["trees"] = "1"
                    expect(fixture.locator("#tree-count")).to_have_text("1", timeout=10000)
                    fixture.locator(".node").click()
                    expect(fixture.locator(".tree-name")).to_have_text(hostile)
                    assert fixture.locator("#detail-content img").count() == 0
                    assert fixture.evaluate("window.__xss") is None
                    fixture.close()
                    assert not errors, errors
                    # Quit with the main page's real SSE connection still open.
                    process.stdin.write("quit\n")
                    process.stdin.flush()
                    assert process.wait(timeout=10) == 0
                    browser.close()
                print("PASS: topology, details, keyboard, pinned replacement, sampled Stop, reconnect gap, mobile, empty state, safe identifiers, Close with SSE")
                print("Screenshots: " + str(output))
            finally:
                if proxy is not None:
                    proxy.shutdown()
                    proxy.server_close()
                if process.poll() is None:
                    try:
                        process.stdin.write("quit\n")
                        process.stdin.flush()
                        process.wait(timeout=5)
                    except (OSError, subprocess.TimeoutExpired):
                        process.kill()
                        process.wait()


if __name__ == "__main__":
    main()
