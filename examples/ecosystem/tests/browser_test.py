"""Real multi-framework app, real databases/broker and Chromium runtime panel."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import urllib.request

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT.parents[1] / ".scratch" / "ecosystem"


def main():
    assert os.environ.get("FOREST_POSTGRES_DSN") and os.environ.get("FOREST_NATS_URL"), "real services required"
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        supplied = os.environ.get("FOREST_ECOSYSTEM_BINARY")
        binary = Path(supplied) if supplied else Path(temporary) / ("ecosystem.exe" if os.name == "nt" else "ecosystem")
        if not supplied:
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/demo"], cwd=ROOT, check=True)
        credential = secrets.token_urlsafe(24)
        env = dict(os.environ, FOREST_TOKEN=credential)
        namespace = "Browser_" + secrets.token_hex(8)
        arguments = ["-namespace", namespace, "-checkpoints", str(Path(temporary) / "checkpoints")]
        subprocess.run([str(binary), "init", *arguments], cwd=ROOT, env=env, check=True)
        process = subprocess.Popen([str(binary), "serve", *arguments], cwd=ROOT, env=env, stdout=subprocess.PIPE, text=True, encoding="utf-8")
        try:
            ready = json.loads(process.stdout.readline())

            def api(path, data=None):
                payload = None if data is None else json.dumps(data).encode()
                request = urllib.request.Request(ready["http"] + path, data=payload, headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=5) as response:
                    return json.load(response)

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(viewport={"width": 1440, "height": 1000})
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)))
                page.goto(ready["observatory"])
                page.wait_for_load_state("networkidle")
                expect(page.locator("#runtime-title")).to_have_text("运行观察")
                page.locator("#token").fill(credential)
                page.locator('#connect-form button[type="submit"]').click()
                expect(page.locator("#runtime-operations tr")).to_have_count(6)
                api("/tasks", {"id": "browser", "input": "compute 2 + 3"})
                deadline = time.monotonic() + 10
                while api("/tasks/browser")["state"] != "completed":
                    assert time.monotonic() < deadline, "agent did not complete"
                    time.sleep(0.05)
                submit = page.locator("#runtime-operations tr").filter(has=page.get_by_text("http.submit", exact=True))
                expect(submit.locator("td").nth(1)).to_have_text("1")
                duplicate = api("/tasks", {"id": "browser", "input": "compute 2 + 3"})
                assert duplicate["state"] == "completed" and duplicate["result"] == "model v1: 5", duplicate
                expect(page.locator("#runtime-coverage")).to_contain_text("explicit boundaries only")
                page.locator("#runtime-title").scroll_into_view_if_needed()
                page.screenshot(path=str(OUTPUT / "runtime-desktop.png"), full_page=True)
                page.locator("section").filter(has=page.locator("#runtime-title")).screenshot(path=str(OUTPUT / "runtime-panel.png"))
                page.set_viewport_size({"width": 390, "height": 844})
                page.screenshot(path=str(OUTPUT / "runtime-mobile.png"), full_page=True)
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), "horizontal page overflow"
                page.locator("#disconnect").click()
                expect(page.locator("#runtime-operations tr")).to_have_count(0)
                expect(page.locator("#runtime-coverage")).to_contain_text("已过期")
                assert not errors, errors
                browser.close()
            print("PASS: real six-framework app, completed Agent task, runtime counts, disconnect, desktop/mobile Chromium", flush=True)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    main()
