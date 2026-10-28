from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
PAGE_PATH = ROOT / "index.html"
COLLECTOR_PATH = ROOT / "collector.py"
REFRESH_SECONDS = max(300, int(os.getenv("FUNDING_SCANNER_REFRESH_SECONDS", "1800")))
_snapshot_lock = threading.Lock()
_snapshot: dict | None = None
_scan_state = {"running": False, "last_error": None}


def collect_snapshot() -> dict:
    """Run one public-data scan and validate its JSON response."""
    result = subprocess.run(
        [sys.executable, str(COLLECTOR_PATH)],
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
        cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError("collector process failed")
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise RuntimeError("collector returned an invalid snapshot")
    return payload


def refresh_loop() -> None:
    global _snapshot
    while True:
        with _snapshot_lock:
            _scan_state["running"] = True
            _scan_state["last_error"] = None
        try:
            fresh = collect_snapshot()
            with _snapshot_lock:
                _snapshot = fresh
        except Exception as exc:
            with _snapshot_lock:
                _scan_state["last_error"] = type(exc).__name__
        finally:
            with _snapshot_lock:
                _scan_state["running"] = False
        time.sleep(REFRESH_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        global _snapshot
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                body = PAGE_PATH.read_bytes()
            except OSError:
                self.send_error(500)
                return
            self._respond(200, body, "text/html; charset=utf-8")
            return
        if path == "/health":
            with _snapshot_lock:
                payload = {
                    "ready": _snapshot is not None,
                    "scan_running": _scan_state["running"],
                    "last_error_type": _scan_state["last_error"],
                }
            self._respond(200, json.dumps(payload).encode(), "application/json")
            return
        if path == "/api/funding-history":
            with _snapshot_lock:
                payload = _snapshot
                state = dict(_scan_state)
            if payload is None:
                status = 503
                response = {
                    "error": "No complete public market-data snapshot is available yet.",
                    "scan_running": state["running"],
                    "last_error_type": state["last_error"],
                }
            else:
                status = 200
                response = payload
            self._respond(status, json.dumps(response, ensure_ascii=False).encode(), "application/json")
            return
        self.send_error(404)

    def _respond(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    host = os.getenv("FUNDING_SCANNER_HOST", "127.0.0.1")
    port = int(os.getenv("FUNDING_SCANNER_PORT", "8765"))
    threading.Thread(target=refresh_loop, name="funding-scan-refresh", daemon=True).start()
    print(f"Read-only funding scanner: http://{host}:{port}/")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
