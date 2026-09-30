"""Fake HTTP destination used by the verify suite.

Stdlib-only.  Behavior is controllable at runtime so the same container can
play the roles of healthy, failing (always 500), and stalling (hang until
timeout or reset) target.

Control surface:
  POST /control/fail    {"mode": "always"|"once"|"off"}
  POST /control/stall   {"mode": "always"|"once"|"off", "seconds": float}
  POST /control/reset                    -- clear receipts/failure, unblock stalls
  GET  /receipts                         -- recorded deliveries (oldest first)
  GET  /state                            -- {"in_flight": N, ...}
  POST /hook                             -- the webhook endpoint
  GET  /healthz
"""
from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .signing import verify

SECRET = os.environ.get("TARGET_SECRET", "")
# Upper bound so a stalled thread can never hang forever, even without reset.
STALL_HARD_LIMIT = 30.0


class State:
    def __init__(self) -> None:
        self.lock = threading.Condition()
        self.receipts: list[dict] = []
        self.fail_mode = "off"        # off | once | always
        self.stall_mode = "off"       # off | once | always
        self.stall_seconds = 10.0
        self.in_flight = 0
        self.generation = 0          # bumped on reset to cancel live stalls

    def reset(self) -> None:
        with self.lock:
            self.receipts.clear()
            self.fail_mode = "off"
            self.stall_mode = "off"
            self.generation += 1
            self.lock.notify_all()

    def take_fail_ticket(self) -> bool:
        with self.lock:
            if self.fail_mode == "always":
                return True
            if self.fail_mode == "once":
                self.fail_mode = "off"
                return True
            return False

    def stall_plan(self) -> tuple[bool, float, int]:
        with self.lock:
            gen = self.generation
            if self.stall_mode == "always":
                return True, self.stall_seconds, gen
            if self.stall_mode == "once":
                self.stall_mode = "off"
                return True, self.stall_seconds, gen
            return False, 0.0, gen

    def wait_stall(self, seconds: float, generation: int) -> bool:
        """Sleep the stall duration.  Returns False if a reset cancelled it."""
        deadline = time.monotonic() + min(seconds, STALL_HARD_LIMIT)
        with self.lock:
            while time.monotonic() < deadline:
                if self.generation != generation:
                    return False
                self.lock.wait(timeout=min(deadline - time.monotonic(), 0.2))
            return self.generation == generation

    def enter(self) -> None:
        with self.lock:
            self.in_flight += 1

    def leave(self) -> None:
        with self.lock:
            self.in_flight -= 1


STATE = State()


class Handler(BaseHTTPRequestHandler):
    server_version = "fake-target/1.0"

    def log_message(self, *args):
        pass

    def _json(self, code: int, payload) -> bool:
        data = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return True
        except (BrokenPipeError, ConnectionResetError):
            # Client (worker) already gave up after its timeout.
            return False

    def do_GET(self):
        if self.path == "/healthz":
            self._json(200, {"status": "ok"})
        elif self.path == "/receipts":
            with STATE.lock:
                receipts = list(STATE.receipts)
            self._json(200, receipts)
        elif self.path == "/state":
            with STATE.lock:
                self._json(200, {
                    "in_flight": STATE.in_flight,
                    "fail_mode": STATE.fail_mode,
                    "stall_mode": STATE.stall_mode,
                    "receipts": len(STATE.receipts),
                })
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/control/reset":
            STATE.reset()
            self._json(200, {"reset": True})
            return
        if self.path in ("/control/fail", "/control/stall"):
            length = int(self.headers.get("Content-Length", "0"))
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "bad json"})
                return
            with STATE.lock:
                if self.path == "/control/fail":
                    STATE.fail_mode = payload.get("mode", "off")
                else:
                    STATE.stall_mode = payload.get("mode", "off")
                    STATE.stall_seconds = float(payload.get("seconds", 10.0))
            self._json(200, {"ok": True})
            return
        if self.path != "/hook":
            self._json(404, {"error": "not found"})
            return

        STATE.enter()
        try:
            self._handle_hook()
        finally:
            STATE.leave()

    def _handle_hook(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        timestamp = self.headers.get("X-Timestamp", "")
        signature = self.headers.get("X-Signature", "")
        event_id = self.headers.get("X-Event-Id", "")
        request_id = self.headers.get("X-Request-Id", "")
        valid_sig = False
        try:
            valid_sig = verify(SECRET, int(timestamp), body, signature)
        except ValueError:
            valid_sig = False

        stall, seconds, gen = STATE.stall_plan()
        if stall:
            STATE.wait_stall(seconds, gen)

        receipt = {
            "event_id": int(event_id) if event_id.isdigit() else event_id,
            "request_id": request_id,
            "timestamp": int(timestamp) if timestamp.lstrip("-").isdigit() else None,
            "signature": signature,
            "signature_valid": valid_sig,
            "body": json.loads(body) if body else None,
            "received_at": time.time(),
        }
        with STATE.lock:
            STATE.receipts.append(receipt)

        if STATE.take_fail_ticket():
            self._json(500, {"status": "injected failure"})
        else:
            self._json(200, {"status": "received"})


def main() -> None:
    port = int(os.environ.get("TARGET_PORT", "8080"))
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"fake target listening on :{port}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
