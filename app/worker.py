"""Standalone delivery worker.

Ordering guarantees
-------------------
* For each configured target one independent asyncio task owns dispatch.  It
  only ever touches the oldest not-terminal event (``min(id)`` among
  pending/in_flight/retrying): while event N is in flight or sleeping in
  retry, event N+1 is never sent.
* Different targets are different tasks -- a slow or failing destination can
  never block another one.

Delivery semantics
------------------
At-least-once.  A crash while a request is in flight leaves the event marked
``in_flight`` and its attempt row marked ``running``; on restart the
interrupted attempt is closed and the event is sent again.  Duplicate
deliveries across a crash are therefore possible, but no event is ever
dropped -- and nothing here pretends to be "exactly once".
"""
from __future__ import annotations

import asyncio
import contextlib
import http.server
import logging
import os
import signal
import threading
from typing import Optional

import httpx
import psycopg

from .clock import now_micros
from .config import Settings, Target, get_settings
from .db import init_db
from .signing import sign

log = logging.getLogger("worker")

# Head event of a target = oldest event that has never been delivered.
# Delivered rows are skipped; a dead-lettered head is NOT skipped -- it parks
# the partition until an operator redrives it, so order of (re)delivery stays
# exactly the order of ingestion.
HEAD_SQL = """
SELECT id, status, attempts, not_before, body
  FROM events
 WHERE target = %s
   AND status <> 'delivered'
 ORDER BY id
 LIMIT 1
"""


def _dead_after(settings: Settings) -> int:
    """Attempts allowed per delivery round (each redrive gets a fresh round)."""
    return settings.max_attempts


async def deliver_once(
    conn, *, settings: Settings, event_id: int, target: Target, body: bytes,
    attempt_no: int, round_attempt: int, started_at: int,
) -> tuple[str, int]:
    """Perform one HTTP attempt and record its result.

    ``attempt_no``    -- continuous number across the event's whole life
                         (redrive appends instead of overwriting history).
    ``round_attempt`` -- 1-based number within the current delivery round.
    """
    timestamp_seconds = started_at // 1_000_000
    signature = sign(target.secret, timestamp_seconds, body)

    async with conn.cursor() as cur:
        await cur.execute("SELECT request_id FROM events WHERE id = %s", (event_id,))
        request_id = (await cur.fetchone())[0]

    headers = {
        "Content-Type": "application/json",
        "X-Event-Id": str(event_id),
        "X-Request-Id": request_id,
        "X-Timestamp": str(timestamp_seconds),
        "X-Signature": signature,
    }

    async with conn.cursor() as cur:
        await cur.execute(
            """INSERT INTO delivery_attempts
                   (event_id, attempt_no, started_at, outcome)
               VALUES (%s, %s, %s, 'running')""",
            (event_id, attempt_no, started_at),
        )

    status_code: Optional[int] = None
    error: Optional[str] = None
    try:
        async with httpx.AsyncClient(timeout=settings.http_timeout_seconds) as client:
            resp = await client.post(target.url, content=body, headers=headers)
        status_code = resp.status_code
        outcome = "delivered" if 200 <= resp.status_code < 300 else "failed"
        if outcome == "failed":
            error = f"HTTP {resp.status_code}"
    except httpx.TimeoutException as exc:
        outcome, error = "failed", f"timeout: {type(exc).__name__}"
    except httpx.HTTPError as exc:
        outcome, error = "failed", f"{type(exc).__name__}: {exc}"

    finished = await now_micros(conn, virtual=settings.virtual_clock)
    async with conn.cursor() as cur:
        await cur.execute(
            """UPDATE delivery_attempts
                  SET finished_at = %s, outcome = %s,
                      status_code = %s, error = %s
                WHERE event_id = %s AND attempt_no = %s""",
            (finished, outcome, status_code, error, event_id, attempt_no),
        )
    log.info("event=%s target=%s attempt=%s -> %s %s",
             event_id, target.name, attempt_no, outcome,
             status_code if status_code is not None else error)
    return outcome, finished


async def finish_delivery(
    conn, *, settings: Settings, event_id: int,
    round_attempt: int, outcome: str, finished: int,
) -> None:
    """Advance the event state machine after an attempt resolved."""
    async with conn.cursor() as cur:
        if outcome == "delivered":
            await cur.execute(
                """UPDATE events
                      SET status = 'delivered', attempts = %s,
                          not_before = 0, updated_at = %s
                    WHERE id = %s""",
                (round_attempt, finished, event_id),
            )
            return

        if round_attempt >= _dead_after(settings):
            await cur.execute(
                """UPDATE events
                      SET status = 'dead', attempts = %s, updated_at = %s
                    WHERE id = %s""",
                (round_attempt, finished, event_id),
            )
            log.warning("event=%s -> dead-letter after %s attempts this round",
                        event_id, round_attempt)
        else:
            # Backoff after round attempt k is retry_delays[k-1] (1s,2s,4s...).
            delay_idx = min(round_attempt - 1, len(settings.retry_delays) - 1)
            not_before = finished + settings.retry_delays[delay_idx] * 1_000_000
            await cur.execute(
                """UPDATE events
                      SET status = 'retrying', attempts = %s,
                          not_before = %s, updated_at = %s
                    WHERE id = %s""",
                (round_attempt, not_before, finished, event_id),
            )


async def recover_in_flight(conn, *, settings: Settings, now: int) -> int:
    """Re-arm everything left over from a crashed worker.

    * attempt rows still ``running`` are closed as ``interrupted``;
    * events still ``in_flight`` (with or without an attempt row -- the crash
      could have happened between the two writes) are re-armed immediately.

    A repeated delivery of the same event is expected after recovery: this is
    at-least-once, not exactly-once.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """UPDATE delivery_attempts
                  SET finished_at = %s, outcome = 'interrupted',
                      error = COALESCE(error, 'worker restarted mid-delivery')
                WHERE outcome = 'running'
            RETURNING event_id""",
            (now,),
        )
        recovered = {r[0] for r in await cur.fetchall()}

        await cur.execute(
            """SELECT id, attempts FROM events
                WHERE status = 'in_flight'""",
        )
        in_flight = await cur.fetchall()

    for event_id, attempts in in_flight:
        recovered.add(event_id)
        async with conn.cursor() as cur:
            if attempts >= _dead_after(settings):
                await cur.execute(
                    "UPDATE events SET status = 'dead', updated_at = %s "
                    "WHERE id = %s",
                    (now, event_id),
                )
            else:
                # Re-arm immediately (not_before = 0); it will be re-sent.
                await cur.execute(
                    """UPDATE events
                          SET status = 'retrying', not_before = 0,
                              updated_at = %s
                        WHERE id = %s""",
                    (now, event_id),
                )
    if recovered:
        log.info("recovered %d in-flight event(s) from a crashed worker",
                 len(recovered))
    return len(recovered)


async def run_target(pool, settings: Settings, target: Target) -> None:
    """Drain loop for one destination -- independent of all other targets."""
    while True:
        try:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT paused FROM target_control WHERE target = %s", (target.name,))
                    control = await cur.fetchone()
                if control and control[0]:
                    await asyncio.sleep(settings.poll_interval_seconds)
                    continue
                now = await now_micros(conn, virtual=settings.virtual_clock)
                async with conn.cursor() as cur:
                    await cur.execute(HEAD_SQL, (target.name,))
                    row = await cur.fetchone()

                if row is None:
                    await asyncio.sleep(settings.poll_interval_seconds)
                    continue

                event_id, status, round_attempts, not_before, body = row
                body = bytes(body)

                if status == "dead":
                    # Parked on a dead-letter until it is redelivered out of
                    # band -- never overtake it.
                    await asyncio.sleep(settings.poll_interval_seconds)
                    continue

                if status == "retrying" and now < not_before:
                    # Backoff window.  In virtual mode time is advanced
                    # externally, so a fixed poll is all we need.
                    if not settings.virtual_clock:
                        remaining = (not_before - now) / 1_000_000
                        await asyncio.sleep(
                            min(max(remaining, 0.0),
                                settings.poll_interval_seconds * 5)
                        )
                    else:
                        await asyncio.sleep(settings.poll_interval_seconds)
                    continue

                round_attempt = round_attempts + 1
                # Continuous attempt number across redrives preserves history.
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT COALESCE(MAX(attempt_no), 0) "
                        "FROM delivery_attempts WHERE event_id = %s",
                        (event_id,),
                    )
                    last_attempt_no = (await cur.fetchone())[0]
                attempt_no = last_attempt_no + 1
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE events SET status = 'in_flight', updated_at = %s "
                        "WHERE id = %s",
                        (now, event_id),
                    )
                outcome, finished = await deliver_once(
                    conn, settings=settings, event_id=event_id, target=target,
                    body=body, attempt_no=attempt_no,
                    round_attempt=round_attempt, started_at=now,
                )
                await finish_delivery(
                    conn, settings=settings, event_id=event_id,
                    round_attempt=round_attempt,
                    outcome=outcome, finished=finished,
                )
        except asyncio.CancelledError:
            raise
        except (psycopg.Error, OSError) as exc:
            log.warning("target=%s transient error: %r", target.name, exc)
            await asyncio.sleep(0.5)
        except Exception:
            log.exception("target=%s loop error", target.name)
            await asyncio.sleep(0.5)


class _CrashHandler(http.server.BaseHTTPRequestHandler):
    """Stdlib admin server: liveness probe + hard crash for restart tests."""

    def log_message(self, *args):  # silence default access logging
        pass

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/admin/crash":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"crashing")

            def _die():
                # Hard exit, no graceful drain -- a genuine crash.  Compose
                # restarts the container; in-flight events are recovered.
                log.error("crash requested via /admin/crash -- exiting hard")
                os._exit(1)

            threading.Timer(0.1, _die).start()
        else:
            self.send_response(404)
            self.end_headers()


class AdminServer:
    def __init__(self, port: int = 8081):
        self.port = port
        self.httpd: Optional[http.server.HTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.httpd = http.server.HTTPServer(("0.0.0.0", self.port), _CrashHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        if self.httpd is not None:
            self.httpd.shutdown()


async def main_async(settings: Settings) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    pool = await init_db(settings.database_url, virtual_clock=settings.virtual_clock)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    async with pool.connection() as conn:
        now = await now_micros(conn, virtual=settings.virtual_clock)
        await recover_in_flight(conn, settings=settings, now=now)

    tasks = [
        asyncio.create_task(run_target(pool, settings, t), name=f"target-{t.name}")
        for t in settings.targets.values()
    ]
    admin = AdminServer(int(os.environ.get("WORKER_ADMIN_PORT", "8081")))
    admin.start()
    log.info("worker started with targets: %s",
             ", ".join(settings.targets))
    try:
        await stop.wait()
    finally:
        log.info("worker shutting down")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        admin.stop()
        await pool.close()


def main() -> None:
    asyncio.run(main_async(get_settings()))


if __name__ == "__main__":
    main()
