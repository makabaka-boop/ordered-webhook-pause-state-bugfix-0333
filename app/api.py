"""Backend event ingress (HTTP API).

Producers POST the *raw* JSON body to ``/events/{target}`` with an
``X-Request-Id`` header.  The bytes are stored verbatim and later signed
verbatim.  Ingress is idempotent per (target, request id).
"""
from __future__ import annotations

import json
from typing import Optional

import psycopg
from fastapi import FastAPI, Header, HTTPException, Request, Response

from .clock import advance_virtual_clock, now_micros
from .config import Settings, get_settings
from .db import init_db

settings: Settings = get_settings()
app = FastAPI(title="event-dispatch")
pool = None  # type: ignore[var-annotated]


@app.on_event("startup")
async def _startup() -> None:
    global pool
    pool = await init_db(settings.database_url, virtual_clock=settings.virtual_clock)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if pool is not None:
        await pool.close()


@app.get("/healthz")
async def healthz() -> dict:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/events/{target}")
async def ingest(
    target: str,
    request: Request,
    response: Response,
    x_request_id: Optional[str] = Header(default=None),
) -> dict:
    if target not in settings.targets:
        raise HTTPException(status_code=404, detail="unknown target")
    if not x_request_id or not x_request_id.strip():
        raise HTTPException(status_code=400, detail="X-Request-Id header is required")
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="request body must be JSON")
    try:
        parsed = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="request body must be valid JSON")
    if not isinstance(parsed, (dict, list)):
        raise HTTPException(status_code=400, detail="JSON body must be an object or array")

    ts = await now_micros_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    """
                    INSERT INTO events
                        (target, request_id, body, status, attempts,
                         not_before, created_at, updated_at)
                    VALUES (%s, %s, %s::bytea, 'pending', 0, 0, %s, %s)
                    RETURNING id, status
                    """,
                    (target, x_request_id, body, ts, ts),
                )
            except psycopg.errors.UniqueViolation:
                # Autocommit: the failed statement's implicit transaction is
                # already rolled back by the server; just read the winner.
                # Idempotent replay: same request id wins, no second event.
                await cur.execute(
                    "SELECT id, status FROM events WHERE target = %s AND request_id = %s",
                    (target, x_request_id),
                )
                row = await cur.fetchone()
                response.status_code = 200
                return {
                    "id": row[0], "target": target,
                    "request_id": x_request_id, "status": row[1],
                    "duplicate": True,
                }
            row = await cur.fetchone()
    return {
        "id": row[0], "target": target, "request_id": x_request_id,
        "status": row[1], "duplicate": False,
    }


async def now_micros_pool() -> int:
    async with pool.connection() as conn:
        return await now_micros(conn, virtual=settings.virtual_clock)


@app.get("/admin/targets/{target}")
async def target_state(target: str) -> dict:
    if target not in settings.targets:
        raise HTTPException(status_code=404, detail="unknown target")
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT paused FROM target_control WHERE target = %s", (target,))
            row = await cur.fetchone()
    return {"target": target, "paused": bool(row and row[0])}


@app.post("/admin/targets/{target}/pause")
async def pause_target(target: str) -> dict:
    if target not in settings.targets:
        raise HTTPException(status_code=404, detail="unknown target")
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO target_control (target, paused) VALUES (%s, TRUE) "
                "ON CONFLICT (target) DO UPDATE SET paused = TRUE", (target,),
            )
    return {"target": target, "paused": True}


@app.post("/admin/targets/{target}/resume")
async def resume_target(target: str) -> dict:
    if target not in settings.targets:
        raise HTTPException(status_code=404, detail="unknown target")
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO target_control (target, paused) VALUES (%s, FALSE) "
                "ON CONFLICT (target) DO UPDATE SET paused = FALSE", (target,),
            )
    return {"target": target, "paused": False}


@app.get("/events")
async def list_events(target: Optional[str] = None) -> list[dict]:
    q = "SELECT id, target, request_id, status, attempts, redelivery_count FROM events"
    args: tuple = ()
    if target is not None:
        q += " WHERE target = %s"
        args = (target,)
    q += " ORDER BY id"
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(q, args)
            rows = await cur.fetchall()
    return [
        {"id": r[0], "target": r[1], "request_id": r[2], "status": r[3],
         "attempts": r[4], "redelivery_count": r[5]}
        for r in rows
    ]


def _attempt_row(r) -> dict:
    return {
        "id": r[0], "attempt_no": r[1], "started_at": r[2],
        "finished_at": r[3], "outcome": r[4], "status_code": r[5], "error": r[6],
    }


@app.get("/events/{event_id}")
async def get_event(event_id: int) -> dict:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT id, target, request_id, status, attempts, not_before,
                          created_at, updated_at, redelivery_count,
                          convert_from(body, 'UTF8')
                     FROM events WHERE id = %s""",
                (event_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="event not found")
            await cur.execute(
                """SELECT id, attempt_no, started_at, finished_at, outcome,
                          status_code, error
                     FROM delivery_attempts WHERE event_id = %s
                    ORDER BY attempt_no""",
                (event_id,),
            )
            attempts = [_attempt_row(r) for r in await cur.fetchall()]
    return {
        "id": row[0], "target": row[1], "request_id": row[2], "status": row[3],
        "attempts_used": row[4], "not_before": row[5], "created_at": row[6],
        "updated_at": row[7], "redelivery_count": row[8], "body": row[9],
        "delivery_attempts": attempts,
    }


@app.post("/events/{event_id}/redeliver")
async def redeliver(event_id: int) -> dict:
    """Re-queue a dead-lettered event.

    The event keeps its original id/request id/body; the full attempt history
    is preserved.  A fresh delivery round starts (attempts reset, new attempts
    appended with a continuous ``attempt_no``).
    """
    ts = await now_micros_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE events
                      SET status = 'pending', attempts = 0, not_before = 0,
                          updated_at = %s, redelivery_count = redelivery_count + 1
                    WHERE id = %s AND status = 'dead'
                RETURNING id, target, status, redelivery_count""",
                (ts, event_id),
            )
            row = await cur.fetchone()
            if row is None:
                await cur.execute("SELECT status FROM events WHERE id = %s", (event_id,))
                got = await cur.fetchone()
                if got is None:
                    raise HTTPException(status_code=404, detail="event not found")
                raise HTTPException(
                    status_code=409,
                    detail=f"event is not in dead-letter state (status={got[0]})",
                )
    return {"id": row[0], "target": row[1], "status": row[2],
            "redelivery_count": row[3]}


@app.post("/admin/clock/advance")
async def admin_advance(seconds: int = 0, micros: int = 0) -> dict:
    """Advance the virtual clock (only meaningful in virtual clock mode)."""
    if not settings.virtual_clock:
        raise HTTPException(status_code=400, detail="real clock mode is in use")
    delta = seconds * 1_000_000 + micros
    if delta <= 0:
        raise HTTPException(status_code=400, detail="delta must be positive")
    async with pool.connection() as conn:
        current = await advance_virtual_clock(conn, delta)
    return {"now_micros": current}
