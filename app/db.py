"""Database pool, schema bootstrap and small helpers."""
from __future__ import annotations

import asyncio

import psycopg
from psycopg_pool import AsyncConnectionPool

from .clock import ensure_virtual_clock

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    target          TEXT        NOT NULL,
    request_id      TEXT        NOT NULL,
    body            BYTEA       NOT NULL,
    status          TEXT        NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    not_before       BIGINT      NOT NULL DEFAULT 0,
    created_at      BIGINT      NOT NULL,
    updated_at      BIGINT      NOT NULL,
    redelivery_count INTEGER    NOT NULL DEFAULT 0,
    UNIQUE (target, request_id)
);

CREATE INDEX IF NOT EXISTS events_target_dispatch_idx
    ON events (target, id)
    WHERE status <> 'delivered';

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id          BIGSERIAL PRIMARY KEY,
    event_id    BIGINT      NOT NULL REFERENCES events (id),
    attempt_no  INTEGER     NOT NULL,
    started_at  BIGINT,
    finished_at BIGINT,
    outcome     TEXT        NOT NULL,
    status_code INTEGER,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS delivery_attempts_event_idx
    ON delivery_attempts (event_id, attempt_no);

CREATE TABLE IF NOT EXISTS clock_state (
    id     INTEGER PRIMARY KEY DEFAULT 1,
    micros BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS target_control (
    target TEXT PRIMARY KEY,
    paused BOOLEAN NOT NULL DEFAULT FALSE
);
"""


async def wait_for_database(dsn: str, *, attempts: int = 60, delay: float = 1.0) -> None:
    """Block until Postgres accepts connections (compose start-up races)."""
    for i in range(attempts):
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True):
                return
        except OSError:
            await asyncio.sleep(delay)
    raise RuntimeError("database did not become available")


async def init_db(dsn: str, *, virtual_clock: bool) -> AsyncConnectionPool:
    await wait_for_database(dsn)
    # Autocommit: the worker must persist the in_flight/running markers in
    # their own transaction *before* the HTTP call, so a crash mid-delivery
    # leaves recoverable evidence.
    pool = AsyncConnectionPool(
        dsn, min_size=1, max_size=8, kwargs={"autocommit": True}, open=False
    )
    await pool.open()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        if virtual_clock:
            await ensure_virtual_clock(conn)
    return pool
