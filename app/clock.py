"""Clock abstraction.

* real mode  -- the wall clock; sleeping is done with asyncio.sleep.
* virtual mode -- the "current time" is a single monotonically advancing
  microsecond counter stored in Postgres (``clock_state``).  Time only moves
  when an operator/endpoint advances it, which makes retry backoff
  deterministic and instant in tests.  Nothing ever "sleeps": the worker loops
  on short polling intervals until virtual time has advanced past the due
  instant.

All timestamps produced by the system are integer microseconds since the Unix
epoch; HTTP signatures send whole seconds (floor of micro-time).
"""
from __future__ import annotations

import time

from psycopg import AsyncConnection

EPOCH_SECONDS_2025_01_01 = 1735689600
INITIAL_VIRTUAL_MICROS = EPOCH_SECONDS_2025_01_01 * 1_000_000


async def now_micros(conn: AsyncConnection, *, virtual: bool) -> int:
    if not virtual:
        return time.time_ns() // 1_000
    async with conn.cursor() as cur:
        await cur.execute("SELECT micros FROM clock_state WHERE id = 1")
        row = await cur.fetchone()
        return int(row[0])


async def ensure_virtual_clock(conn: AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO clock_state (id, micros)
            VALUES (1, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (INITIAL_VIRTUAL_MICROS,),
        )


async def advance_virtual_clock(conn: AsyncConnection, delta_micros: int) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE clock_state SET micros = micros + %s WHERE id = 1 RETURNING micros",
            (delta_micros,),
        )
        row = await cur.fetchone()
        return int(row[0])
