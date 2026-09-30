"""Deployment-time configuration.

The set of allowed destinations ("targets") is fixed at *deploy time* via the
``WEBHOOK_TARGETS`` environment variable.  Producers never supply a URL: they
only address a configured target by name, so the destination allow-list can
never be widened at runtime.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class Target:
    name: str
    url: str
    secret: str


def _parse_targets(raw: str) -> Dict[str, Target]:
    """Parse ``name=url:secret,other=url:secret`` entries."""
    targets: Dict[str, Target] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" not in chunk or ":" not in chunk.split("=", 1)[1]:
            raise ValueError(f"invalid target entry: {chunk!r} (want name=url:secret)")
        name, rest = chunk.split("=", 1)
        url, secret = rest.rsplit(":", 1)
        name, url, secret = name.strip(), url.strip(), secret
        if not name or not url or not secret:
            raise ValueError(f"invalid target entry: {chunk!r}")
        targets[name] = Target(name=name, url=url, secret=secret)
    if not targets:
        raise ValueError("WEBHOOK_TARGETS must configure at least one target")
    return targets


@dataclass(frozen=True)
class Settings:
    database_url: str
    targets: Dict[str, Target]
    # Retry policy: failures retry after delays[0], delays[1], ... seconds;
    # once attempts reach max_attempts the event goes to the dead-letter.
    retry_delays: tuple[int, ...]
    max_attempts: int
    # "virtual" stores a deterministic, externally-advanceable clock in the
    # database (used by the verify suite); "real" uses the wall clock.
    clock_mode: str
    http_timeout_seconds: float
    poll_interval_seconds: float

    @property
    def virtual_clock(self) -> bool:
        return self.clock_mode == "virtual"


def _retry_delays() -> tuple[int, ...]:
    raw = os.environ.get("WEBHOOK_RETRY_DELAYS", "1,2,4").strip()
    return tuple(int(part) for part in raw.split(",") if part)


def get_settings() -> Settings:
    return Settings(
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql://dispatch:dispatch@db:5432/dispatch"
        ),
        targets=_parse_targets(os.environ.get("WEBHOOK_TARGETS", "")),
        retry_delays=_retry_delays(),
        max_attempts=int(os.environ.get("WEBHOOK_MAX_ATTEMPTS", "4")),
        clock_mode=os.environ.get("WEBHOOK_CLOCK_MODE", "real"),
        http_timeout_seconds=float(os.environ.get("WEBHOOK_HTTP_TIMEOUT", "2")),
        poll_interval_seconds=float(os.environ.get("WEBHOOK_POLL_INTERVAL", "0.03")),
    )
