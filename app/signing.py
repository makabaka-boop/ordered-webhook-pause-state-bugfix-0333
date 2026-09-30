"""Per-target HMAC signatures over timestamp and the *original* JSON bytes.

Signed string (no separator ambiguity -- timestamp is fixed width digits)::

    f"{timestamp}." + raw_body_bytes

Header ``X-Signature`` carries hex(HMAC-SHA256).  Because the body bytes are
the exact payload accepted at ingress (stored as BYTEA), a re-serialized body
would not verify.
"""
from __future__ import annotations

import hashlib
import hmac


def build_signing_string(timestamp: int, raw_body: bytes) -> bytes:
    return str(timestamp).encode("ascii") + b"." + raw_body


def sign(secret: str, timestamp: int, raw_body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), build_signing_string(timestamp, raw_body),
                   hashlib.sha256)
    return mac.hexdigest()


def verify(secret: str, timestamp: int, raw_body: bytes, signature: str) -> bool:
    expected = sign(secret, timestamp, raw_body)
    return hmac.compare_digest(expected, signature or "")
