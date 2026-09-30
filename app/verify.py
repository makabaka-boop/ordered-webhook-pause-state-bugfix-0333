"""One-shot acceptance suite (``docker compose run --rm verify``).

Exercises, against the full compose stack:

 1. allow-list: unknown target is rejected
 2. ingress idempotency on X-Request-Id
 3. HMAC signature over timestamp + original JSON bytes
 4. strict per-target ordering (in-order production; head-of-line block)
 5. non-2xx -> 1s/2s/4s backoff -> 4th failure dead-letters (virtual clock)
 6. independent target isolation (one target failing never blocks the other)
 7. timeout counts as a failed attempt and is retried
 8. worker crash/restart: no loss, in-flight event is re-sent (duplicate OK),
    interrupted attempt is kept in history
 9. dead-letter redrive keeps event identity and full attempt history

Exit code is non-zero on the first failed assertion; a summary is printed.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://api:8000")
WORKER_ADMIN = os.environ.get("WORKER_ADMIN_URL", "http://worker:8081")
TARGETS = {
    "target-a": os.environ.get("TARGET_A_URL", "http://target-a:8080"),
    "target-b": os.environ.get("TARGET_B_URL", "http://target-b:8080"),
}
SECRETS = {
    "target-a": os.environ.get("TARGET_A_SECRET", "secret-a"),
    "target-b": os.environ.get("TARGET_B_SECRET", "secret-b"),
}

RESULTS: list[tuple[str, bool, str]] = []


# ---------------------------------------------------------------- HTTP helpers

def http(method: str, url: str, body=None, headers=None, timeout: float = 5.0):
    """Return (status, parsed_json_or_None)."""
    data = None
    hdrs = {"Accept": "application/json"}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json"
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                parsed = raw.decode("utf-8", errors="replace")
            return resp.status, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, None


def post_event(target: str, request_id: str, payload, timeout: float = 5.0):
    raw = json.dumps(payload).encode()
    return http(
        "POST", f"{API}/events/{target}", body=raw,
        headers={"X-Request-Id": request_id}, timeout=timeout,
    )


def get_event(event_id: int):
    return http("GET", f"{API}/events/{event_id}")[1]


def list_events(target: str):
    return http("GET", f"{API}/events?target={target}")[1]


def receipts(name: str):
    return http("GET", f"{TARGETS[name]}/receipts")[1]


def state(name: str):
    return http("GET", f"{TARGETS[name]}/state")[1]


def control(name: str, path: str, payload=None):
    return http("POST", f"{TARGETS[name]}/control/{path}", payload or {})


def reset(name: str):
    control(name, "reset")


def advance(seconds: int):
    status, body = http("POST", f"{API}/admin/clock/advance?seconds={seconds}")
    assert status == 200, f"clock advance failed: {status} {body}"
    return body


def wait_until(desc: str, predicate, timeout: float = 20.0, interval: float = 0.1):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
        except Exception as exc:  # tolerate transient lookup errors
            last = exc
        if last is True:
            return
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {desc} (last: {last!r})")


def check(name: str, fn):
    try:
        fn()
    except Exception as exc:
        RESULTS.append((name, False, f"{type(exc).__name__}: {exc}"))
        print(f"FAIL  {name}: {exc}")
        return False
    RESULTS.append((name, True, ""))
    print(f"PASS  {name}")
    return True


# ---------------------------------------------------------------- signature

def expected_signature(secret: str, timestamp: int, raw_body: bytes) -> str:
    return hmac.new(secret.encode(),
                    str(timestamp).encode() + b"." + raw_body,
                    hashlib.sha256).hexdigest()


# ---------------------------------------------------------------- wait stack

def wait_for_stack() -> None:
    for label, url in [
        ("api", f"{API}/healthz"),
        ("worker", f"{WORKER_ADMIN}/healthz"),
        ("target-a", f"{TARGETS['target-a']}/healthz"),
        ("target-b", f"{TARGETS['target-b']}/healthz"),
    ]:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                status, _ = http("GET", url, timeout=2)
                if status == 200:
                    break
            except OSError:
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError(f"service {label} never became healthy: {url}")
    print("all services healthy")


# ---------------------------------------------------------------- tests

def test_allowlist() -> None:
    status, body = http("POST", f"{API}/events/nope", body=b'{"a":1}',
                        headers={"X-Request-Id": "r-allow-1"})
    assert status == 404, f"unknown target must be rejected, got {status} {body}"


def test_idempotency() -> None:
    rid = "rid-idem-1"
    s1, b1 = post_event("target-b", rid, {"n": 1})
    s2, b2 = post_event("target-b", rid, {"n": 1})
    assert s1 == 200 and b1["duplicate"] is False, b1
    assert s2 == 200 and b2["duplicate"] is True, b2
    assert b1["id"] == b2["id"], "replay must map to the same event"
    # no extra event should exist for this request id
    evs = [e for e in list_events("target-b") if e["request_id"] == rid]
    assert len(evs) == 1, f"expected 1 event, got {len(evs)}"


def test_signature() -> None:
    rid = "rid-sig-1"
    payload = {"hello": "signature", "with": ["nested", "json", 3]}
    status, ev = post_event("target-a", rid, payload)
    assert status == 200, ev
    eid = ev["id"]
    wait_until("sig event delivered",
               lambda: get_event(eid)["status"] == "delivered")
    raw = json.dumps(payload).encode()
    found = [r for r in receipts("target-a") if r["event_id"] == eid]
    assert len(found) == 1, found
    r = found[0]
    assert r["signature_valid"] is True, "target rejected HMAC"
    assert r["signature"] == expected_signature(
        SECRETS["target-a"], r["timestamp"], raw
    ), "signature must cover timestamp + exact original JSON bytes"


def test_ordering() -> None:
    # In-order production: N events must be dispatched in id order.
    ids = []
    for i in range(6):
        status, ev = post_event("target-b", f"rid-order-{i}", {"seq": i})
        assert status == 200
        ids.append(ev["id"])

    def all_delivered():
        return all(get_event(i)["status"] == "delivered" for i in ids)

    wait_until("ordered batch delivered", all_delivered)
    got = [r for r in receipts("target-b") if r["event_id"] in set(ids)]
    got.sort(key=lambda r: r["received_at"])
    got_ids = [r["event_id"] for r in got]
    assert got_ids == ids, f"dispatch order violated: {got_ids} != {ids}"
    assert all(r["signature_valid"] for r in got), "bad signature in batch"

    # Head-of-line blocking: a slow first event must not be overtaken.
    # Stall shorter than the client timeout (2s) so the head still succeeds,
    # just slowly -- while it is outstanding event 2 must never be sent.
    control("target-b", "stall", {"mode": "once", "seconds": 0.6})
    _, h1 = post_event("target-b", "rid-block-1", {"pos": "head"})
    _, h2 = post_event("target-b", "rid-block-2", {"pos": "behind"})
    time.sleep(0.25)
    recv = receipts("target-b")
    assert [r for r in recv if r["event_id"] in (h1["id"], h2["id"])] == [], \
        f"second event overtook an outstanding head: {recv}"
    # Once the slow head completes, both must land -- head first.
    wait_until("both blocked events delivered", lambda: (
        get_event(h1["id"])["status"] == "delivered"
        and get_event(h2["id"])["status"] == "delivered"
    ))
    landed = [r for r in receipts("target-b")
              if r["event_id"] in (h1["id"], h2["id"])]
    landed.sort(key=lambda r: r["received_at"])
    assert [r["event_id"] for r in landed] == [h1["id"], h2["id"]], landed


def test_retry_and_dead_letter() -> None:
    control("target-a", "fail", {"mode": "always"})
    _, ev = post_event("target-a", "rid-dlq-1", {"case": "retry/DLQ"})
    eid = ev["id"]

    def attempts(n):
        return lambda: len(get_event(eid)["delivery_attempts"]) == n

    wait_until("attempt 1 recorded", attempts(1))
    advance(1)
    wait_until("attempt 2 after 1s", attempts(2))
    advance(2)
    wait_until("attempt 3 after 2s", attempts(3))
    advance(4)
    wait_until("attempt 4 after 4s", attempts(4))

    def dead():
        e = get_event(eid)
        return e["status"] == "dead" and len(e["delivery_attempts"]) == 4

    wait_until("4th failure dead-letters", dead)
    detail = get_event(eid)
    outcomes = [a["outcome"] for a in detail["delivery_attempts"]]
    codes = [a["status_code"] for a in detail["delivery_attempts"]]
    assert outcomes == ["failed"] * 4, outcomes
    assert codes == [500] * 4, codes
    # Backoff schedule 1/2/4 between attempts.
    starts = [a["started_at"] for a in detail["delivery_attempts"]]
    finished = [a["finished_at"] for a in detail["delivery_attempts"]]
    gaps = [(starts[i] - finished[i - 1]) / 1_000_000 for i in range(1, 4)]
    assert gaps == [1.0, 2.0, 4.0], f"backoff gaps wrong: {gaps}"
    # A dead head must not be dispatched (target only saw 4 attempts).
    assert len([r for r in receipts("target-a") if r["event_id"] == eid]) == 4


def test_isolation() -> None:
    # target-a is still failing 500 (and rid-dlq-1 sits dead at its head).
    # target-b must be fully unaffected.
    reset("target-b")
    delivered_ids = []
    for i in range(3):
        _, ev = post_event("target-b", f"rid-iso-{i}", {"iso": i})
        delivered_ids.append(ev["id"])
    wait_until("target-b unaffected by target-a failures", lambda: all(
        get_event(i)["status"] == "delivered" for i in delivered_ids
    ))
    # While target-a fails, a *new* event for it must wait behind the dead
    # head -- and crucially must never touch target-b.
    _, blocked = post_event("target-a", "rid-iso-blocked", {"x": 1})
    time.sleep(1.0)
    assert get_event(blocked["id"])["status"] == "pending", \
        "event behind a dead-letter must not be dispatched"
    b_receipts = receipts("target-b")
    assert {r["event_id"] for r in b_receipts} == set(delivered_ids), \
        "target-a traffic leaked onto target-b"
    # Clear the failure; the dead head itself is redriven in the next test.
    control("target-a", "reset")


def redrive_dead_event() -> None:
    """Dead-lettered rid-dlq-1: redrive keeps identity + attempt history."""
    # Find the dead event from the retry test by request id.
    dead_events = [e for e in list_events("target-a")
                   if e["request_id"] == "rid-dlq-1"]
    assert len(dead_events) == 1 and dead_events[0]["status"] == "dead", dead_events
    eid = dead_events[0]["id"]
    before = get_event(eid)
    assert len(before["delivery_attempts"]) == 4

    status, body = http("POST", f"{API}/events/{eid}/redeliver")
    assert status == 200, body
    assert body["id"] == eid and body["redelivery_count"] == 1

    wait_until("redelivered event succeeds",
               lambda: get_event(eid)["status"] == "delivered")
    after = get_event(eid)
    assert after["request_id"] == "rid-dlq-1", "original identity must survive"
    assert len(after["delivery_attempts"]) == 5, \
        "original 4 attempts must be preserved, new attempt appended"
    assert after["delivery_attempts"][-1]["outcome"] == "delivered"
    # The event previously blocked behind the dead head now flows too.
    wait_until(
        "blocked neighbor delivered after redrive",
        lambda: next(
            (e for e in list_events("target-a")
             if e["request_id"] == "rid-iso-blocked"), {}
        ).get("status") == "delivered",
    )


def test_timeout() -> None:
    reset("target-a")
    # Stall one attempt beyond the client timeout; second attempt is normal.
    control("target-a", "stall", {"mode": "once", "seconds": 8})
    _, ev = post_event("target-a", "rid-timeout-1", {"case": "timeout"})
    eid = ev["id"]

    def first_attempt_done():
        a = get_event(eid)["delivery_attempts"]
        return len(a) == 1 and a[0]["finished_at"] is not None

    wait_until("timed-out attempt recorded", first_attempt_done, timeout=10)
    first = get_event(eid)["delivery_attempts"][0]
    assert first["outcome"] == "failed" and first["status_code"] is None
    assert "timeout" in first["error"], first
    assert len(receipts("target-a")) == 0, "timed-out request must not be accepted"

    # After the 1s backoff (virtual) the retried attempt succeeds.
    advance(1)
    wait_until("retried after timeout",
               lambda: get_event(eid)["status"] == "delivered")
    detail = get_event(eid)
    assert len(detail["delivery_attempts"]) == 2
    assert [a["outcome"] for a in detail["delivery_attempts"]] == [
        "failed", "delivered"]
    assert len(receipts("target-a")) == 1


def test_restart() -> None:
    reset("target-a")
    # Make the HTTP attempt hang outstanding, then crash the worker while the
    # request is demonstrably in flight at the target.
    control("target-a", "stall", {"mode": "always", "seconds": 8})
    _, ev = post_event("target-a", "rid-restart-1", {"case": "restart"})
    eid = ev["id"]
    wait_until("request is outstanding at the target",
               lambda: state("target-a")["in_flight"] >= 1, timeout=10)
    wait_until("event is marked in_flight",
               lambda: get_event(eid)["status"] == "in_flight", timeout=10)

    try:
        status, _ = http("POST", f"{WORKER_ADMIN}/admin/crash", timeout=5)
        assert status == 200
    except OSError:
        # os._exit can race the response flush; a dropped connection is fine.
        pass

    # Wait until the *restarted* process answers (the crashed one has exited).
    saw_down = False
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        try:
            s, _ = http("GET", f"{WORKER_ADMIN}/healthz", timeout=2)
            if s == 200 and saw_down:
                break
        except OSError:
            saw_down = True
        time.sleep(0.4)
    else:
        raise AssertionError("worker did not restart after crash")

    # Release the stalled HTTP call and move virtual time past any backoff.
    control("target-a", "reset")
    advance(2)
    wait_until("restarted event eventually delivered",
               lambda: get_event(eid)["status"] == "delivered", timeout=20)

    detail = get_event(eid)
    attempts = detail["delivery_attempts"]
    outcomes = [a["outcome"] for a in attempts]
    assert "interrupted" in outcomes, \
        f"crash must be recorded as an interrupted attempt: {outcomes}"
    assert outcomes[-1] == "delivered", outcomes
    # Duplicate delivery is expected/allowed after restart (at-least-once):
    # the same event identity reached the target at least twice.
    got = [r for r in receipts("target-a") if r["event_id"] == eid]
    assert len(got) >= 2, f"expected re-delivery of the same event, got {got}"
    assert all(r["request_id"] == "rid-restart-1" for r in got)
    assert all(r["signature_valid"] for r in got)


def main() -> int:
    wait_for_stack()
    # Fresh slate for both fake destinations.
    reset("target-a")
    reset("target-b")

    check("1. unknown target rejected (allow-list)", test_allowlist)
    check("2. request-id idempotent de-duplication", test_idempotency)
    check("3. HMAC over timestamp + original JSON bytes", test_signature)
    check("4. strict per-target ordering / no overtaking", test_ordering)
    check("5. 1/2/4s backoff, 4th failure -> dead-letter",
          test_retry_and_dead_letter)
    check("6. independent target isolation", test_isolation)
    check("7. dead-letter redrive keeps identity+history", redrive_dead_event)
    check("8. timeout is a failed attempt, then retried", test_timeout)
    check("9. crash/restart: no loss, duplicate re-send, history kept",
          test_restart)

    print("\n=== summary ===")
    failed = 0
    for name, ok, err in RESULTS:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f" -- {err}"))
        if not ok:
            failed += 1
    print(f"{len(RESULTS) - failed}/{len(RESULTS)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
