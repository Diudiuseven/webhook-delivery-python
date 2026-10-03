"""End-to-end tests for the reliable event delivery layer.

Coverage:
* crash / commit consistency (event + delivery tasks committed together)
* retryable vs permanent failure classification and dead lettering
* per-subscription FIFO ordering, retries not overtaking, keyed concurrency
* subscription isolation under a slow consumer
* receiver-side deduplication of redeliveries after timeout
* signing key rotation (old + new keys accepted, retired keys rejected)
* dead-letter replay preserving history, replay time range
* permission checks (401/403) and audit trail
* restart recovery of in-flight tasks and periodic claim reclaim
* metrics / backlog observability
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app.config import Config
from app.db import Database
from app.engine import DeliveryEngine
from app.receiver import Receiver, ReceiverBehavior
from app.routes import create_app
from app.signing import DELIVERY_ID_HEADER, sign_headers
from app.transport import HttpTransport, InProcessTransport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_config(tmp: str, **overrides) -> Config:
    defaults = dict(
        db_path=os.path.join(tmp, "test.db"),
        scheduler_interval=0.02,
        backoff_base=0.05,
        backoff_factor=2.0,
        backoff_max=0.5,
        jitter_ratio=0.1,
        delivery_timeout=1.0,
        global_concurrency=4,
        per_subscription_concurrency=1,
        claim_timeout=0.5,
        signature_tolerance=300.0,
        admin_api_key="admin-dev-key",
        viewer_api_key="viewer-dev-key",
    )
    defaults.update(overrides)
    return Config(**defaults)


async def make_engine(tmp: str, **overrides):
    cfg = make_config(tmp, **overrides)
    db = Database(cfg.db_path)
    await db.open()
    inproc = InProcessTransport()
    http = HttpTransport()
    from app.signing import KeyStore

    ks = KeyStore(db)
    await ks.initialize()
    eng = DeliveryEngine(db, ks, inproc, http, cfg)
    return eng, db, ks, inproc, http, cfg


async def create_sub(db: Database, name: str, target: str, event_types: list[str]) -> str:
    now = time.time()
    sid = uuid.uuid4().hex
    await db.execute(
        "INSERT INTO subscriptions (id, name, target, event_types, active, created_at) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        (sid, name, target, json.dumps(event_types), now),
    )
    return sid


async def wait_for_terminal(db: Database, event_id: str, timeout: float = 5.0) -> list:
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        rows = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        if not rows or all(r["status"] in ("succeeded", "dead_letter") for r in rows):
            return rows
        await asyncio.sleep(0.02)
    raise AssertionError(f"event {event_id} not terminal: {[dict(r) for r in rows]}")


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. Commit consistency / crash no-loss
# ---------------------------------------------------------------------------
def test_event_and_tasks_commit_together_and_survive_crash(tmp_path):
    """Event and its delivery tasks are committed together; with the delivery
    loop stopped (simulating a crash right after commit) nothing is lost and
    delivery completes after 'restart'."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])

        res = await eng.enqueue_event("order.created", {"id": 1})
        # Engine is NOT started: rows must already be durable and pending.
        ev = await db.fetchone("SELECT status FROM events WHERE id=?", (res["event_id"],))
        assert ev["status"] == "pending"
        tasks = await db.fetchall(
            "SELECT status, attempt_count FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert len(tasks) == 1 and tasks[0]["status"] == "pending"

        # Restart the process.
        await eng.start()
        await wait_for_terminal(db, res["event_id"])
        assert len(rx.seen) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_subscription_matching_fanout(tmp_path):
    """Only subscriptions whose event types match get a delivery task; the
    event itself is always persisted."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await create_sub(db, "orders", "inprocess://r1", ["order.created"])
        await create_sub(db, "billing", "inprocess://r1", ["invoice.paid"])
        await eng.start()

        r1 = await eng.enqueue_event("order.created", {"id": 1})
        r2 = await eng.enqueue_event("invoice.paid", {"id": 2})
        r3 = await eng.enqueue_event("unknown.type", {"id": 3})
        assert len(r1["matched_subscriptions"]) == 1
        assert len(r2["matched_subscriptions"]) == 1
        assert len(r3["matched_subscriptions"]) == 0

        await wait_for_terminal(db, r1["event_id"])
        await wait_for_terminal(db, r2["event_id"])
        await wait_for_terminal(db, r3["event_id"])
        assert len(rx.seen) == 2
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 2. Retry policy: retryable then success / permanent / exhausted
# ---------------------------------------------------------------------------
def test_retryable_failures_then_success(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(fail_times=2))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_terminal(db, res["event_id"])

        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert task["status"] == "succeeded"
        assert task["attempt_count"] == 3
        attempts = await db.fetchall(
            "SELECT result FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt_number",
            (task["id"],),
        )
        assert [a["result"] for a in attempts] == ["retryable", "retryable", "success"]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_permanent_failure_moves_to_dead_letter(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_terminal(db, res["event_id"])

        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
        )
        assert task["status"] == "dead_letter"
        assert task["dead_letter_reason"] == "permanent_failure"
        assert task["attempt_count"] == 1  # permanent failures are not retried
        dl = await db.fetchone(
            "SELECT * FROM dead_letters WHERE delivery_id=?", (task["id"],)
        )
        assert dl is not None and dl["reason"] == "permanent_failure"
        assert json.loads(dl["attempts_json"])[0]["result"] == "permanent"
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_retries_exhausted_dead_letter(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path), max_attempts=3)
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(fail_times=100))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_terminal(db, res["event_id"])

        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
        )
        assert task["status"] == "dead_letter"
        assert task["dead_letter_reason"] == "retries_exhausted"
        assert task["attempt_count"] == 3
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 3. Ordering
# ---------------------------------------------------------------------------
def test_per_subscription_fifo_ordering(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(delay=0.3, delay_times=1))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        r1 = await eng.enqueue_event("t", {"id": 1})
        r2 = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, r1["event_id"])
        await wait_for_terminal(db, r2["event_id"])

        first = [e for e in rx.received if not e.duplicate]
        ids = [e.payload["id"] for e in first]
        assert ids == [1, 2]
        assert first[1].received_at - first[0].received_at >= 0.25
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_retry_does_not_overtake_newer_event(tmp_path):
    """A retryable first delivery must complete (success or dead letter)
    before a later event for the same subscription is delivered."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver(
            "r1", ks, cfg,
            ReceiverBehavior(fail_times=1, delay=0.2, delay_times=1),
        )
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        r1 = await eng.enqueue_event("t", {"id": 1})
        r2 = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, r1["event_id"])
        await wait_for_terminal(db, r2["event_id"])

        first = [e for e in rx.received if not e.duplicate]
        ids = [e.payload["id"] for e in first]
        assert ids == [1, 2]
        assert first[1].received_at - first[0].received_at >= 0.15
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_keyed_ordering_allows_concurrent_keys(tmp_path):
    """Different ordering keys for the same subscription deliver concurrently
    (up to the per-sub cap), while the same key stays serial."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), per_subscription_concurrency=2, global_concurrency=4
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(delay=0.2))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        r1 = await eng.enqueue_event("t", {"id": 1}, ordering_key="k1")
        r2 = await eng.enqueue_event("t", {"id": 2}, ordering_key="k2")
        t0 = time.time()
        await wait_for_terminal(db, r1["event_id"])
        await wait_for_terminal(db, r2["event_id"])
        elapsed = time.time() - t0
        assert elapsed < 0.35  # concurrent, not ~0.4s serial

        r3 = await eng.enqueue_event("t", {"id": 3}, ordering_key="k1")
        r4 = await eng.enqueue_event("t", {"id": 4}, ordering_key="k1")
        await wait_for_terminal(db, r3["event_id"])
        await wait_for_terminal(db, r4["event_id"])
        ids = [e.payload["id"] for e in rx.received]
        assert ids.index(3) > ids.index(1)
        assert ids.index(4) > ids.index(3)
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 4. Isolation / backlog
# ---------------------------------------------------------------------------
def test_slow_subscription_does_not_block_others(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), global_concurrency=4, per_subscription_concurrency=1
        )
        slow = Receiver("slow", ks, cfg, ReceiverBehavior(delay=0.1))
        fast = Receiver("fast", ks, cfg)
        inproc.register("slow", slow)
        inproc.register("fast", fast)
        await create_sub(db, "slow-sub", "inprocess://slow", ["slow.type"])
        await create_sub(db, "fast-sub", "inprocess://fast", ["fast.type"])
        await eng.start()

        slow_ids, fast_ids = [], []
        for i in range(5):
            slow_ids.append((await eng.enqueue_event("slow.type", {"i": i}))["event_id"])
            fast_ids.append((await eng.enqueue_event("fast.type", {"i": i}))["event_id"])

        # Wait until the fast subscription is fully drained.
        deadline = time.time() + 5
        while time.time() < deadline:
            rows = await db.fetchall(
                "SELECT status FROM delivery_tasks WHERE event_id IN "
                + "(" + ",".join("?" * len(fast_ids)) + ")",
                fast_ids,
            )
            if rows and all(r["status"] == "succeeded" for r in rows):
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError("fast subscription not drained in time")

        # At that point the slow subscription must still have pending work.
        slow_rows = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id IN "
            + "(" + ",".join("?" * len(slow_ids)) + ")",
            slow_ids,
        )
        assert any(r["status"] != "succeeded" for r in slow_rows)

        # Eventually everything succeeds.
        for eid in slow_ids:
            await wait_for_terminal(db, eid)
        assert len(slow.seen) == 5 and len(fast.seen) == 5
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 5. Receiver dedup on redelivery after timeout
# ---------------------------------------------------------------------------
def test_redelivery_after_timeout_is_deduplicated(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), delivery_timeout=0.1
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(delay=0.3, delay_times=1))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_terminal(db, res["event_id"])

        task = await db.fetchone(
            "SELECT status, attempt_count FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert task["status"] == "succeeded"
        assert task["attempt_count"] == 2  # timed out, then redelivered
        assert len(rx.received) == 2       # receiver saw both attempts
        assert len(rx.seen) == 1           # but processed exactly once
        assert rx.received[1].duplicate is True
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 6. Key rotation
# ---------------------------------------------------------------------------
def test_key_rotation_accepts_old_and_new_rejects_retired(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        r1 = await eng.enqueue_event("t", {"id": 1})
        await wait_for_terminal(db, r1["event_id"])

        await ks.rotate()
        r2 = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, r2["event_id"])
        assert len(rx.seen) == 2  # both signatures verified

        # Old (rotated) key is still accepted during the transition window.
        keys = await ks.all()
        old_key = next(k for k in keys.values() if k.status == "rotated")
        body = b'{"id":3}'
        headers = sign_headers(old_key.secret, old_key.key_id, int(time.time()), body)
        headers[DELIVERY_ID_HEADER] = "manual-dedup-1"
        result = await rx.handle(headers, body)
        assert result.outcome == "success"

        # Retired keys are rejected.
        await ks.retire(old_key.key_id)
        result = await rx.handle(headers, body)
        assert result.outcome == "permanent" and result.status_code == 401
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 7. Restart recovery / claim reclaim
# ---------------------------------------------------------------------------
def test_startup_reclaims_inflight_tasks(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), claim_timeout=10_000
        )
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await eng.stop()

        # Simulate a crash: the task is stuck in flight with an old claim.
        await db.execute(
            "UPDATE delivery_tasks SET status='in_flight', claimed_at=? WHERE event_id=?",
            (time.time() - 9999, res["event_id"]),
        )

        await eng.start()
        await wait_for_terminal(db, res["event_id"])
        assert len(rx.seen) == 1
        task = await db.fetchone(
            "SELECT status, attempt_count FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert task["status"] == "succeeded"
        assert task["attempt_count"] == 2  # interrupted attempt + redelivery
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_periodic_reclaim_of_stale_claims(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), claim_timeout=0.3
        )
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        # Force a stale in-flight claim (as if the handler hung).
        await db.execute(
            "UPDATE delivery_tasks SET status='in_flight', claimed_at=? WHERE event_id=?",
            (time.time() - 9999, res["event_id"]),
        )
        await wait_for_terminal(db, res["event_id"])
        assert len(rx.seen) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 8. Dead letter replay + permissions + audit (API level)
# ---------------------------------------------------------------------------
def test_dead_letter_replay_preserves_history(tmp_path):
    cfg = make_config(str(tmp_path))
    app = create_app(cfg)
    with TestClient(app) as client:
        rx = Receiver("r1", app.state.key_store, cfg, ReceiverBehavior(permanent=True))
        app.state.inproc.register("r1", rx)

        r = client.post("/subscriptions", json={
            "name": "s1", "target": "inprocess://r1", "event_types": ["*"]})
        sub_id = r.json()["id"]
        r = client.post("/events", json={"event_type": "t", "payload": {"id": 1}})
        event_id = r.json()["event_id"]

        for _ in range(200):
            ev = client.get(f"/events/{event_id}").json()
            if ev["deliveries"][0]["status"] == "dead_letter":
                break
            import time as _t; _t.sleep(0.02)
        else:
            raise AssertionError("not dead-lettered")

        # Viewer can read dead letters.
        r = client.get("/admin/dead-letters", headers={"X-API-Key": cfg.viewer_api_key})
        assert r.status_code == 200 and len(r.json()) == 1
        dl = r.json()[0]
        assert dl["reason"] == "permanent_failure"
        assert len(dl["attempts"]) == 1

        # Replay as admin, now with a healthy receiver.
        rx.behavior.permanent = False
        r = client.post(
            f"/admin/subscriptions/{sub_id}/replay",
            headers={"X-API-Key": cfg.admin_api_key},
        )
        assert r.status_code == 200 and r.json()["replayed"] == 1

        for _ in range(200):
            ev = client.get(f"/events/{event_id}").json()
            if ev["deliveries"][0]["status"] == "succeeded":
                break
            import time as _t; _t.sleep(0.02)
        else:
            raise AssertionError("replayed event not delivered")

        task = ev["deliveries"][0]
        detail = client.get(f"/deliveries/{task['id']}").json()
        assert detail["status"] == "succeeded"
        # Attempt history is preserved across the replay.
        assert len(detail["attempts"]) >= 2
        assert any(a["result"] == "permanent" for a in detail["attempts"])

        dl2 = client.get(
            "/admin/dead-letters", headers={"X-API-Key": cfg.viewer_api_key}
        ).json()[0]
        assert dl2["replay_count"] == 1 and dl2["replayed_at"] is not None


def test_replay_time_range_filters(tmp_path):
    cfg = make_config(str(tmp_path))
    app = create_app(cfg)
    with TestClient(app) as client:
        rx = Receiver("r1", app.state.key_store, cfg, ReceiverBehavior(permanent=True))
        app.state.inproc.register("r1", rx)
        r = client.post("/subscriptions", json={
            "name": "s1", "target": "inprocess://r1", "event_types": ["*"]})
        sub_id = r.json()["id"]
        r = client.post("/events", json={"event_type": "t", "payload": {"id": 1}})
        event_id = r.json()["event_id"]
        for _ in range(200):
            ev = client.get(f"/events/{event_id}").json()
            if ev["deliveries"][0]["status"] == "dead_letter":
                break
            import time as _t; _t.sleep(0.02)

        # Range entirely in the future -> nothing replayed.
        r = client.post(
            f"/admin/subscriptions/{sub_id}/replay?from_ts=9999999999&to_ts=10000000000",
            headers={"X-API-Key": cfg.admin_api_key},
        )
        assert r.status_code == 200 and r.json()["replayed"] == 0
        task = client.get(f"/events/{event_id}").json()["deliveries"][0]
        assert task["status"] == "dead_letter"

        # Open range -> replayed.
        rx.behavior.permanent = False
        r = client.post(
            f"/admin/subscriptions/{sub_id}/replay",
            headers={"X-API-Key": cfg.admin_api_key},
        )
        assert r.json()["replayed"] == 1


def test_unauthorized_replay_and_dead_letter_access_are_audited(tmp_path):
    cfg = make_config(str(tmp_path))
    app = create_app(cfg)
    with TestClient(app) as client:
        r = client.post("/subscriptions", json={
            "name": "s1", "target": "inprocess://r1", "event_types": ["*"]})
        sub_id = r.json()["id"]

        assert client.get("/admin/dead-letters").status_code == 401
        assert client.post(f"/admin/subscriptions/{sub_id}/replay").status_code == 401
        assert client.post(
            f"/admin/subscriptions/{sub_id}/replay",
            headers={"X-API-Key": cfg.viewer_api_key},
        ).status_code == 403

        audit = client.get(
            "/admin/audit", headers={"X-API-Key": cfg.admin_api_key}
        ).json()
        decisions = [(a["action"], a["decision"]) for a in audit]
        assert ("dead_letters:read", "denied") in decisions
        assert ("replay", "denied") in decisions
        # Viewer must not read the audit log.
        assert client.get(
            "/admin/audit", headers={"X-API-Key": cfg.viewer_api_key}
        ).status_code == 403


def test_metrics_expose_backlog_and_limits(tmp_path):
    cfg = make_config(str(tmp_path))
    app = create_app(cfg)
    with TestClient(app) as client:
        rx = Receiver("r1", app.state.key_store, cfg)
        app.state.inproc.register("r1", rx)
        client.post("/subscriptions", json={
            "name": "s1", "target": "inprocess://r1", "event_types": ["*"]})
        for i in range(3):
            client.post("/events", json={"event_type": "t", "payload": {"id": i}})
        for _ in range(200):
            m = client.get("/metrics").json()
            if m["succeeded_total"] == 3:
                break
            import time as _t; _t.sleep(0.02)
        else:
            raise AssertionError("not delivered")
        assert m["backlog"]["pending"] == 0
        assert m["backlog"]["in_flight"] == 0
        assert m["limits"]["global_concurrency"] == cfg.global_concurrency
        assert m["limits"]["per_subscription_concurrency"] == cfg.per_subscription_concurrency


# ---------------------------------------------------------------------------
# 9. Rotation across retries + permanent-failure isolation
# ---------------------------------------------------------------------------
def test_rotation_during_retry_resigns_with_new_key(tmp_path):
    """An event that is mid-retry when rotation happens must be re-signed with
    the new key on its next attempt and remain verifiable."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), backoff_base=0.2, backoff_max=0.2, jitter_ratio=0.0
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(fail_times=1))
        inproc.register("r1", rx)
        await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        old_key = await ks.get_active()
        res = await eng.enqueue_event("t", {"id": 1})
        # Wait for the first (failed) attempt to land, then rotate.
        deadline = time.time() + 5
        while time.time() < deadline:
            task = await db.fetchone(
                "SELECT attempt_count FROM delivery_tasks WHERE event_id=?",
                (res["event_id"],),
            )
            if task and task["attempt_count"] >= 1:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("first attempt never landed")
        new_key = await ks.rotate()
        assert new_key.key_id != old_key.key_id

        await wait_for_terminal(db, res["event_id"])
        assert len(rx.received) == 2
        assert rx.received[0].key_id == old_key.key_id
        assert rx.received[1].key_id == new_key.key_id
        # Retries carry the same delivery id -> receiver dedups them.
        assert rx.received[1].duplicate is True
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_permanent_failure_does_not_block_other_subscriptions(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        bad = Receiver("bad", ks, cfg, ReceiverBehavior(permanent=True))
        good = Receiver("good", ks, cfg)
        inproc.register("bad", bad)
        inproc.register("good", good)
        await create_sub(db, "bad-sub", "inprocess://bad", ["bad.type"])
        await create_sub(db, "good-sub", "inprocess://good", ["good.type"])
        await eng.start()

        r_bad = await eng.enqueue_event("bad.type", {"id": 1})
        r_good = await eng.enqueue_event("good.type", {"id": 2})
        await wait_for_terminal(db, r_bad["event_id"])
        await wait_for_terminal(db, r_good["event_id"])

        bad_task = await db.fetchone(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (r_bad["event_id"],)
        )
        good_task = await db.fetchone(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (r_good["event_id"],)
        )
        assert bad_task["status"] == "dead_letter"
        assert good_task["status"] == "succeeded"
        assert len(good.seen) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_metrics_reflect_backlog_for_slow_consumer(tmp_path):
    """A slow receiver with a saturated per-subscription queue shows up as
    backlog (receiver-bound), while the global cap is not reached."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), global_concurrency=4, per_subscription_concurrency=1
        )
        slow = Receiver("slow", ks, cfg, ReceiverBehavior(delay=0.5))
        inproc.register("slow", slow)
        await create_sub(db, "slow-sub", "inprocess://slow", ["*"])
        await eng.start()

        ids = []
        for i in range(4):
            ids.append((await eng.enqueue_event("t", {"i": i}))["event_id"])
        await asyncio.sleep(0.15)
        m = await eng.metrics()
        assert m["backlog"]["pending"] >= 2
        assert m["backlog"]["oldest_pending_age_s"] > 0.1
        assert m["inflight"] >= 1
        assert m["saturated"] is False  # global cap not reached -> receiver-bound
        for eid in ids:
            await wait_for_terminal(db, eid, timeout=5)
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())
