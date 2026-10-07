"""Tests for dead-letter replay semantics.

Replay creates a NEW delivery task (new delivery id, fresh attempt budget)
for the same event; the dead-lettered delivery and its attempt history are
preserved.  Covered here:

* a replayed event is really delivered and processed as a new event
  (not deduplicated against the original delivery)
* repeated dead-letter / replay cycles are stable and never error
* replaying the same dead-letter occurrence twice is a no-op
* concurrent replays over the same range are deterministic
* replay interleaved with normal delivery keeps exactly-once processing
* replay jobs survive process restart (recovery mid-replay)
* mass replay respects global / per-subscription concurrency limits
* replay jobs (success + failure) are recorded and queryable
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import pytest

from app.config import Config
from app.db import Database
from app.engine import DeliveryEngine
from app.receiver import Receiver, ReceiverBehavior
from app.routes import create_app
from app.signing import KeyStore
from app.transport import HttpTransport, InProcessTransport

from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers (mirrors of tests/test_delivery.py, kept local for independence)
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


async def dead_letter_one(eng, db, sub_id: str, payload: dict, event_type: str = "t") -> str:
    """Enqueue one event and wait until its delivery is dead-lettered."""
    res = await eng.enqueue_event(event_type, payload)
    await wait_for_terminal(db, res["event_id"])
    row = await db.fetchone(
        "SELECT status FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
    )
    assert row["status"] == "dead_letter"
    return res["event_id"]


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. Replay is really delivered and processed as a NEW event
# ---------------------------------------------------------------------------
def test_replay_is_delivered_as_new_event_not_deduplicated(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})
        assert len(rx.seen) == 1  # original delivery id

        rx.behavior.permanent = False
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result["replayed"] == 1
        new_delivery_id = result["delivery_ids"][0]

        await wait_for_terminal(db, event_id)
        tasks = await db.fetchall(
            "SELECT * FROM delivery_tasks WHERE event_id=? ORDER BY created_at",
            (event_id,),
        )
        assert len(tasks) == 2
        old, new = tasks
        assert old["status"] == "dead_letter" and old["attempt_count"] == 1
        assert new["id"] == new_delivery_id
        assert new["status"] == "succeeded"
        assert new["attempt_count"] == 1  # re-counted from zero
        assert new["replay_of"] == old["id"]

        # Receiver saw a brand-new delivery id: processed, not deduplicated.
        assert len(rx.seen) == 2
        replay_receipts = [e for e in rx.received if e.delivery_id == new_delivery_id]
        assert len(replay_receipts) == 1
        assert replay_receipts[0].duplicate is False

        # Full history: old attempts intact, new attempts separate.
        old_attempts = await db.fetchall(
            "SELECT result FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt_number",
            (old["id"],),
        )
        new_attempts = await db.fetchall(
            "SELECT result FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt_number",
            (new["id"],),
        )
        assert [a["result"] for a in old_attempts] == ["permanent"]
        assert [a["result"] for a in new_attempts] == ["success"]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_retries_are_deduplicated_but_replay_is_not(tmp_path):
    """Retries of the replayed delivery keep its (new) delivery id and are
    deduplicated by the receiver; only the replay itself is a new event."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})

        # Replay into a receiver that fails once (retryable) then succeeds.
        rx.behavior = ReceiverBehavior(fail_times=1)
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        new_id = result["delivery_ids"][0]
        await wait_for_terminal(db, event_id)

        new_task = await db.fetchone("SELECT * FROM delivery_tasks WHERE id=?", (new_id,))
        assert new_task["status"] == "succeeded"
        assert new_task["attempt_count"] == 2  # retryable + success, re-counted
        receipts = [e for e in rx.received if e.delivery_id == new_id]
        assert len(receipts) == 2
        assert receipts[0].duplicate is False   # replay itself: new event
        assert receipts[1].duplicate is True    # retry of the replay: deduplicated
        assert len(rx.seen) == 2                # processed exactly twice overall
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 2. Repeated dead-letter / replay cycles
# ---------------------------------------------------------------------------
def test_repeated_dead_letter_and_replay_cycles_are_stable(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})

        # Three full dead-letter -> replay -> dead-letter cycles.  Each cycle
        # must succeed without errors and produce a new dead-letter row.
        for cycle in range(3):
            result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
            assert result["replayed"] == 1, f"cycle {cycle}"
            await wait_for_terminal(db, event_id)

        # Receiver recovers: the next replay succeeds and stays succeeded.
        rx.behavior.permanent = False
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result["replayed"] == 1
        await wait_for_terminal(db, event_id)

        # Replaying the same range again is a stable no-op: every dead-letter
        # occurrence has already been replayed exactly once.
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result["replayed"] == 0

        # Every dead-letter occurrence produced its own row, each replayed once.
        dls = await db.fetchall(
            "SELECT * FROM dead_letters WHERE event_id=? ORDER BY dead_lettered_at",
            (event_id,),
        )
        assert len(dls) == 4  # initial + 3 failed replays
        assert len({d["delivery_id"] for d in dls}) == 4
        assert [d["replay_count"] for d in dls] == [1, 1, 1, 1]

        # 4 dead-lettered deliveries + 1 succeeded replay; nothing stuck.
        tasks = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        assert sorted(t["status"] for t in tasks) == [
            "dead_letter", "dead_letter", "dead_letter", "dead_letter", "succeeded",
        ]
        stuck = await db.fetchall(
            "SELECT id FROM delivery_tasks WHERE status IN ('pending', 'in_flight')"
        )
        assert stuck == []

        # The engine is still healthy: a normal event is delivered.
        ok = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, ok["event_id"])
        assert len(rx.seen) == 6  # 5 deliveries of event 1 + 1 of event 2
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_second_dead_letter_after_replay_does_not_break_engine(tmp_path):
    """Regression: re-dead-lettering a replayed delivery used to violate the
    dead_letters.delivery_id UNIQUE constraint, kill the delivery coroutine
    and leave the task bouncing between pending and in_flight forever."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})
        await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        await wait_for_terminal(db, event_id)  # dead-letters a second time

        # Give the scheduler a few ticks: nothing may bounce or stay stuck.
        await asyncio.sleep(0.3)
        tasks = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        assert len(tasks) == 2
        assert all(t["status"] == "dead_letter" for t in tasks)
        dls = await db.fetchall(
            "SELECT delivery_id FROM dead_letters WHERE event_id=?", (event_id,)
        )
        assert len(dls) == 2

        # Engine still processes new work.
        rx.behavior.permanent = False
        ok = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, ok["event_id"])
        assert len(rx.seen) == 3  # 2 dead-lettered deliveries + 1 success
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 3. Concurrency: parallel replays and replay vs normal delivery
# ---------------------------------------------------------------------------
def test_concurrent_replays_of_same_range_are_deterministic(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})
        rx.behavior.permanent = False

        now = time.time()
        results = await asyncio.gather(
            *(eng.replay_dead_letters(sub_id, 0.0, now, "admin") for _ in range(5))
        )
        # Exactly one of the racing replays wins; the others are no-ops.
        assert sorted(r["replayed"] for r in results) == [0, 0, 0, 0, 1]

        await wait_for_terminal(db, event_id)
        tasks = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        assert sorted(t["status"] for t in tasks) == ["dead_letter", "succeeded"]
        # Processed exactly twice: original attempt + one replay.
        assert len(rx.seen) == 2
        non_dup = [e for e in rx.received if not e.duplicate]
        assert len(non_dup) == 2
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_interleaved_with_normal_delivery(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        dead_id = await dead_letter_one(eng, db, sub_id, {"id": "dead"})
        rx.behavior.permanent = False

        async def replay():
            return await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")

        async def produce():
            ids = []
            for i in range(3):
                ids.append((await eng.enqueue_event("t", {"id": f"new-{i}"}))["event_id"])
            return ids

        result, new_ids = await asyncio.gather(replay(), produce())
        assert result["replayed"] == 1

        await wait_for_terminal(db, dead_id)
        for eid in new_ids:
            await wait_for_terminal(db, eid)

        # Every event processed exactly once: 1 original attempt + 1 replay
        # for the dead-lettered event, 1 each for the new events.
        assert len(rx.seen) == 5
        non_dup_ids = [e.delivery_id for e in rx.received if not e.duplicate]
        assert len(non_dup_ids) == len(set(non_dup_ids)) == 5
        # Nothing left behind.
        remaining = await db.fetchall(
            "SELECT id FROM delivery_tasks WHERE status IN ('pending', 'in_flight')"
        )
        assert remaining == []
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 4. Restart recovery around replay
# ---------------------------------------------------------------------------
def test_replay_created_tasks_survive_restart(tmp_path):
    """A replay issued while the delivery loop is down must be picked up
    after restart; a replayed task in flight at restart is reclaimed and
    still reaches a terminal state."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()
        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})
        await eng.stop()

        # Replay while the scheduler is down (process 'offline').
        rx.behavior.permanent = False
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result["replayed"] == 1
        task = await db.fetchone(
            "SELECT status FROM delivery_tasks WHERE id=?", (result["delivery_ids"][0],)
        )
        assert task["status"] == "pending"

        # Restart: the pending replayed task is delivered.
        await eng.start()
        await wait_for_terminal(db, event_id)
        assert len(rx.seen) == 2

        # Second replay, then simulate a crash with the replayed task claimed.
        event2 = await eng.enqueue_event("t", {"id": 2})
        await wait_for_terminal(db, event2["event_id"])
        rx.behavior.permanent = True
        dead2 = await dead_letter_one(eng, db, sub_id, {"id": 3})
        rx.behavior.permanent = False
        result2 = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result2["replayed"] == 1
        # Force the replayed task into a stale in-flight claim (crash mid-flight).
        await db.execute(
            "UPDATE delivery_tasks SET status='in_flight', claimed_at=? WHERE id=?",
            (time.time() - 9999, result2["delivery_ids"][0]),
        )
        await eng.stop()
        await eng.start()
        await wait_for_terminal(db, dead2)
        final = await db.fetchone(
            "SELECT status FROM delivery_tasks WHERE id=?", (result2["delivery_ids"][0],)
        )
        assert final["status"] == "succeeded"
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 5. Mass replay respects concurrency limits
# ---------------------------------------------------------------------------
def test_mass_replay_respects_concurrency_limits(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), global_concurrency=2, per_subscription_concurrency=1
        )
        slow = Receiver("slow", ks, cfg, ReceiverBehavior(permanent=True))
        fast = Receiver("fast", ks, cfg)
        inproc.register("slow", slow)
        inproc.register("fast", fast)
        slow_sub = await create_sub(db, "slow-sub", "inprocess://slow", ["slow.type"])
        await create_sub(db, "fast-sub", "inprocess://fast", ["fast.type"])
        await eng.start()

        # Dead-letter 6 events on the slow subscription.
        dead_ids = []
        for i in range(6):
            dead_ids.append(await dead_letter_one(eng, db, slow_sub, {"i": i}, "slow.type"))

        # Fix the slow receiver (now merely slow) and replay everything at once.
        slow.behavior = ReceiverBehavior(delay=0.15)
        result = await eng.replay_dead_letters(slow_sub, 0.0, time.time(), "admin")
        assert result["replayed"] == 6

        # Normal traffic on another subscription concurrently.
        fast_event = (await eng.enqueue_event("fast.type", {"i": "f"}))["event_id"]

        # While the replay backlog drains, limits must hold at every sample.
        deadline = time.time() + 10
        fast_done_at = None
        while time.time() < deadline:
            rows = await db.fetchall(
                "SELECT subscription_id, COUNT(*) AS c FROM delivery_tasks "
                "WHERE status='in_flight' GROUP BY subscription_id"
            )
            assert all(r["c"] <= 1 for r in rows)  # per-subscription cap
            assert sum(r["c"] for r in rows) <= 2  # global cap
            assert eng._inflight <= 2
            if fast_done_at is None:
                ft = await db.fetchone(
                    "SELECT status FROM delivery_tasks WHERE event_id=?", (fast_event,)
                )
                if ft and ft["status"] == "succeeded":
                    fast_done_at = time.time()
            pending = await db.fetchone(
                "SELECT COUNT(*) AS c FROM delivery_tasks WHERE status='pending'"
            )
            if pending["c"] == 0 and not rows:
                break
            await asyncio.sleep(0.01)

        for eid in dead_ids:
            await wait_for_terminal(db, eid)
        await wait_for_terminal(db, fast_event)
        # The fast subscription was not starved by the replay storm.
        assert fast_done_at is not None
        assert len(fast.seen) == 1
        assert len(slow.seen) == 12  # 6 failed originals + 6 replays
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 6. Replay jobs: queryable, distinguishable, failures recorded
# ---------------------------------------------------------------------------
def test_failed_replay_is_recorded_and_state_unchanged(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()
        event_id = await dead_letter_one(eng, db, sub_id, {"id": 1})

        # Inject a fault into the replay transaction.
        orig_execute = db._execute

        async def boom(sql, params=()):
            if "INSERT INTO delivery_tasks" in sql:
                raise RuntimeError("injected fault")
            return await orig_execute(sql, params)

        db._execute = boom
        try:
            with pytest.raises(RuntimeError):
                await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        finally:
            db._execute = orig_execute

        # The failure is recorded as a failed replay job.
        jobs = await db.fetchall("SELECT * FROM replay_jobs")
        assert len(jobs) == 1
        assert jobs[0]["status"] == "failed"
        assert "injected fault" in jobs[0]["detail"]

        # State is unchanged: dead letter still replayable, no orphan task.
        tasks = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        assert len(tasks) == 1 and tasks[0]["status"] == "dead_letter"
        dl = await db.fetchone("SELECT replayed_at FROM dead_letters")
        assert dl["replayed_at"] is None

        # A retry of the replay now succeeds.
        rx.behavior.permanent = False
        result = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert result["replayed"] == 1
        await wait_for_terminal(db, event_id)
        jobs = await db.fetchall("SELECT status FROM replay_jobs ORDER BY created_at")
        assert [j["status"] for j in jobs] == ["failed", "completed"]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_jobs_api_and_overlapping_ranges(tmp_path):
    cfg = make_config(str(tmp_path))
    app = create_app(cfg)
    with TestClient(app) as client:
        rx = Receiver("r1", app.state.key_store, cfg, ReceiverBehavior(permanent=True))
        app.state.inproc.register("r1", rx)
        r = client.post("/subscriptions", json={
            "name": "s1", "target": "inprocess://r1", "event_types": ["*"]})
        sub_id = r.json()["id"]
        for i in range(2):
            r = client.post("/events", json={"event_type": "t", "payload": {"id": i}})
            event_id = r.json()["event_id"]
            for _ in range(200):
                ev = client.get(f"/events/{event_id}").json()
                if ev["deliveries"][0]["status"] == "dead_letter":
                    break
                time.sleep(0.02)

        # Two overlapping replay ranges: each dead letter is replayed once,
        # the second (overlapping) job is a no-op.
        rx.behavior.permanent = False
        now = time.time()
        r1 = client.post(
            f"/admin/subscriptions/{sub_id}/replay?from_ts=0&to_ts={now}",
            headers={"X-API-Key": cfg.admin_api_key},
        ).json()
        r2 = client.post(
            f"/admin/subscriptions/{sub_id}/replay?from_ts=0&to_ts={time.time()}",
            headers={"X-API-Key": cfg.admin_api_key},
        ).json()
        assert r1["replayed"] == 2 and r2["replayed"] == 0
        assert r1["job_id"] != r2["job_id"]

        # Replay jobs are queryable and distinguishable (viewer allowed).
        r = client.get("/admin/replay-jobs", headers={"X-API-Key": cfg.viewer_api_key})
        assert r.status_code == 200
        jobs = {j["id"]: j for j in r.json()}
        assert jobs[r1["job_id"]]["status"] == "completed"
        assert jobs[r1["job_id"]]["replayed_count"] == 2
        assert jobs[r2["job_id"]]["replayed_count"] == 0
        assert jobs[r1["job_id"]]["created_by"] == "admin"

        # Authz: anonymous and viewer cannot replay; anonymous cannot list jobs.
        assert client.post(f"/admin/subscriptions/{sub_id}/replay").status_code == 401
        assert client.post(
            f"/admin/subscriptions/{sub_id}/replay",
            headers={"X-API-Key": cfg.viewer_api_key},
        ).status_code == 403
        assert client.get("/admin/replay-jobs").status_code == 401

        # Denied and allowed replays are both in the audit trail.
        audit = client.get("/admin/audit", headers={"X-API-Key": cfg.admin_api_key}).json()
        replay_entries = [a for a in audit if a["action"] == "replay"]
        assert any(a["decision"] == "denied" for a in replay_entries)
        assert any(a["decision"] == "allowed" for a in replay_entries)


# ---------------------------------------------------------------------------
# 7. Legacy schema migration
# ---------------------------------------------------------------------------
def test_legacy_schema_is_migrated_and_replayable(tmp_path):
    """A database created with the old schema (UNIQUE(event_id,
    subscription_id) on delivery_tasks, no replay columns) is migrated on
    open and supports the new replay semantics."""

    async def scenario():
        db_path = os.path.join(str(tmp_path), "legacy.db")
        # Build a legacy database by hand.
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE events (
                id TEXT PRIMARY KEY, event_type TEXT NOT NULL, payload TEXT NOT NULL,
                ordering_key TEXT, status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL
            );
            CREATE TABLE subscriptions (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, target TEXT NOT NULL,
                event_types TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL
            );
            CREATE TABLE delivery_tasks (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES events(id),
                subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
                ordering_key TEXT NOT NULL, status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL, next_attempt_at REAL NOT NULL,
                claimed_at REAL, last_error TEXT, dead_letter_reason TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                UNIQUE(event_id, subscription_id)
            );
            """
        )
        now = time.time()
        conn.execute(
            "INSERT INTO events VALUES ('e1', 't', '{}', NULL, 'pending', ?)", (now,)
        )
        conn.execute(
            "INSERT INTO subscriptions VALUES ('s1', 's1', 'inprocess://r1', '[\"*\"]', 1, ?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO delivery_tasks VALUES "
            "('d1', 'e1', 's1', 's1', 'dead_letter', 1, 5, ?, NULL, 'boom', "
            "'permanent_failure', ?, ?)",
            (now, now, now),
        )
        conn.commit()
        conn.close()

        # Open through the new code: migration runs.
        cfg = make_config(str(tmp_path), db_path=db_path)
        db = Database(cfg.db_path)
        await db.open()
        inproc = InProcessTransport()
        http = HttpTransport()
        ks = KeyStore(db)
        await ks.initialize()
        eng = DeliveryEngine(db, ks, inproc, http, cfg)

        # Legacy UNIQUE constraint is gone; replay columns exist.
        indexes = await db.fetchall("PRAGMA index_list(delivery_tasks)")
        assert not any(idx[2] == 1 and idx[3] == "u" for idx in indexes)
        cols = {r[1] for r in await db.fetchall("PRAGMA table_info(delivery_tasks)")}
        assert {"replay_of", "replay_job_id"} <= cols

        # The legacy dead-lettered row survived the migration.
        task = await db.fetchone("SELECT * FROM delivery_tasks WHERE id='d1'")
        assert task["status"] == "dead_letter"

        # Replay creates a second task for the same (event, subscription).
        rx = Receiver("r1", ks, cfg)
        inproc.register("r1", rx)
        await eng.start()
        await db.execute(
            "INSERT INTO dead_letters "
            "(id, delivery_id, event_id, subscription_id, reason, last_error, "
            "attempt_count, attempts_json, dead_lettered_at, replay_count) "
            "VALUES ('dl1', 'd1', 'e1', 's1', 'permanent_failure', 'boom', 1, '[]', ?, 0)",
            (time.time(),),
        )
        result = await eng.replay_dead_letters("s1", 0.0, time.time(), "admin")
        assert result["replayed"] == 1
        await wait_for_terminal(db, "e1")
        tasks = await db.fetchall(
            "SELECT status FROM delivery_tasks WHERE event_id='e1' ORDER BY created_at"
        )
        assert [t["status"] for t in tasks] == ["dead_letter", "succeeded"]
        assert len(rx.seen) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())
