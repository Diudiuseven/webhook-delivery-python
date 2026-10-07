"""Dead-letter replay semantics.

Coverage:
* a replayed delivery reaches the receiver as a *new* event (fresh wire
  delivery id) and is really processed, not deduplicated away
* redeliveries inside a replay round (timeout / crash recovery) are still
  deduplicated by the receiver
* repeated dead-lettering of the same event and consecutive replays are
  stable, reach a terminal state and never error out
* attempt counting restarts per replay round while the full attempt history
  stays queryable (tagged by replay_seq)
* concurrent replay jobs and replay interleaved with normal delivery are
  deterministic (no double replay, no lost replay)
* bulk replay honours the per-subscription / global concurrency limits
* replay survives a process restart
* replay jobs are auditable and permission-checked
"""
from __future__ import annotations

import asyncio
import json
import time

from fastapi.testclient import TestClient

from app.receiver import Receiver, ReceiverBehavior
from app.routes import create_app

from helpers import (
    create_sub,
    make_config,
    make_engine,
    run,
    wait_for_task_status,
    wait_for_terminal,
)


# ---------------------------------------------------------------------------
# 1. Replay is delivered and processed as a new event
# ---------------------------------------------------------------------------
def test_replay_is_delivered_and_processed_as_new_event(tmp_path):
    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")
        assert len(rx.seen) == 1
        round0_delivery_id = rx.received[0].delivery_id

        # Receiver healthy again: replay must produce a real new delivery.
        rx.behavior.permanent = False
        out = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out["replayed"] == 1
        await wait_for_task_status(db, res["event_id"], "succeeded")

        # The receiver processed two distinct deliveries; the replay was NOT
        # deduplicated against the dead-lettered round.
        assert len(rx.seen) == 2
        replays = [e for e in rx.received if e.delivery_id != round0_delivery_id]
        assert len(replays) == 1
        assert replays[0].duplicate is False
        assert replays[0].event_id == res["event_id"]

        # Attempt counting restarts for the new round; history is preserved.
        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
        )
        assert task["replay_seq"] == 1
        assert task["attempt_count"] == 1  # recounted from zero after replay
        attempts = await db.fetchall(
            "SELECT replay_seq, attempt_number, result FROM delivery_attempts "
            "WHERE delivery_id=? ORDER BY id",
            (task["id"],),
        )
        assert [(a["replay_seq"], a["attempt_number"], a["result"]) for a in attempts] == [
            (0, 1, "permanent"),
            (1, 1, "success"),
        ]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_redelivery_inside_replay_round_is_still_deduplicated(tmp_path):
    """A timed-out attempt inside the replay round is redelivered with the
    same wire id and the receiver deduplicates it; only the replay itself
    counts as new."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), delivery_timeout=0.1
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")

        # Replay round: first attempt is slow (times out), the redelivery is
        # a duplicate for the receiver.
        rx.behavior = ReceiverBehavior(delay=0.3, delay_times=1)
        await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        await wait_for_task_status(db, res["event_id"], "succeeded")

        # Two distinct wire ids overall (round 0, round 1)...
        assert len(rx.seen) == 2
        # ...but the replay round was delivered twice (timeout + redelivery)
        # and the second one was recognised as a duplicate.
        round1 = [e for e in rx.received if e.delivery_id.endswith(":r1")]
        assert len(round1) == 2
        assert round1[0].duplicate is False
        assert round1[1].duplicate is True
        task = await db.fetchone(
            "SELECT status, attempt_count FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert task["status"] == "succeeded"
        assert task["attempt_count"] == 2  # counted within the replay round
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 2. Repeated dead-lettering and consecutive replays are stable
# ---------------------------------------------------------------------------
def test_repeated_dead_letter_and_consecutive_replays(tmp_path):
    """Event dead-letters, is replayed, fails again and dead-letters again,
    then is replayed once more and succeeds.  No errors, no stuck states."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")

        # Replay #1: receiver still broken -> must dead-letter again cleanly.
        out1 = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out1["replayed"] == 1
        await wait_for_task_status(db, res["event_id"], "dead_letter")

        dls = await db.fetchall(
            "SELECT * FROM dead_letters WHERE event_id=?", (res["event_id"],)
        )
        assert len(dls) == 1  # upserted, not duplicated
        dl = dls[0]
        assert dl["reason"] == "permanent_failure"
        assert dl["replay_count"] == 1        # cumulative across rounds
        assert dl["replayed_at"] is None      # replayable again
        assert dl["last_replay_job_id"] == out1["job_id"]
        # Snapshot belongs to the round that just failed.
        snap = json.loads(dl["attempts_json"])
        assert [a["replay_seq"] for a in snap] == [1]
        assert snap[0]["result"] == "permanent"

        # Replay #2: receiver fixed -> succeeds.
        rx.behavior.permanent = False
        out2 = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out2["replayed"] == 1
        assert out2["job_id"] != out1["job_id"]
        await wait_for_task_status(db, res["event_id"], "succeeded")

        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
        )
        assert task["replay_seq"] == 2
        assert task["attempt_count"] == 1
        # Receiver saw three distinct deliveries (one per round).
        assert len(rx.seen) == 3
        # Full history across all rounds is preserved.
        attempts = await db.fetchall(
            "SELECT replay_seq, result FROM delivery_attempts WHERE delivery_id=? "
            "ORDER BY id",
            (task["id"],),
        )
        assert [(a["replay_seq"], a["result"]) for a in attempts] == [
            (0, "permanent"),
            (1, "permanent"),
            (2, "success"),
        ]
        dl2 = await db.fetchone(
            "SELECT replay_count FROM dead_letters WHERE event_id=?", (res["event_id"],)
        )
        assert dl2["replay_count"] == 2

        # The engine is still healthy afterwards.
        res2 = await eng.enqueue_event("t", {"id": 2})
        await wait_for_task_status(db, res2["event_id"], "succeeded")
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_of_already_replayed_entry_is_a_noop(tmp_path):
    """A second replay over the same range while the task is no longer
    dead-lettered replays nothing and does not error."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")
        rx.behavior.permanent = False

        out1 = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out1["replayed"] == 1
        # Immediate second replay over the same range: nothing to do.
        out2 = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out2["replayed"] == 0

        await wait_for_task_status(db, res["event_id"], "succeeded")
        assert len(rx.seen) == 2  # exactly one replay round happened
        jobs = await db.fetchall("SELECT replayed_count FROM replay_jobs ORDER BY created_at")
        assert [j["replayed_count"] for j in jobs] == [1, 0]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 3. Concurrency: replay jobs vs each other and vs normal delivery
# ---------------------------------------------------------------------------
def test_concurrent_replay_jobs_are_deterministic(tmp_path):
    """Two replay jobs racing over the same dead letter: exactly one of them
    replays it; the receiver processes exactly one new delivery."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")
        rx.behavior.permanent = False

        now = time.time()
        out1, out2 = await asyncio.gather(
            eng.replay_dead_letters(sub_id, 0.0, now, "admin"),
            eng.replay_dead_letters(sub_id, 0.0, now, "admin"),
        )
        # Deterministic: the dead letter was replayed by exactly one job.
        assert sorted([out1["replayed"], out2["replayed"]]) == [0, 1]
        assert out1["job_id"] != out2["job_id"]

        await wait_for_task_status(db, res["event_id"], "succeeded")
        # Exactly one replay round reached the receiver.
        assert len(rx.seen) == 2
        task = await db.fetchone(
            "SELECT replay_seq FROM delivery_tasks WHERE event_id=?", (res["event_id"],)
        )
        assert task["replay_seq"] == 1
        jobs = await db.fetchall(
            "SELECT replayed_count FROM replay_jobs ORDER BY created_at, id"
        )
        assert sorted(j["replayed_count"] for j in jobs) == [0, 1]
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_interleaved_with_normal_delivery(tmp_path):
    """A replay racing normal deliveries of newer events: both complete,
    neither is lost nor double-processed."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        r1 = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, r1["event_id"], "dead_letter")

        # Receiver recovers; a new event and the replay happen concurrently.
        rx.behavior.permanent = False
        r2 = await eng.enqueue_event("t", {"id": 2})
        out = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out["replayed"] == 1

        await wait_for_task_status(db, r1["event_id"], "succeeded")
        await wait_for_task_status(db, r2["event_id"], "succeeded")

        # Receiver processed: round 0 of event 1 (failed), replay round of
        # event 1, and event 2 -- each new delivery exactly once.
        assert len(rx.seen) == 3
        processed = [e for e in rx.received if not e.duplicate]
        by_event = {}
        for e in processed:
            by_event.setdefault(e.event_id, []).append(e.delivery_id)
        assert len(by_event[r1["event_id"]]) == 2  # original round + replay round
        assert len(by_event[r2["event_id"]]) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_bulk_replay_respects_concurrency_limits(tmp_path):
    """Many dead letters replayed at once flow through the normal scheduler:
    the per-subscription cap keeps them serial and everything terminates."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), global_concurrency=4, per_subscription_concurrency=1
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        n = 6
        ids = []
        for i in range(n):
            ids.append((await eng.enqueue_event("t", {"i": i}))["event_id"])
        for eid in ids:
            await wait_for_task_status(db, eid, "dead_letter")
        assert len(rx.seen) == n

        # Healthy but slow receiver; replay everything in one job.
        rx.behavior = ReceiverBehavior(delay=0.1)
        t0 = time.time()
        out = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out["replayed"] == n
        for eid in ids:
            await wait_for_task_status(db, eid, "succeeded")
        elapsed = time.time() - t0

        # Per-subscription cap is 1 and all tasks share one ordering key, so
        # the replayed deliveries are serialised instead of bursting.
        assert elapsed >= n * 0.09
        # Global in-flight never exceeded the configured cap.
        assert eng._inflight <= cfg.global_concurrency
        # Every event was processed exactly twice (one failed round + replay).
        assert len(rx.seen) == 2 * n
        for eid in ids:
            task = await db.fetchone(
                "SELECT replay_seq, status FROM delivery_tasks WHERE event_id=?", (eid,)
            )
            assert task["replay_seq"] == 1 and task["status"] == "succeeded"
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 4. Replay and restart recovery
# ---------------------------------------------------------------------------
def test_replay_survives_restart_before_delivery(tmp_path):
    """Replay is durable: replaying while the scheduler is down, then
    'restarting', still delivers the replayed event exactly once more."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(str(tmp_path))
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")
        await eng.stop()  # simulate shutdown right after dead-lettering

        # Replay is a pure DB operation and survives the stopped scheduler.
        rx.behavior.permanent = False
        out = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out["replayed"] == 1
        task = await db.fetchone(
            "SELECT status, replay_seq FROM delivery_tasks WHERE event_id=?",
            (res["event_id"],),
        )
        assert task["status"] == "pending" and task["replay_seq"] == 1

        # Restart: the pending replayed task is claimed and delivered.
        await eng.start()
        await wait_for_task_status(db, res["event_id"], "succeeded")
        assert len(rx.seen) == 2
        assert rx.received[-1].duplicate is False
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


def test_replay_round_recovers_from_crash_mid_delivery(tmp_path):
    """A replayed task stuck in_flight (crash) is reclaimed on startup and
    redelivered within the same round: the receiver dedups the redelivery."""

    async def scenario():
        eng, db, ks, inproc, http, cfg = await make_engine(
            str(tmp_path), claim_timeout=10_000
        )
        rx = Receiver("r1", ks, cfg, ReceiverBehavior(permanent=True))
        inproc.register("r1", rx)
        sub_id = await create_sub(db, "s1", "inprocess://r1", ["*"])
        await eng.start()

        res = await eng.enqueue_event("t", {"id": 1})
        await wait_for_task_status(db, res["event_id"], "dead_letter")

        rx.behavior.permanent = False
        out = await eng.replay_dead_letters(sub_id, 0.0, time.time(), "admin")
        assert out["replayed"] == 1

        # Simulate a crash mid-round: replayed task stuck in_flight.
        await eng.stop()
        await db.execute(
            "UPDATE delivery_tasks SET status='in_flight', claimed_at=? "
            "WHERE event_id=? AND subscription_id=?",
            (time.time() - 9999, res["event_id"], sub_id),
        )
        await eng.start()

        await wait_for_task_status(db, res["event_id"], "succeeded")

        task = await db.fetchone(
            "SELECT status, replay_seq FROM delivery_tasks "
            "WHERE event_id=? AND subscription_id=?",
            (res["event_id"], sub_id),
        )
        assert task["status"] == "succeeded"
        assert task["replay_seq"] == 1
        # The replayed round was processed exactly once for this subscription.
        replay_hits = [
            e for e in rx.received if e.delivery_id.endswith(":r1") and not e.duplicate
        ]
        assert len(replay_hits) == 1
        await eng.stop()
        await http.aclose()
        await db.close()

    run(scenario())


# ---------------------------------------------------------------------------
# 5. API level: permissions, audit trail, replay job traceability
# ---------------------------------------------------------------------------
def _wait_event_status(client, event_id, status, tries=200):
    for _ in range(tries):
        ev = client.get(f"/events/{event_id}").json()
        if ev["deliveries"] and ev["deliveries"][0]["status"] == status:
            return ev
        time.sleep(0.02)
    raise AssertionError(f"event {event_id} never reached {status!r}")


def test_replay_jobs_endpoint_and_audit_trail(tmp_path):
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
        _wait_event_status(client, event_id, "dead_letter")

        # Replay jobs endpoint is permission-checked.
        assert client.get("/admin/replay-jobs").status_code == 401
        assert client.get(
            "/admin/replay-jobs", headers={"X-API-Key": cfg.viewer_api_key}
        ).status_code == 200

        # Successful replay is audited and listed as a job.
        rx.behavior.permanent = False
        r = client.post(
            f"/admin/subscriptions/{sub_id}/replay",
            headers={"X-API-Key": cfg.admin_api_key},
        )
        assert r.status_code == 200 and r.json()["replayed"] == 1
        job_id = r.json()["job_id"]
        _wait_event_status(client, event_id, "succeeded")

        jobs = client.get(
            "/admin/replay-jobs", headers={"X-API-Key": cfg.viewer_api_key}
        ).json()
        assert len(jobs) == 1
        job = jobs[0]
        assert job["id"] == job_id
        assert job["subscription_id"] == sub_id
        assert job["status"] == "completed"
        assert job["replayed_count"] == 1
        assert job["created_by"] == "admin"

        # The dead-letter entry links back to the replay job.
        dl = client.get(
            "/admin/dead-letters", headers={"X-API-Key": cfg.viewer_api_key}
        ).json()[0]
        assert dl["last_replay_job_id"] == job_id
        assert dl["replay_count"] == 1

        # Audit log shows the allowed replay and the denied anonymous read.
        audit = client.get(
            "/admin/audit", headers={"X-API-Key": cfg.admin_api_key}
        ).json()
        entries = [(a["action"], a["decision"]) for a in audit]
        assert ("replay", "allowed") in entries
        assert ("replay_jobs:read", "denied") in entries
        replay_entry = next(
            a for a in audit if a["action"] == "replay" and a["decision"] == "allowed"
        )
        assert job_id in replay_entry["detail"]


def test_overlapping_replay_ranges_are_distinguishable(tmp_path):
    """Two replay jobs over overlapping time ranges produce two distinct,
    individually auditable jobs; only the first one replays the entry."""

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
        ev = _wait_event_status(client, event_id, "dead_letter")

        rx.behavior.permanent = False
        now = time.time()
        r1 = client.post(
            f"/admin/subscriptions/{sub_id}/replay?from_ts=0&to_ts={now}",
            headers={"X-API-Key": cfg.admin_api_key},
        ).json()
        # Overlapping range, issued immediately afterwards.
        r2 = client.post(
            f"/admin/subscriptions/{sub_id}/replay?from_ts=0&to_ts={time.time()}",
            headers={"X-API-Key": cfg.admin_api_key},
        ).json()
        assert r1["replayed"] == 1 and r2["replayed"] == 0
        assert r1["job_id"] != r2["job_id"]

        _wait_event_status(client, event_id, "succeeded")
        jobs = client.get(
            "/admin/replay-jobs", headers={"X-API-Key": cfg.admin_api_key}
        ).json()
        assert len(jobs) == 2
        assert sorted(j["replayed_count"] for j in jobs) == [0, 1]
        # The event went through exactly one replay round.
        ev = client.get(f"/events/{event_id}").json()
        assert ev["deliveries"][0]["replay_seq"] == 1
