"""Reliable delivery engine.

Guarantees implemented here:

* Events and their per-subscription delivery tasks are inserted in one
  transaction; a crash between commit and delivery leaves rows in
  ``pending`` / ``in_flight`` which are reclaimed on restart.
* Per-attempt results are classified as ``success``, ``retryable`` or
  ``permanent``; permanent failures and exhausted retries move the task to
  the dead-letter store.
* Ordering: for each ordering key only the oldest pending task is eligible,
  and it is claimable only when no task with the same key is in flight.  With
  the default ordering key (the subscription id) this gives per-subscription
  FIFO ordering.
* Isolation: a global concurrency semaphore plus a per-subscription in-flight
  cap prevent a single subscription from monopolising delivery capacity.
* Claim timeout: tasks stuck in ``in_flight`` past the claim timeout are
  reaped and re-delivered (redelivery is safe because receivers deduplicate).
* Replay: replaying a dead letter starts a new *delivery round*
  (``replay_seq``).  The wire delivery id is derived from the round, so a
  replayed event is processed by receivers as a new event, while retries
  inside a round keep the same id and are still deduplicated.  Re-dead-
  lettering upserts the dead-letter row, so an event can be replayed any
  number of times without errors or stuck states.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from .signing import (
    DELIVERY_ID_HEADER,
    EVENT_ID_HEADER,
    EVENT_TYPE_HEADER,
    sign_headers,
)
from .transport import DeliveryResult, HttpTransport, InProcessTransport, select_transport


# Due-task selection: oldest pending task per ordering key, only when that key
# has no in-flight task, and only while the subscription is under its
# in-flight cap.  This is the heart of ordering + isolation.
# rowid is used for FIFO ordering because the public id is a random UUID.
DUE_TASKS_SQL = """
SELECT d.* FROM delivery_tasks d
WHERE d.status='pending' AND d.next_attempt_at <= ?
  AND d.attempt_count < d.max_attempts
  AND NOT EXISTS (
    SELECT 1 FROM delivery_tasks f
    WHERE f.ordering_key = d.ordering_key AND f.status='in_flight'
  )
  AND d.rowid = (
    SELECT f2.rowid FROM delivery_tasks f2
    WHERE f2.ordering_key = d.ordering_key AND f2.status='pending'
    ORDER BY f2.rowid LIMIT 1
  )
  AND (
    SELECT COUNT(*) FROM delivery_tasks f3
    WHERE f3.subscription_id = d.subscription_id AND f3.status='in_flight'
  ) < ?
ORDER BY d.rowid
LIMIT ?
"""


@dataclass
class Backoff:
    base: float
    factor: float
    max: float
    jitter_ratio: float

    def delay(self, attempt: int) -> float:
        raw = self.base * (self.factor ** (attempt - 1))
        raw = min(raw, self.max)
        jitter = raw * self.jitter_ratio * (random.random() * 2 - 1)
        return max(0.0, raw + jitter)


class DeliveryEngine:
    def __init__(self, db, key_store, inproc: InProcessTransport, http: HttpTransport, config) -> None:
        self.db = db
        self.key_store = key_store
        self.inproc = inproc
        self.http = http
        self.config = config
        self.backoff = Backoff(
            config.backoff_base, config.backoff_factor, config.backoff_max, config.jitter_ratio,
        )
        self._global_sem = asyncio.Semaphore(config.global_concurrency)
        self._inflight = 0
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        # in-memory counters (durable counters live in SQLite)
        self.counters = {
            "delivered": 0,
            "retries": 0,
            "dead_lettered": 0,
            "reclaimed": 0,
            "interrupted": 0,
            "replayed": 0,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        # Every in-flight task is orphaned by definition when a process starts.
        await self._reclaim_orphans(all_inflight=True)
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------
    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001 - the scheduler must never die
                pass
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.config.scheduler_interval)
            except asyncio.TimeoutError:
                pass

    async def _reclaim_orphans(self, all_inflight: bool = False) -> int:
        """Reset in-flight tasks whose claim has timed out (or all on start)."""
        now = time.time()
        if all_inflight:
            rows = await self.db.fetchall("SELECT * FROM delivery_tasks WHERE status='in_flight'")
        else:
            cutoff = now - self.config.claim_timeout
            rows = await self.db.fetchall(
                "SELECT * FROM delivery_tasks WHERE status='in_flight' AND claimed_at < ?",
                (cutoff,),
            )
        for row in rows:
            await self._record_attempt(
                row["id"],
                row["replay_seq"],
                row["attempt_count"],
                DeliveryResult(outcome="interrupted", error="claim timed out / process restarted"),
            )
            await self.db.execute(
                "UPDATE delivery_tasks SET status='pending', claimed_at=NULL, next_attempt_at=?, "
                "updated_at=? WHERE id=? AND status='in_flight'",
                (now, now, row["id"]),
            )
            self.counters["reclaimed"] += 1
            self.counters["interrupted"] += 1
        return len(rows)

    async def _tick(self) -> None:
        await self._reclaim_orphans()

        # Tasks that exhausted their budget while pending go to dead letter.
        exhausted = await self.db.fetchall(
            "SELECT * FROM delivery_tasks WHERE status='pending' AND attempt_count >= max_attempts"
        )
        for task in exhausted:
            await self._dead_letter(
                task, "retries_exhausted",
                DeliveryResult(outcome="retryable", error="max attempts exceeded"),
            )

        free = self.config.global_concurrency - self._inflight
        if free <= 0:
            return
        due = await self.db.fetchall(
            DUE_TASKS_SQL,
            (time.time(), self.config.per_subscription_concurrency, free),
        )
        for task in due:
            if self._inflight >= self.config.global_concurrency:
                break
            if await self._claim(task):
                asyncio.create_task(self._deliver_guarded(task))

    async def _claim(self, task: dict) -> bool:
        now = time.time()
        async with self.db.transaction():
            cur = await self.db._execute(
                "UPDATE delivery_tasks SET status='in_flight', claimed_at=?, "
                "attempt_count=attempt_count+1, updated_at=? "
                "WHERE id=? AND status='pending'",
                (now, now, task["id"]),
            )
            if cur == 0:
                return False
        return True

    async def _deliver_guarded(self, task: dict) -> None:
        async with self._global_sem:
            self._inflight += 1
            try:
                await self._attempt(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never lose a task to an unexpected error
                await self._finish_retryable(
                    task, DeliveryResult(outcome="retryable", error=f"engine error: {exc}"),
                )
            finally:
                self._inflight -= 1

    @staticmethod
    def _wire_delivery_id(task) -> str:
        """Delivery id sent to the receiver for the task's current round.

        Round 0 (original delivery) uses the bare task id.  Each replay starts
        a new round (``replay_seq`` is incremented) and derives a fresh id, so
        the receiver treats a replayed delivery as a *new* event instead of
        deduplicating it against the dead-lettered round.  Retries and
        crash-recovery redeliveries stay within the same round and keep the
        same id, so receivers still deduplicate them.
        """
        replay_seq = task["replay_seq"]
        if replay_seq == 0:
            return task["id"]
        return f"{task['id']}:r{replay_seq}"

    async def _attempt(self, task: dict) -> None:
        wire_delivery_id = self._wire_delivery_id(task)
        sub = await self.db.fetchone(
            "SELECT * FROM subscriptions WHERE id=?", (task["subscription_id"],)
        )
        event = await self.db.fetchone("SELECT * FROM events WHERE id=?", (task["event_id"],))
        if sub is None or event is None:
            # Should be impossible; treat as permanent to avoid spinning.
            await self._finish_permanent(
                task, DeliveryResult(outcome="permanent", error="subscription or event vanished"),
            )
            return

        key = await self.key_store.get_active()
        timestamp = int(time.time())
        body = event["payload"].encode("utf-8")
        headers = sign_headers(key.secret, key.key_id, timestamp, body)
        headers[DELIVERY_ID_HEADER] = wire_delivery_id
        headers[EVENT_ID_HEADER] = event["id"]
        headers[EVENT_TYPE_HEADER] = event["event_type"]

        transport = select_transport(sub["target"], self.inproc, self.http)
        result = await transport.deliver(
            sub["target"], headers, body, timeout=self.config.delivery_timeout,
        )

        # attempt_count was incremented atomically at claim time.
        attempt_number = task["attempt_count"] + 1
        await self._record_attempt(task["id"], task["replay_seq"], attempt_number, result)
        if result.outcome == "success":
            await self._finish_success(task, result)
        elif result.outcome == "permanent":
            await self._finish_permanent(task, result)
        else:
            await self._finish_retryable(task, result)

    # ------------------------------------------------------------------
    # Attempt outcome handling
    # ------------------------------------------------------------------
    async def _record_attempt(
        self, delivery_id: str, replay_seq: int, attempt_number: int, result: DeliveryResult
    ) -> None:
        now = time.time()
        await self.db.execute(
            "INSERT INTO delivery_attempts "
            "(delivery_id, replay_seq, attempt_number, result, status_code, error, "
            "latency_ms, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                delivery_id, replay_seq, attempt_number, result.outcome, result.status_code,
                result.error, result.latency_ms, now - max(result.latency_ms, 0) / 1000.0, now,
            ),
        )

    async def _finish_success(self, task: dict, result: DeliveryResult) -> None:
        now = time.time()
        async with self.db.transaction():
            cur = await self.db._execute(
                "UPDATE delivery_tasks SET status='succeeded', claimed_at=NULL, updated_at=? "
                "WHERE id=? AND status='in_flight'",
                (now, task["id"]),
            )
            if cur:
                await self.db._execute(
                    "UPDATE events SET status='delivered' WHERE id=?",
                    (task["event_id"],),
                )
        self.counters["delivered"] += 1

    async def _finish_retryable(self, task: dict, result: DeliveryResult) -> None:
        now = time.time()
        row = await self.db.fetchone(
            "SELECT attempt_count, max_attempts FROM delivery_tasks WHERE id=?", (task["id"],)
        )
        if row is None:
            return
        if row["attempt_count"] >= row["max_attempts"]:
            await self._dead_letter(task, "retries_exhausted", result)
            return
        delay = self.backoff.delay(row["attempt_count"])
        async with self.db.transaction():
            await self.db._execute(
                "UPDATE delivery_tasks SET status='pending', next_attempt_at=?, claimed_at=NULL, "
                "last_error=?, updated_at=? WHERE id=? AND status='in_flight'",
                (now + delay, result.error, now, task["id"]),
            )
        self.counters["retries"] += 1

    async def _finish_permanent(self, task: dict, result: DeliveryResult) -> None:
        await self._dead_letter(task, "permanent_failure", result)

    async def _dead_letter(self, task: dict, reason: str, result: DeliveryResult) -> None:
        now = time.time()
        fresh = await self.db.fetchone(
            "SELECT attempt_count, replay_seq FROM delivery_tasks WHERE id=?", (task["id"],)
        )
        attempt_count = fresh["attempt_count"] if fresh else task["attempt_count"]
        replay_seq = fresh["replay_seq"] if fresh else task["replay_seq"]
        # Snapshot the attempts of the round that just failed; the full
        # cross-round history stays queryable in delivery_attempts.
        attempts = await self.db.fetchall(
            "SELECT replay_seq, attempt_number, result, status_code, error, latency_ms, "
            "started_at, finished_at FROM delivery_attempts "
            "WHERE delivery_id=? AND replay_seq=? ORDER BY attempt_number",
            (task["id"], replay_seq),
        )
        attempts_json = json.dumps([dict(a) for a in attempts])
        async with self.db.transaction():
            cur = await self.db._execute(
                "UPDATE delivery_tasks SET status='dead_letter', dead_letter_reason=?, claimed_at=NULL, "
                "updated_at=? WHERE id=? AND status!='dead_letter'",
                (reason, now, task["id"]),
            )
            if cur:
                # Upsert: a task may be replayed and dead-letter again any
                # number of times.  The row tracks the *latest* dead-lettering
                # (and becomes replayable again via replayed_at=NULL) while
                # replay_count keeps accumulating across rounds.
                await self.db._execute(
                    "INSERT INTO dead_letters "
                    "(id, delivery_id, event_id, subscription_id, reason, last_error, attempt_count, "
                    "attempts_json, dead_lettered_at, replayed_at, replay_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0) "
                    "ON CONFLICT(delivery_id) DO UPDATE SET "
                    "reason=excluded.reason, last_error=excluded.last_error, "
                    "attempt_count=excluded.attempt_count, attempts_json=excluded.attempts_json, "
                    "dead_lettered_at=excluded.dead_lettered_at, replayed_at=NULL",
                    (
                        uuid.uuid4().hex, task["id"], task["event_id"], task["subscription_id"],
                        reason, result.error, attempt_count, attempts_json, now,
                    ),
                )
        self.counters["dead_lettered"] += 1

    # ------------------------------------------------------------------
    # Public operations used by the API layer
    # ------------------------------------------------------------------
    async def enqueue_event(self, event_type: str, payload: dict, ordering_key: Optional[str] = None) -> dict:
        """Persist the event and one delivery task per matching subscription.

        The event and all its delivery tasks are written in a single
        transaction so they are never observed apart.
        """
        now = time.time()
        event_id = uuid.uuid4().hex
        payload_str = json.dumps(payload, separators=(",", ":"))

        subs = await self.db.fetchall(
            "SELECT * FROM subscriptions WHERE active=1 ORDER BY created_at"
        )
        matching = [
            s for s in subs
            if event_type in json.loads(s["event_types"]) or "*" in json.loads(s["event_types"])
        ]

        async with self.db.transaction():
            await self.db._execute(
                "INSERT INTO events (id, event_type, payload, ordering_key, status, created_at) "
                "VALUES (?, ?, ?, ?, 'pending', ?)",
                (event_id, event_type, payload_str, ordering_key, now),
            )
            for sub in matching:
                await self.db._execute(
                    "INSERT INTO delivery_tasks "
                    "(id, event_id, subscription_id, ordering_key, status, attempt_count, max_attempts, "
                    "next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)",
                    (
                        uuid.uuid4().hex, event_id, sub["id"],
                        ordering_key or sub["id"],
                        self.config.max_attempts, now, now, now,
                    ),
                )
        return {"event_id": event_id, "matched_subscriptions": [s["id"] for s in matching]}

    async def replay_dead_letters(
        self, subscription_id: str, from_ts: float, to_ts: float, actor: str
    ) -> dict:
        """Replay dead letters for a subscription in a time range.

        Each replayed task starts a **new delivery round**: ``replay_seq`` is
        incremented, the attempt budget is reset (history is preserved in
        ``delivery_attempts``), and the task goes back to ``pending`` so the
        normal scheduler claims it under the usual concurrency limits.  The
        new round derives a fresh wire delivery id, so receivers process the
        replay as a new event instead of deduplicating it.

        The selection and all updates happen in a single ``BEGIN IMMEDIATE``
        transaction and every row is transitioned with a conditional UPDATE,
        so concurrent replay jobs (overlapping ranges or the same event) are
        deterministic: each dead-letter entry is replayed by exactly one job,
        and a job's ``replayed`` count reflects only the rows it actually
        transitioned.  A task that dead-letters again becomes replayable again
        (its ``dead_letters`` row is reset to ``replayed_at=NULL``).
        """
        now = time.time()
        job_id = uuid.uuid4().hex
        replayed_ids: list[str] = []
        async with self.db.transaction():
            rows = await self.db._fetchall(
                "SELECT delivery_id FROM dead_letters WHERE subscription_id=? "
                "AND dead_lettered_at >= ? AND dead_lettered_at <= ? AND replayed_at IS NULL "
                "ORDER BY dead_lettered_at",
                (subscription_id, from_ts, to_ts),
            )
            for row in rows:
                cur = await self.db._execute(
                    "UPDATE delivery_tasks SET status='pending', attempt_count=0, "
                    "replay_seq=replay_seq+1, next_attempt_at=?, claimed_at=NULL, "
                    "dead_letter_reason=NULL, last_error=NULL, updated_at=? "
                    "WHERE id=? AND status='dead_letter'",
                    (now, now, row["delivery_id"]),
                )
                if not cur:
                    # Task is not in dead_letter state (e.g. a concurrent job
                    # already replayed it): skip without counting it.
                    continue
                await self.db._execute(
                    "UPDATE dead_letters SET replayed_at=?, replay_count=replay_count+1, "
                    "last_replay_job_id=? WHERE delivery_id=? AND replayed_at IS NULL",
                    (now, job_id, row["delivery_id"]),
                )
                replayed_ids.append(row["delivery_id"])
            await self.db._execute(
                "INSERT INTO replay_jobs "
                "(id, subscription_id, from_ts, to_ts, status, replayed_count, created_by, created_at) "
                "VALUES (?, ?, ?, ?, 'completed', ?, ?, ?)",
                (job_id, subscription_id, from_ts, to_ts, len(replayed_ids), actor, now),
            )
        self.counters["replayed"] += len(replayed_ids)
        return {"job_id": job_id, "replayed": len(replayed_ids)}

    async def metrics(self) -> dict:
        now = time.time()
        status_rows = await self.db.fetchall(
            "SELECT status, COUNT(*) AS n FROM delivery_tasks GROUP BY status"
        )
        status_counts = {r["status"]: r["n"] for r in status_rows}
        per_sub_rows = await self.db.fetchall(
            "SELECT subscription_id, status, COUNT(*) AS n FROM delivery_tasks "
            "GROUP BY subscription_id, status"
        )
        per_sub: dict[str, dict[str, int]] = {}
        for r in per_sub_rows:
            per_sub.setdefault(r["subscription_id"], {})[r["status"]] = r["n"]
        oldest = await self.db.fetchone(
            "SELECT MIN(created_at) AS m FROM delivery_tasks WHERE status='pending'"
        )
        oldest_age = (now - oldest["m"]) if oldest and oldest["m"] else 0.0
        dl = await self.db.fetchone("SELECT COUNT(*) AS n FROM dead_letters")
        return {
            "backlog": {
                "pending": status_counts.get("pending", 0),
                "in_flight": status_counts.get("in_flight", 0),
                "oldest_pending_age_s": round(oldest_age, 3),
            },
            "counters": dict(self.counters),
            "dead_letter_total": dl["n"] if dl else 0,
            "succeeded_total": status_counts.get("succeeded", 0),
            "per_subscription": per_sub,
            "limits": {
                "global_concurrency": self.config.global_concurrency,
                "per_subscription_concurrency": self.config.per_subscription_concurrency,
            },
            "inflight": self._inflight,
            "saturated": self._inflight >= self.config.global_concurrency or oldest_age > 30.0,
        }
