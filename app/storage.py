"""SQLite 持久化层：事件、订阅、投递、尝试记录、死信、密钥、审计。

关键保证：
- enqueue_event 在单个事务内写入事件与所有匹配订阅的投递任务，
  提交与投递之间崩溃不会丢事件（重启后 pending 事件会被重新领取）。
- 同一订阅的投递通过 (subscription_id, seq) 单调序号保证按入队顺序投递：
  只有不存在更早的非终态投递时，一条投递才可被领取。
- 领取使用租约（claim_token + claim_expires_at），超时未完成可被重新领取。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from typing import Optional

from .models import (
    AuditRecord,
    Attempt,
    AttemptOutcome,
    Delivery,
    DeliveryStatus,
    Event,
    SigningKey,
    Subscription,
    new_id,
    now,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    event_types TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sub_seq (
    subscription_id TEXT PRIMARY KEY,
    next_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
    seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    claim_token TEXT,
    claim_expires_at REAL NOT NULL DEFAULT 0,
    replay_of TEXT,
    created_at REAL NOT NULL,
    UNIQUE (subscription_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_sub ON deliveries(subscription_id, seq);
CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES deliveries(id),
    attempt_no INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    status_code INTEGER,
    error TEXT,
    duration_ms REAL NOT NULL,
    key_id TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON attempts(delivery_id);
CREATE TABLE IF NOT EXISTS dead_letters (
    delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id),
    reason TEXT NOT NULL,
    dead_at REAL NOT NULL,
    replayed_at REAL
);
CREATE TABLE IF NOT EXISTS signing_keys (
    id TEXT PRIMARY KEY,
    secret TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    valid_until REAL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS audits (
    id TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    allowed INTEGER NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);
"""

TERMINAL = (DeliveryStatus.DELIVERED.value, DeliveryStatus.DEAD.value)


def _new_token() -> str:
    return uuid.uuid4().hex


class Storage:
    """线程安全的 SQLite 存储封装（单连接 + 可重入锁，事务显式控制）。"""

    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    # ---- 生命周期 ----
    def connect(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(SCHEMA)
            self._conn = conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "Storage":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "storage not connected"
        return self._conn

    # ---- 行映射 ----
    @staticmethod
    def _to_subscription(r: sqlite3.Row) -> Subscription:
        return Subscription(id=r["id"], url=r["url"], event_types=json.loads(r["event_types"]),
                            max_concurrency=r["max_concurrency"], created_at=r["created_at"])

    @staticmethod
    def _to_delivery(r: sqlite3.Row) -> Delivery:
        return Delivery(id=r["id"], event_id=r["event_id"], subscription_id=r["subscription_id"],
                        seq=r["seq"], status=DeliveryStatus(r["status"]), attempt_count=r["attempt_count"],
                        next_attempt_at=r["next_attempt_at"], claim_token=r["claim_token"],
                        claim_expires_at=r["claim_expires_at"], replay_of=r["replay_of"],
                        created_at=r["created_at"])

    @staticmethod
    def _to_attempt(r: sqlite3.Row) -> Attempt:
        return Attempt(id=r["id"], delivery_id=r["delivery_id"], attempt_no=r["attempt_no"],
                       outcome=AttemptOutcome(r["outcome"]), status_code=r["status_code"],
                       error=r["error"], duration_ms=r["duration_ms"], key_id=r["key_id"],
                       created_at=r["created_at"])

    @staticmethod
    def _to_key(r: sqlite3.Row) -> SigningKey:
        return SigningKey(id=r["id"], secret=r["secret"], active=bool(r["active"]),
                          valid_until=r["valid_until"], created_at=r["created_at"])

    @staticmethod
    def _to_audit(r: sqlite3.Row) -> AuditRecord:
        return AuditRecord(id=r["id"], actor=r["actor"], action=r["action"], target=r["target"],
                           allowed=bool(r["allowed"]), detail=r["detail"], created_at=r["created_at"])

    # ---- 订阅 ----
    def add_subscription(self, sub: Subscription) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO subscriptions(id,url,event_types,max_concurrency,created_at) VALUES(?,?,?,?,?)",
                (sub.id, sub.url, json.dumps(sub.event_types), sub.max_concurrency, sub.created_at))
            self.conn.execute(
                "INSERT INTO sub_seq(subscription_id,next_seq) VALUES(?,0) "
                "ON CONFLICT(subscription_id) DO NOTHING", (sub.id,))

    def list_subscriptions(self) -> list[Subscription]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM subscriptions ORDER BY created_at").fetchall()
            return [self._to_subscription(r) for r in rows]

    def get_subscription(self, sub_id: str) -> Optional[Subscription]:
        with self._lock:
            r = self.conn.execute("SELECT * FROM subscriptions WHERE id=?", (sub_id,)).fetchone()
            return self._to_subscription(r) if r else None

    # ---- 事件入队：事件 + 匹配订阅的投递任务在同一事务 ----
    def enqueue_event(self, event: Event) -> list[Delivery]:
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("INSERT INTO events(id,event_type,payload,created_at) VALUES(?,?,?,?)",
                             (event.id, event.event_type, json.dumps(event.payload), event.created_at))
                subs = conn.execute("SELECT * FROM subscriptions").fetchall()
                deliveries: list[Delivery] = []
                ts = now()
                for r in subs:
                    types = json.loads(r["event_types"])
                    if "*" not in types and event.event_type not in types:
                        continue
                    seq = self._next_seq(conn, r["id"])
                    d = Delivery(id=new_id(), event_id=event.id, subscription_id=r["id"], seq=seq,
                                 status=DeliveryStatus.PENDING, next_attempt_at=ts, created_at=ts)
                    conn.execute(
                        "INSERT INTO deliveries(id,event_id,subscription_id,seq,status,attempt_count,"
                        "next_attempt_at,claim_token,claim_expires_at,replay_of,created_at) "
                        "VALUES(?,?,?,?,?,0,?,NULL,0,NULL,?)",
                        (d.id, d.event_id, d.subscription_id, d.seq, d.status.value,
                         d.next_attempt_at, d.created_at))
                    deliveries.append(d)
                conn.execute("COMMIT")
                return deliveries
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _next_seq(conn: sqlite3.Connection, sub_id: str) -> int:
        row = conn.execute(
            "UPDATE sub_seq SET next_seq = next_seq + 1 WHERE subscription_id=? RETURNING next_seq",
            (sub_id,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO sub_seq(subscription_id,next_seq) VALUES(?,1)", (sub_id,))
            return 0
        return row["next_seq"] - 1

    # ---- 投递状态机 ----
    def claim_due_deliveries(self, limit: int, lease_seconds: float) -> list[Delivery]:
        """领取到期投递。顺序门控：订阅内不存在更早的非终态投递才可领取。"""
        ts = now()
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    """
                    SELECT d.* FROM deliveries d
                    WHERE d.status = ? AND d.next_attempt_at <= ?
                      AND NOT EXISTS (
                          SELECT 1 FROM deliveries e
                          WHERE e.subscription_id = d.subscription_id
                            AND e.seq < d.seq
                            AND e.status IN (?, ?)
                      )
                    ORDER BY d.subscription_id, d.seq
                    LIMIT ?
                    """,
                    (DeliveryStatus.PENDING.value, ts,
                     DeliveryStatus.PENDING.value, DeliveryStatus.CLAIMED.value, limit)).fetchall()
                claimed: list[Delivery] = []
                for r in rows:
                    token = _new_token()
                    expires = ts + lease_seconds
                    cur = conn.execute(
                        "UPDATE deliveries SET status=?, claim_token=?, claim_expires_at=? "
                        "WHERE id=? AND status=?",
                        (DeliveryStatus.CLAIMED.value, token, expires, r["id"],
                         DeliveryStatus.PENDING.value))
                    if cur.rowcount == 1:
                        d = self._to_delivery(r)
                        d.status = DeliveryStatus.CLAIMED
                        d.claim_token = token
                        d.claim_expires_at = expires
                        claimed.append(d)
                conn.execute("COMMIT")
                return claimed
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _finish_attempt(self, delivery_id: str, claim_token: str, attempt: Attempt,
                        new_status: DeliveryStatus, next_attempt_at: float) -> bool:
        """在事务内：校验租约、写入尝试记录、更新投递状态。"""
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
                if row is None or row["status"] != DeliveryStatus.CLAIMED.value \
                        or row["claim_token"] != claim_token:
                    conn.execute("ROLLBACK")
                    return False
                conn.execute(
                    "INSERT INTO attempts(id,delivery_id,attempt_no,outcome,status_code,error,"
                    "duration_ms,key_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (attempt.id, delivery_id, attempt.attempt_no, attempt.outcome.value,
                     attempt.status_code, attempt.error, attempt.duration_ms, attempt.key_id,
                     attempt.created_at))
                conn.execute(
                    "UPDATE deliveries SET status=?, attempt_count=attempt_count+1, "
                    "next_attempt_at=?, claim_token=NULL, claim_expires_at=0 WHERE id=?",
                    (new_status.value, next_attempt_at, delivery_id))
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def complete_delivery(self, delivery_id: str, claim_token: str, attempt: Attempt) -> bool:
        return self._finish_attempt(delivery_id, claim_token, attempt,
                                    DeliveryStatus.DELIVERED, next_attempt_at=0.0)

    def reschedule_delivery(self, delivery_id: str, claim_token: str, attempt: Attempt,
                            next_attempt_at: float) -> bool:
        return self._finish_attempt(delivery_id, claim_token, attempt,
                                    DeliveryStatus.PENDING, next_attempt_at)

    def dead_letter_delivery(self, delivery_id: str, claim_token: str, attempt: Attempt,
                             reason: str) -> bool:
        with self._lock:
            ok = self._finish_attempt(delivery_id, claim_token, attempt,
                                      DeliveryStatus.DEAD, next_attempt_at=0.0)
            if ok:
                self.conn.execute(
                    "INSERT INTO dead_letters(delivery_id,reason,dead_at,replayed_at) VALUES(?,?,?,NULL)",
                    (delivery_id, reason, now()))
            return ok

    def release_expired_claims(self) -> int:
        """租约超时未完成的投递重新变为可领取。"""
        ts = now()
        with self._lock:
            cur = self.conn.execute(
                "UPDATE deliveries SET status=?, claim_token=NULL, claim_expires_at=0 "
                "WHERE status=? AND claim_expires_at < ?",
                (DeliveryStatus.PENDING.value, DeliveryStatus.CLAIMED.value, ts))
            return cur.rowcount

    def reset_claims_on_startup(self) -> int:
        """进程重启：所有 claimed 状态（持有者在崩溃中消失）回到 pending。"""
        with self._lock:
            cur = self.conn.execute(
                "UPDATE deliveries SET status=?, claim_token=NULL, claim_expires_at=0 WHERE status=?",
                (DeliveryStatus.PENDING.value, DeliveryStatus.CLAIMED.value))
            return cur.rowcount

    # ---- 查询 ----
    def get_delivery(self, delivery_id: str) -> Optional[Delivery]:
        with self._lock:
            r = self.conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            return self._to_delivery(r) if r else None

    def get_event(self, event_id: str) -> Optional[Event]:
        with self._lock:
            r = self.conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if not r:
                return None
            return Event(id=r["id"], event_type=r["event_type"],
                         payload=json.loads(r["payload"]), created_at=r["created_at"])

    def list_deliveries(self, subscription_id: Optional[str] = None,
                        status: Optional[DeliveryStatus] = None) -> list[Delivery]:
        with self._lock:
            q, args = "SELECT * FROM deliveries WHERE 1=1", []
            if subscription_id:
                q += " AND subscription_id=?"
                args.append(subscription_id)
            if status:
                q += " AND status=?"
                args.append(status.value)
            q += " ORDER BY subscription_id, seq"
            return [self._to_delivery(r) for r in self.conn.execute(q, args).fetchall()]

    def list_attempts(self, delivery_id: str) -> list[Attempt]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM attempts WHERE delivery_id=? ORDER BY attempt_no", (delivery_id,)).fetchall()
            return [self._to_attempt(r) for r in rows]

    def list_dead_letters(self, subscription_id: Optional[str] = None, since: float = 0.0,
                          until: float = float("inf")) -> list[dict]:
        with self._lock:
            q = """
                SELECT dl.delivery_id, dl.reason, dl.dead_at, dl.replayed_at,
                       d.event_id, d.subscription_id, d.seq, d.attempt_count
                FROM dead_letters dl JOIN deliveries d ON d.id = dl.delivery_id
                WHERE dl.dead_at >= ? AND dl.dead_at <= ?
                """
            args: list = [since, until]
            if subscription_id:
                q += " AND d.subscription_id=?"
                args.append(subscription_id)
            q += " ORDER BY dl.dead_at"
            return [dict(r) for r in self.conn.execute(q, args).fetchall()]

    def backlog_metrics(self) -> dict:
        """积压指标：按状态计数、按订阅的待投递数与最老待投递年龄。"""
        ts = now()
        with self._lock:
            by_status = {r["status"]: r["n"] for r in self.conn.execute(
                "SELECT status, COUNT(*) AS n FROM deliveries GROUP BY status").fetchall()}
            per_sub = [dict(r) for r in self.conn.execute(
                "SELECT subscription_id, COUNT(*) AS pending, "
                "MIN(next_attempt_at) AS oldest_due_at FROM deliveries "
                "WHERE status=? GROUP BY subscription_id",
                (DeliveryStatus.PENDING.value,)).fetchall()]
            for row in per_sub:
                row["oldest_age_seconds"] = max(0.0, ts - row.pop("oldest_due_at"))
            return {"by_status": by_status, "per_subscription": per_sub, "now": ts}

    # ---- 死信重放 ----
    def replay_dead_letters(self, delivery_ids: list[str]) -> list[Delivery]:
        """为死信创建新投递（attempt_count 归零、新 seq），保持原事件与订阅。

        与正常投递并发时共用同一事务与序号分配，结果确定。
        """
        ts = now()
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                replays: list[Delivery] = []
                for did in delivery_ids:
                    row = conn.execute(
                        "SELECT d.*, dl.replayed_at IS NULL AS not_replayed "
                        "FROM deliveries d JOIN dead_letters dl ON dl.delivery_id=d.id "
                        "WHERE d.id=? AND d.status=?", (did, DeliveryStatus.DEAD.value)).fetchone()
                    if row is None:
                        continue
                    seq = self._next_seq(conn, row["subscription_id"])
                    d = Delivery(id=new_id(), event_id=row["event_id"],
                                 subscription_id=row["subscription_id"], seq=seq,
                                 status=DeliveryStatus.PENDING, next_attempt_at=ts,
                                 replay_of=did, created_at=ts)
                    conn.execute(
                        "INSERT INTO deliveries(id,event_id,subscription_id,seq,status,attempt_count,"
                        "next_attempt_at,claim_token,claim_expires_at,replay_of,created_at) "
                        "VALUES(?,?,?,?,?,0,?,NULL,0,?,?)",
                        (d.id, d.event_id, d.subscription_id, d.seq, d.status.value,
                         d.next_attempt_at, d.replay_of, d.created_at))
                    conn.execute("UPDATE dead_letters SET replayed_at=? WHERE delivery_id=?",
                                 (ts, did))
                    replays.append(d)
                conn.execute("COMMIT")
                return replays
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ---- 签名密钥 ----
    def add_signing_key(self, key: SigningKey) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO signing_keys(id,secret,active,valid_until,created_at) VALUES(?,?,?,?,?)",
                (key.id, key.secret, int(key.active), key.valid_until, key.created_at))

    def get_active_key(self) -> Optional[SigningKey]:
        with self._lock:
            r = self.conn.execute(
                "SELECT * FROM signing_keys WHERE active=1 ORDER BY created_at DESC LIMIT 1").fetchone()
            return self._to_key(r) if r else None

    def list_valid_keys(self) -> list[SigningKey]:
        ts = now()
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM signing_keys WHERE valid_until IS NULL OR valid_until > ? "
                "ORDER BY created_at", (ts,)).fetchall()
            return [self._to_key(r) for r in rows]

    def rotate_key(self, new_key: SigningKey, old_key_valid_until: float) -> None:
        """轮换：旧密钥保留 valid_until 过渡期用于验签，新密钥成为唯一签名密钥。"""
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("UPDATE signing_keys SET active=0, valid_until=? WHERE active=1",
                             (old_key_valid_until,))
                conn.execute(
                    "INSERT INTO signing_keys(id,secret,active,valid_until,created_at) VALUES(?,?,1,NULL,?)",
                    (new_key.id, new_key.secret, new_key.created_at))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ---- 审计 ----
    def add_audit(self, record: AuditRecord) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO audits(id,actor,action,target,allowed,detail,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (record.id, record.actor, record.action, record.target,
                 int(record.allowed), record.detail, record.created_at))

    def list_audits(self) -> list[AuditRecord]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audits ORDER BY created_at").fetchall()
            return [self._to_audit(r) for r in rows]
