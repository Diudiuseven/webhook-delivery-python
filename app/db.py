"""SQLite persistence wrapper.

sqlite3 is opened in autocommit mode (``isolation_level=None``); explicit
transactions are started with ``BEGIN IMMEDIATE`` via :class:`Transaction`.
All access is serialised through an asyncio lock and executed in worker
threads so the event loop is never blocked.
"""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, Iterable, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    ordering_key TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    target TEXT NOT NULL,
    event_types TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_tasks (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
    ordering_key TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    replay_seq INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    claimed_at REAL,
    last_error TEXT,
    dead_letter_reason TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(event_id, subscription_id)
);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON delivery_tasks(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_tasks_sub ON delivery_tasks(subscription_id, status);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL REFERENCES delivery_tasks(id),
    replay_seq INTEGER NOT NULL DEFAULT 0,
    attempt_number INTEGER NOT NULL,
    result TEXT NOT NULL,
    status_code INTEGER,
    error TEXT,
    latency_ms REAL,
    started_at REAL NOT NULL,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON delivery_attempts(delivery_id);

CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE REFERENCES delivery_tasks(id),
    event_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    last_error TEXT,
    attempt_count INTEGER NOT NULL,
    attempts_json TEXT NOT NULL,
    dead_lettered_at REAL NOT NULL,
    replayed_at REAL,
    replay_count INTEGER NOT NULL DEFAULT 0,
    last_replay_job_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_sub ON dead_letters(subscription_id, dead_lettered_at);

CREATE TABLE IF NOT EXISTS signing_keys (
    key_id TEXT PRIMARY KEY,
    secret TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    rotated_at REAL
);

CREATE TABLE IF NOT EXISTS replay_jobs (
    id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    from_ts REAL NOT NULL,
    to_ts REAL NOT NULL,
    status TEXT NOT NULL,
    replayed_count INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT,
    decision TEXT NOT NULL,
    detail TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS principals (
    api_key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL
);
"""

# Columns added after the initial schema; applied idempotently to databases
# created by older versions so upgrades do not require a rebuild.
_MIGRATIONS = (
    ("delivery_tasks", "replay_seq", "replay_seq INTEGER NOT NULL DEFAULT 0"),
    ("delivery_attempts", "replay_seq", "replay_seq INTEGER NOT NULL DEFAULT 0"),
    ("dead_letters", "last_replay_job_id", "last_replay_job_id TEXT"),
)


def _apply_migrations(conn: sqlite3.Connection) -> None:
    for table, column, ddl in _MIGRATIONS:
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


class Transaction:
    """Context manager grouping multiple statements into one atomic commit."""

    def __init__(self, db: "Database") -> None:
        self.db = db

    async def __aenter__(self) -> "Transaction":
        await self.db._lock.acquire()
        try:
            await self.db._execute("BEGIN IMMEDIATE")
        except Exception:
            self.db._lock.release()
            raise
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                await self.db._execute("COMMIT")
            else:
                await self.db._execute("ROLLBACK")
        finally:
            self.db._lock.release()


class Database:
    """Async-friendly sqlite3 wrapper with a serialised access lock."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        def _open() -> sqlite3.Connection:
            conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(SCHEMA)
            _apply_migrations(conn)
            return conn

        self._conn = await asyncio.to_thread(_open)

    async def close(self) -> None:
        if self._conn is not None:
            conn = self._conn
            self._conn = None
            await asyncio.to_thread(conn.close)

    def transaction(self) -> Transaction:
        return Transaction(self)

    # -- low-level helpers (caller must hold the lock inside transactions) --
    async def _execute(self, sql: str, params: Iterable[Any] = ()) -> int:
        assert self._conn is not None
        cur = await asyncio.to_thread(self._conn.execute, sql, list(params))
        return cur.rowcount

    async def _fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        assert self._conn is not None

        def _run() -> Optional[sqlite3.Row]:
            return self._conn.execute(sql, list(params)).fetchone()

        return await asyncio.to_thread(_run)

    async def _fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        assert self._conn is not None

        def _run() -> list[sqlite3.Row]:
            return list(self._conn.execute(sql, list(params)).fetchall())

        return await asyncio.to_thread(_run)

    # -- public API (acquires the lock) -------------------------------------
    async def execute(self, sql: str, params: Iterable[Any] = ()) -> int:
        async with self._lock:
            return await self._execute(sql, params)

    async def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> int:
        assert self._conn is not None
        async with self._lock:
            def _run() -> int:
                cur = self._conn.executemany(sql, list(seq))
                return cur.rowcount

            return await asyncio.to_thread(_run)

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        async with self._lock:
            return await self._fetchone(sql, params)

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        async with self._lock:
            return await self._fetchall(sql, params)
