"""Shared helpers for the delivery test suites."""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

from app.config import Config
from app.db import Database
from app.engine import DeliveryEngine
from app.signing import KeyStore
from app.transport import HttpTransport, InProcessTransport


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


async def wait_for_task_status(db: Database, event_id: str, status: str, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = await db.fetchone(
            "SELECT status FROM delivery_tasks WHERE event_id=?", (event_id,)
        )
        if row and row["status"] == status:
            return row
        await asyncio.sleep(0.02)
    raise AssertionError(f"event {event_id} never reached status {status!r}")


def run(coro):
    return asyncio.run(coro)
