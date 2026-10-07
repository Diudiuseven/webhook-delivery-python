"""HTTP API for event production, delivery inspection and administration."""
from __future__ import annotations

import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from .config import Config
from .db import Database
from .engine import DeliveryEngine
from .signing import KeyStore
from .transport import HttpTransport, InProcessTransport


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class EventIn(BaseModel):
    event_type: str
    payload: dict
    ordering_key: Optional[str] = None


class SubscriptionIn(BaseModel):
    name: str
    target: str
    event_types: list[str]


# ---------------------------------------------------------------------------
# Audit / auth helpers
# ---------------------------------------------------------------------------
async def _audit(db, actor: str, role: str, action: str, resource: str,
                decision: str, detail: str = "") -> None:
    await db.execute(
        "INSERT INTO audit_log (actor, role, action, resource, decision, detail, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (actor, role, action, resource, decision, detail, time.time()),
    )


async def _principal(db, api_key: Optional[str]):
    if not api_key:
        return None
    return await db.fetchone("SELECT * FROM principals WHERE api_key=?", (api_key,))


def _require(principal, allowed_roles: set[str], action: str, resource: str):
    if principal is None:
        raise HTTPException(status_code=401, detail="missing or invalid API key")
    if principal["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail=f"role {principal['role']!r} cannot {action}")
    return principal


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
def create_router(engine: DeliveryEngine, db: Database, key_store: KeyStore, config: Config) -> APIRouter:
    router = APIRouter()

    async def _auth(request: Request, allowed_roles: set[str], action: str):
        principal = await _principal(db, request.headers.get("X-API-Key"))
        resource = request.url.path
        if principal is None:
            await _audit(db, "anonymous", "-", action, resource, "denied", "no valid API key")
            raise HTTPException(status_code=401, detail="missing or invalid API key")
        if principal["role"] not in allowed_roles:
            await _audit(db, principal["name"], principal["role"], action, resource, "denied",
                         f"role {principal['role']} not allowed")
            raise HTTPException(status_code=403, detail=f"role {principal['role']!r} cannot {action}")
        return principal

    # ---- production -------------------------------------------------------
    @router.post("/events")
    async def create_event(body: EventIn):
        result = await engine.enqueue_event(body.event_type, body.payload, body.ordering_key)
        return result

    @router.get("/events/{event_id}")
    async def get_event(event_id: str):
        event = await db.fetchone("SELECT * FROM events WHERE id=?", (event_id,))
        if event is None:
            raise HTTPException(status_code=404, detail="event not found")
        tasks = await db.fetchall(
            "SELECT id, subscription_id, status, attempt_count, max_attempts, next_attempt_at, "
            "dead_letter_reason, last_error, replay_of, replay_job_id, created_at, updated_at "
            "FROM delivery_tasks WHERE event_id=? ORDER BY created_at",
            (event_id,),
        )
        return {
            "id": event["id"],
            "event_type": event["event_type"],
            "payload": json.loads(event["payload"]),
            "status": event["status"],
            "created_at": event["created_at"],
            "deliveries": [dict(t) for t in tasks],
        }

    # ---- subscriptions ----------------------------------------------------
    @router.post("/subscriptions")
    async def create_subscription(body: SubscriptionIn):
        now = time.time()
        sub_id = uuid.uuid4().hex
        await db.execute(
            "INSERT INTO subscriptions (id, name, target, event_types, active, created_at) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            (sub_id, body.name, body.target, json.dumps(body.event_types), now),
        )
        return {"id": sub_id, **body.model_dump()}

    @router.get("/subscriptions")
    async def list_subscriptions():
        rows = await db.fetchall(
            "SELECT id, name, target, event_types, active, created_at FROM subscriptions ORDER BY created_at"
        )
        out = []
        for r in rows:
            d = dict(r)
            d["event_types"] = json.loads(d["event_types"])
            d["active"] = bool(d["active"])
            out.append(d)
        return out

    @router.get("/subscriptions/{sub_id}")
    async def get_subscription(sub_id: str):
        r = await db.fetchone("SELECT * FROM subscriptions WHERE id=?", (sub_id,))
        if r is None:
            raise HTTPException(status_code=404, detail="subscription not found")
        d = dict(r)
        d["event_types"] = json.loads(d["event_types"])
        d["active"] = bool(d["active"])
        return d

    # ---- delivery inspection ---------------------------------------------
    @router.get("/deliveries/{delivery_id}")
    async def get_delivery(delivery_id: str):
        task = await db.fetchone(
            "SELECT * FROM delivery_tasks WHERE id=?", (delivery_id,)
        )
        if task is None:
            raise HTTPException(status_code=404, detail="delivery not found")
        attempts = await db.fetchall(
            "SELECT attempt_number, result, status_code, error, latency_ms, started_at, finished_at "
            "FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt_number",
            (delivery_id,),
        )
        d = dict(task)
        d["attempts"] = [dict(a) for a in attempts]
        return d

    # ---- dead letters (viewer + admin) -----------------------------------
    @router.get("/admin/dead-letters")
    async def list_dead_letters(request: Request, subscription_id: Optional[str] = None,
                               from_ts: Optional[float] = None, to_ts: Optional[float] = None):
        principal = await _auth(request, {"viewer", "admin"}, "dead_letters:read")
        sql = "SELECT * FROM dead_letters WHERE 1=1"
        params: list = []
        if subscription_id:
            sql += " AND subscription_id=?"
            params.append(subscription_id)
        if from_ts is not None:
            sql += " AND dead_lettered_at >= ?"
            params.append(from_ts)
        if to_ts is not None:
            sql += " AND dead_lettered_at <= ?"
            params.append(to_ts)
        sql += " ORDER BY dead_lettered_at DESC"
        rows = await db.fetchall(sql, params)
        await _audit(db, principal["name"], principal["role"], "dead_letters:read",
                    request.url.path, "allowed", f"{len(rows)} rows")
        out = []
        for r in rows:
            d = dict(r)
            d["attempts"] = json.loads(d.pop("attempts_json"))
            out.append(d)
        return out

    # ---- replay (admin only) ---------------------------------------------
    @router.post("/admin/subscriptions/{sub_id}/replay")
    async def replay(sub_id: str, request: Request, from_ts: Optional[float] = None,
                     to_ts: Optional[float] = None):
        principal = await _auth(request, {"admin"}, "replay")
        sub = await db.fetchone("SELECT id FROM subscriptions WHERE id=?", (sub_id,))
        if sub is None:
            raise HTTPException(status_code=404, detail="subscription not found")
        if from_ts is None:
            from_ts = 0.0
        if to_ts is None:
            to_ts = time.time()
        result = await engine.replay_dead_letters(sub_id, from_ts, to_ts, principal["name"])
        await _audit(db, principal["name"], principal["role"], "replay",
                     f"subscription:{sub_id}", "allowed", json.dumps(result))
        return result

    # ---- replay jobs (viewer + admin) ------------------------------------
    @router.get("/admin/replay-jobs")
    async def list_replay_jobs(request: Request, subscription_id: Optional[str] = None,
                               limit: int = 100):
        principal = await _auth(request, {"viewer", "admin"}, "replay_jobs:read")
        sql = ("SELECT id, subscription_id, from_ts, to_ts, status, replayed_count, "
               "created_by, created_at, finished_at, detail FROM replay_jobs")
        params: list = []
        if subscription_id:
            sql += " WHERE subscription_id=?"
            params.append(subscription_id)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = await db.fetchall(sql, params)
        await _audit(db, principal["name"], principal["role"], "replay_jobs:read",
                     request.url.path, "allowed", f"{len(rows)} rows")
        return [dict(r) for r in rows]

    # ---- key rotation (admin only) ---------------------------------------
    @router.post("/admin/keys/rotate")
    async def rotate_key(request: Request):
        principal = await _auth(request, {"admin"}, "keys:rotate")
        new_key = await key_store.rotate()
        await _audit(db, principal["name"], principal["role"], "keys:rotate",
                     new_key.key_id, "allowed", "previous active key marked rotated")
        return {"key_id": new_key.key_id, "status": new_key.status}

    @router.post("/admin/keys/{key_id}/retire")
    async def retire_key(key_id: str, request: Request):
        principal = await _auth(request, {"admin"}, "keys:retire")
        await key_store.retire(key_id)
        await _audit(db, principal["name"], principal["role"], "keys:retire",
                     key_id, "allowed", "key retired")
        return {"key_id": key_id, "status": "retired"}

    @router.get("/admin/keys")
    async def list_keys(request: Request):
        await _auth(request, {"viewer", "admin"}, "keys:read")
        keys = await key_store.all()
        return [
            {"key_id": k.key_id, "status": k.status, "created_at": k.created_at,
             "rotated_at": k.rotated_at}
            for k in keys.values()
        ]

    # ---- audit log (admin only) ------------------------------------------
    @router.get("/admin/audit")
    async def list_audit(request: Request, limit: int = 100):
        principal = await _auth(request, {"admin"}, "audit:read")
        rows = await db.fetchall(
            "SELECT actor, role, action, resource, decision, detail, created_at "
            "FROM audit_log ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        await _audit(db, principal["name"], principal["role"], "audit:read",
                     request.url.path, "allowed", f"{len(rows)} rows")
        return [dict(r) for r in rows]

    # ---- metrics ----------------------------------------------------------
    @router.get("/metrics")
    async def get_metrics():
        return await engine.metrics()

    return router


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    db: Database = app.state.db
    await db.open()
    key_store: KeyStore = app.state.key_store
    await key_store.initialize()
    # Seed default principals (idempotent).
    await db.execute(
        "INSERT OR IGNORE INTO principals (api_key, name, role) VALUES (?, ?, ?)",
        (app.state.config.admin_api_key, "admin", "admin"),
    )
    await db.execute(
        "INSERT OR IGNORE INTO principals (api_key, name, role) VALUES (?, ?, ?)",
        (app.state.config.viewer_api_key, "viewer", "viewer"),
    )
    engine: DeliveryEngine = app.state.engine
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()
        await app.state.http.aclose()
        await db.close()


def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or Config.from_env()
    db = Database(config.db_path)
    inproc = InProcessTransport()
    http = HttpTransport()
    key_store = KeyStore(db)
    engine = DeliveryEngine(db, key_store, inproc, http, config)

    app = FastAPI(title="Webhook Delivery Layer", lifespan=lifespan)
    app.state.config = config
    app.state.db = db
    app.state.inproc = inproc
    app.state.http = http
    app.state.key_store = key_store
    app.state.engine = engine

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    app.include_router(create_router(engine, db, key_store, config))
    return app
