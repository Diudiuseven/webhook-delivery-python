"""FastAPI 应用：订阅管理、事件入队、状态查询、死信查询/重放、密钥轮换、指标、审计。

保留原有 GET / 行为不变。死信查询与重放、密钥轮换、审计查询均需权限，
所有鉴权结果（含拒绝）都会写入审计日志。
"""
from __future__ import annotations

import secrets
from dataclasses import asdict
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .auth import Authorizer, Principal
from .dispatcher import Dispatcher
from .models import (
    AuditRecord,
    Event,
    SigningKey,
    Subscription,
    new_id,
    now,
)
from .storage import Storage


class SubscriptionIn(BaseModel):
    url: str
    event_types: list[str] = ["*"]
    max_concurrency: int = 1


class EventIn(BaseModel):
    event_type: str
    payload: dict
    id: Optional[str] = None


class ReplayIn(BaseModel):
    delivery_ids: Optional[list[str]] = None
    subscription_id: Optional[str] = None
    since: float = 0.0
    until: float = float("inf")


class RotateIn(BaseModel):
    grace_seconds: float = 3600.0   # 旧密钥过渡期


def create_app(storage: Storage, dispatcher: Dispatcher, authorizer: Authorizer) -> FastAPI:
    app = FastAPI(title="webhook-delivery")

    def get_principal(authorization: Optional[str] = Header(default=None),
                      x_admin_token: Optional[str] = Header(default=None)) -> Optional[Principal]:
        token = x_admin_token
        if not token and authorization and authorization.startswith("Bearer "):
            token = authorization[len("Bearer "):]
        return authorizer.authenticate(token)

    def audit(actor: str, action: str, target: str, allowed: bool, detail: str = "") -> None:
        storage.add_audit(AuditRecord(id=new_id(), actor=actor, action=action,
                                      target=target, allowed=allowed, detail=detail))

    def require_scope(principal: Optional[Principal], scope: str, action: str, target: str) -> Principal:
        if not authorizer.require(principal, scope):
            actor = principal.actor if principal else "anonymous"
            audit(actor, action, target, allowed=False, detail=f"missing scope: {scope}")
            raise HTTPException(status_code=403, detail=f"forbidden: requires scope {scope!r}")
        return principal  # type: ignore[return-value]

    # ---- 原有接口，行为不变 ----
    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    # ---- 订阅管理 ----
    @app.post("/subscriptions", status_code=201)
    def create_subscription(body: SubscriptionIn):
        sub = Subscription(id=new_id(), url=body.url, event_types=body.event_types,
                           max_concurrency=max(1, body.max_concurrency))
        storage.add_subscription(sub)
        return asdict(sub)

    @app.get("/subscriptions")
    def list_subscriptions():
        return [asdict(s) for s in storage.list_subscriptions()]

    # ---- 事件入队（事件 + 匹配投递同一事务落库） ----
    @app.post("/events", status_code=201)
    def ingest_event(body: EventIn):
        event = Event(id=body.id or new_id(), event_type=body.event_type, payload=body.payload)
        deliveries = storage.enqueue_event(event)
        return {"event_id": event.id,
                "deliveries": [{"id": d.id, "subscription_id": d.subscription_id, "seq": d.seq}
                               for d in deliveries]}

    # ---- 状态查询 ----
    @app.get("/deliveries/{delivery_id}")
    def get_delivery(delivery_id: str):
        d = storage.get_delivery(delivery_id)
        if d is None:
            raise HTTPException(status_code=404, detail="delivery not found")
        return {"delivery": asdict(d),
                "attempts": [asdict(a) for a in storage.list_attempts(delivery_id)]}

    @app.get("/subscriptions/{subscription_id}/deliveries")
    def list_deliveries(subscription_id: str):
        return [asdict(d) for d in storage.list_deliveries(subscription_id=subscription_id)]

    # ---- 死信查询与重放（需权限 + 审计） ----
    @app.get("/dead-letters")
    def list_dead_letters(subscription_id: Optional[str] = None, since: float = 0.0,
                          until: float = float("inf"),
                          principal: Optional[Principal] = Depends(get_principal)):
        require_scope(principal, "deadletters:read", "deadletters.read", subscription_id or "*")
        items = storage.list_dead_letters(subscription_id=subscription_id, since=since, until=until)
        for item in items:
            item["attempts"] = [asdict(a) for a in storage.list_attempts(item["delivery_id"])]
        return items

    @app.post("/dead-letters/replay", status_code=200)
    def replay_dead_letters(body: ReplayIn,
                            principal: Optional[Principal] = Depends(get_principal)):
        require_scope(principal, "replay", "deadletters.replay",
                      body.subscription_id or ",".join(body.delivery_ids or []))
        ids = body.delivery_ids
        if not ids:
            ids = [item["delivery_id"] for item in storage.list_dead_letters(
                subscription_id=body.subscription_id, since=body.since, until=body.until)]
        replays = storage.replay_dead_letters(ids)
        audit(principal.actor, "deadletters.replay", ",".join(ids) or "-",
              allowed=True, detail=f"replayed={len(replays)}")
        return {"replayed": [{"id": d.id, "replay_of": d.replay_of,
                              "subscription_id": d.subscription_id, "seq": d.seq}
                             for d in replays]}

    # ---- 密钥轮换（需权限 + 审计） ----
    @app.post("/keys/rotate", status_code=200)
    def rotate_key(body: RotateIn, principal: Optional[Principal] = Depends(get_principal)):
        require_scope(principal, "keys:rotate", "keys.rotate", "signing-keys")
        key = SigningKey(id=new_id(), secret=secrets.token_hex(32), active=True)
        storage.rotate_key(key, old_key_valid_until=now() + body.grace_seconds)
        audit(principal.actor, "keys.rotate", key.id, allowed=True,
              detail=f"grace_seconds={body.grace_seconds}")
        return {"key_id": key.id, "grace_seconds": body.grace_seconds}

    # ---- 指标与审计 ----
    @app.get("/metrics")
    def metrics():
        return dispatcher.metrics()

    @app.get("/audits")
    def list_audits(principal: Optional[Principal] = Depends(get_principal)):
        require_scope(principal, "audit:read", "audit.read", "audits")
        return [asdict(a) for a in storage.list_audits()]

    return app
