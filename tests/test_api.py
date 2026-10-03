"""场景：HTTP 接口——原有行为不变、越权重放被拒且留审计、死信查询鉴权、
密钥轮换接口、指标接口。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.auth import Authorizer
from app.models import DeliveryStatus, SigningKey
from app.storage import Storage
from app.transport import TransportResult
from tests.conftest import FakeTransport, drain_with, run

TOKENS = {
    "admin-token": ("admin", ["*"]),
    "ops-token": ("ops", ["replay", "deadletters:read"]),
    "viewer-token": ("viewer", []),
}
URL = "http://127.0.0.1:9001/hook"


@pytest.fixture
def env():
    storage = Storage(":memory:")
    storage.connect()
    storage.add_signing_key(SigningKey(id="k1", secret="secret-1", active=True))
    transport = FakeTransport()
    from app.dispatcher import Dispatcher
    from tests.conftest import TEST_POLICY
    dispatcher = Dispatcher(storage, transport, TEST_POLICY)
    app = create_app(storage, dispatcher, Authorizer(TOKENS))
    return storage, transport, TestClient(app)


def _make_dead_letter(storage, transport, client) -> str:
    client.post("/subscriptions", json={"url": URL, "event_types": ["*"]})
    resp = client.post("/events", json={"event_type": "t", "payload": {"v": 1}})
    delivery_id = resp.json()["deliveries"][0]["id"]
    transport.set_handler(URL, lambda i, h, b: TransportResult(status_code=400))
    run(drain_with(storage, transport))
    assert storage.get_delivery(delivery_id).status == DeliveryStatus.DEAD
    return delivery_id


def test_root_behavior_unchanged(env):
    _, _, client = env
    assert client.get("/").json() == {"Hello": "World"}


def test_unauthorized_replay_rejected_and_audited(env):
    storage, transport, client = env
    delivery_id = _make_dead_letter(storage, transport, client)

    # 无 token -> 403
    r = client.post("/dead-letters/replay", json={"delivery_ids": [delivery_id]})
    assert r.status_code == 403
    # 无 replay 权限的 token -> 403
    r = client.post("/dead-letters/replay", json={"delivery_ids": [delivery_id]},
                    headers={"X-Admin-Token": "viewer-token"})
    assert r.status_code == 403
    # 死信仍在，未被重放
    assert len(storage.list_deliveries("s1") if False else
               [d for d in storage.list_deliveries() if d.replay_of == delivery_id]) == 0

    # 越权行为已留审计（含匿名与已认证主体）
    audits = storage.list_audits()
    denied = [a for a in audits if not a.allowed and a.action == "deadletters.replay"]
    assert len(denied) == 2
    assert {a.actor for a in denied} == {"anonymous", "viewer"}


def test_authorized_replay_and_deadletter_query(env):
    storage, transport, client = env
    delivery_id = _make_dead_letter(storage, transport, client)

    # 死信查询需要权限
    assert client.get("/dead-letters").status_code == 403
    r = client.get("/dead-letters", headers={"X-Admin-Token": "ops-token"})
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1 and items[0]["delivery_id"] == delivery_id
    assert len(items[0]["attempts"]) == 1  # 失败原因与尝试记录可查

    # 修复接收方后重放
    transport.set_handler(URL, lambda i, h, b: TransportResult(status_code=200))
    r = client.post("/dead-letters/replay", json={"delivery_ids": [delivery_id]},
                    headers={"X-Admin-Token": "ops-token"})
    assert r.status_code == 200
    replay_id = r.json()["replayed"][0]["id"]

    run(drain_with(storage, transport))
    replay = storage.get_delivery(replay_id)
    assert replay.status == DeliveryStatus.DELIVERED
    assert replay.attempt_count == 1  # 重放不累计原尝试次数

    # 成功的重放也有审计
    ok = [a for a in storage.list_audits() if a.allowed and a.action == "deadletters.replay"]
    assert len(ok) == 1 and ok[0].actor == "ops"


def test_key_rotation_endpoint_requires_scope(env):
    storage, _, client = env
    assert client.post("/keys/rotate", json={}).status_code == 403
    r = client.post("/keys/rotate", json={"grace_seconds": 60},
                    headers={"X-Admin-Token": "admin-token"})
    assert r.status_code == 200
    keys = storage.list_valid_keys()
    assert len(keys) == 2  # 新旧密钥过渡期内均有效
    assert any(a.action == "keys.rotate" and a.allowed for a in storage.list_audits())


def test_metrics_endpoint_exposes_backlog(env):
    storage, transport, client = env
    client.post("/subscriptions", json={"url": URL, "event_types": ["*"]})
    client.post("/events", json={"event_type": "t", "payload": {}})
    client.post("/events", json={"event_type": "t", "payload": {}})
    m = client.get("/metrics").json()
    assert m["by_status"]["pending"] == 2
    assert m["per_subscription"][0]["pending"] == 2
    assert "oldest_age_seconds" in m["per_subscription"][0]
    assert m["dispatcher"]["global_concurrency"] > 0


def test_audit_query_requires_scope(env):
    _, _, client = env
    assert client.get("/audits").status_code == 403
    r = client.get("/audits", headers={"X-Admin-Token": "admin-token"})
    assert r.status_code == 200
