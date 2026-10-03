"""集成：HttpTransport 真实代码路径（ASGI 进程内接收方，不依赖外部服务）。"""
from __future__ import annotations

import asyncio

import httpx
from fastapi import FastAPI, Request

from app.dispatcher import Dispatcher
from app.models import DeliveryStatus, Event, Subscription
from app.signing import verify_signature
from app.storage import Storage
from app.transport import HttpTransport
from tests.conftest import TEST_POLICY


def test_http_transport_loopback_delivery(storage, signing_key):
    received = []

    receiver_app = FastAPI()

    @receiver_app.post("/hook")
    async def hook(request: Request):
        body = await request.body()
        ok = verify_signature(storage.list_valid_keys(),
                              request.headers["x-signature-timestamp"],
                              request.headers["x-signature"], body)
        if not ok:
            return {"ok": False}, 401
        received.append(request.headers["x-event-id"])
        return {"ok": True}

    async def drive():
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=receiver_app),
            base_url="http://127.0.0.1")
        try:
            storage.add_subscription(Subscription(
                id="s1", url="http://127.0.0.1/hook", event_types=["*"]))
            storage.enqueue_event(Event(id="e1", event_type="t", payload={"v": 1}))
            d = Dispatcher(storage, HttpTransport(client=client), TEST_POLICY)
            await d.drain()
        finally:
            await client.aclose()

    asyncio.run(drive())
    assert received == ["e1"]
    assert storage.list_deliveries("s1")[0].status == DeliveryStatus.DELIVERED


def test_http_transport_connection_refused_is_retryable(storage, signing_key):
    """连接异常（未监听的回环端口）归类为可重试失败，不拖垮进程。"""
    async def drive():
        client = httpx.AsyncClient()
        try:
            storage.add_subscription(Subscription(
                id="s1", url="http://127.0.0.1:1/hook", event_types=["*"]))
            storage.enqueue_event(Event(id="e1", event_type="t", payload={}))
            d = Dispatcher(storage, HttpTransport(client=client), TEST_POLICY)
            await d.run_once()
        finally:
            await client.aclose()

    asyncio.run(asyncio.wait_for(drive(), timeout=10))
    dl = storage.list_deliveries("s1")[0]
    assert dl.status.value == "pending"   # 可重试：回到待投递等待退避
    assert dl.attempt_count == 1
    attempt = storage.list_attempts(dl.id)[0]
    assert attempt.status_code is None and "connection" in (attempt.error or "")
