"""测试基础设施：进程内可控传输层、验签+去重接收方、公共夹具。"""
from __future__ import annotations

import asyncio
import json
from typing import Callable, Optional

import pytest

from app.dispatcher import Dispatcher
from app.models import DeliveryStatus, SigningKey
from app.retry import RetryPolicy
from app.signing import verify_signature
from app.storage import Storage
from app.transport import TransportResult

# 测试策略：退避间隔设得很大，测试通过 force_due 显式控制重试时机，保证确定性。
TEST_POLICY = RetryPolicy(max_attempts=3, base_delay=100.0, max_delay=100.0,
                          jitter=0.0, timeout=1.0, lease_seconds=30.0)

Handler = Callable[[int, dict, bytes], TransportResult]


class FakeTransport:
    """进程内传输层：按 URL 注入行为，记录全部请求。"""

    def __init__(self) -> None:
        self.handlers: dict[str, Handler] = {}
        self.requests: list[dict] = []
        self._counts: dict[str, int] = {}

    def set_handler(self, url: str, handler: Handler) -> None:
        self.handlers[url] = handler

    def count(self, url: str) -> int:
        return self._counts.get(url, 0)

    async def post(self, url: str, body: bytes, headers: dict, timeout: float) -> TransportResult:
        idx = self._counts.get(url, 0)
        self._counts[url] = idx + 1
        self.requests.append({"url": url, "headers": dict(headers), "body": body})
        handler = self.handlers.get(url)
        if handler is not None:
            return handler(idx, headers, body)
        return TransportResult(status_code=200)


class Receiver:
    """模拟接收方：验签（接受任一有效密钥，覆盖轮换过渡期）+ 按事件 ID 幂等去重。"""

    def __init__(self, keys_provider: Callable[[], list[SigningKey]]) -> None:
        self._keys_provider = keys_provider
        self.received: list[str] = []       # 首次接受的事件 ID（有序）
        self.duplicates: list[str] = []     # 被去重识别出的重复投递
        self.bad_signatures: int = 0

    def handle(self, headers: dict, body: bytes) -> int:
        ts = headers.get("X-Signature-Timestamp", "")
        sig = headers.get("X-Signature", "")
        if not verify_signature(self._keys_provider(), ts, sig, body):
            self.bad_signatures += 1
            return 401
        event_id = headers.get("X-Event-Id", "")
        if event_id in self.received:
            self.duplicates.append(event_id)
            return 200  # 重复投递被识别，直接确认，不重复处理
        self.received.append(event_id)
        return 200

    def as_handler(self) -> Handler:
        def _handler(idx: int, headers: dict, body: bytes) -> TransportResult:
            return TransportResult(status_code=self.handle(headers, body))
        return _handler


def force_due(storage: Storage) -> None:
    """把所有 pending 投递的到期时间提前到现在（跳过退避等待）。"""
    with storage._lock:
        storage.conn.execute("UPDATE deliveries SET next_attempt_at=0 WHERE status=?",
                             (DeliveryStatus.PENDING.value,))


def run(coro):
    return asyncio.run(coro)


async def drain_with(storage: Storage, transport: FakeTransport,
                     policy: RetryPolicy = TEST_POLICY, rounds: int = 50) -> int:
    """新建一次性调度器并 drain；每轮前强制到期。"""
    dispatcher = Dispatcher(storage, transport, policy)
    total = 0
    for _ in range(rounds):
        force_due(storage)
        n = await dispatcher.run_once()
        total += n
        if n == 0:
            break
    return total


@pytest.fixture
def storage() -> Storage:
    s = Storage(":memory:")
    s.connect()
    yield s
    s.close()


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def signing_key(storage: Storage) -> SigningKey:
    key = SigningKey(id="k1", secret="secret-1", active=True)
    storage.add_signing_key(key)
    return key


@pytest.fixture
def receiver(storage: Storage, signing_key: SigningKey) -> Receiver:
    return Receiver(keys_provider=storage.list_valid_keys)
