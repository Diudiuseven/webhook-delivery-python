"""场景：密钥轮换——过渡期内新旧密钥均可验签，重试跨越轮换点仍可验证。"""
from __future__ import annotations

import time

from app.models import DeliveryStatus, Event, SigningKey, Subscription, now
from app.signing import build_signature_header, verify_signature
from app.storage import Storage
from app.transport import TransportResult
from tests.conftest import Receiver, drain_with, force_due, run

URL = "http://127.0.0.1:9001/hook"


def test_rotation_across_retries(storage, transport, signing_key):
    """事件第一次尝试用旧密钥签名并失败；轮换后重试用新密钥签名，
    接收方在整个过渡期都能验签，事件不丢失、不失效。"""
    storage.add_subscription(Subscription(id="s1", url=URL, event_types=["*"]))
    receiver = Receiver(keys_provider=storage.list_valid_keys)
    state = {"fail_first": True}

    def handler(idx, headers, body):
        # 接收方始终先验签；第一次尝试业务上返回 500 触发重试
        status = receiver.handle(headers, body)
        if status != 200:
            return TransportResult(status_code=status)
        if state["fail_first"]:
            state["fail_first"] = False
            receiver.received.clear()  # 业务失败，未真正接收
            return TransportResult(status_code=500)
        return TransportResult(status_code=200)

    transport.set_handler(URL, handler)
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))

    async def drive():
        from app.dispatcher import Dispatcher
        from tests.conftest import TEST_POLICY
        d = Dispatcher(storage, transport, TEST_POLICY)
        await d.run_once()  # 第 1 次尝试：旧密钥 k1 签名，业务 500
        # 轮换：k2 成为签名密钥，k1 保留 1 小时过渡期
        k2 = SigningKey(id="k2", secret="secret-2", active=True)
        storage.rotate_key(k2, old_key_valid_until=now() + 3600)
        force_due(storage)
        await d.run_once()  # 第 2 次尝试：新密钥 k2 签名，成功
        return d

    run(drive())

    assert receiver.bad_signatures == 0          # 两次尝试验签均通过
    assert receiver.received == ["e1"]
    delivery = storage.list_deliveries("s1")[0]
    assert delivery.status == DeliveryStatus.DELIVERED
    key_ids = [a.key_id for a in storage.list_attempts(delivery.id)]
    assert key_ids == ["k1", "k2"]               # 重试跨越轮换点，自动改用新密钥


def test_old_key_accepted_during_grace_and_rejected_after(storage):
    """过渡期内旧密钥签出的请求被接受；过渡期结束后被拒绝。"""
    old = SigningKey(id="k-old", secret="old-secret", active=False,
                     valid_until=now() + 3600)
    new = SigningKey(id="k-new", secret="new-secret", active=True)
    storage.add_signing_key(old)
    storage.add_signing_key(new)

    body = b'{"x":1}'
    ts, hdr = build_signature_header(old, body)
    assert verify_signature(storage.list_valid_keys(), ts, hdr, body)

    # 旧密钥过期后不再有效
    expired = SigningKey(id="k-gone", secret="gone", active=False,
                         valid_until=now() - 1)
    storage.add_signing_key(expired)
    ts2, hdr2 = build_signature_header(expired, body)
    assert not verify_signature(storage.list_valid_keys(), ts2, hdr2, body)

    # 篡改报文任何密钥都验不过
    assert not verify_signature(storage.list_valid_keys(), ts, hdr, b'{"x":2}')
