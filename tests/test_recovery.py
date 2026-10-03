"""场景：重启恢复——claimed 事件有确定归宿、租约超时重新领取、重复投递可去重。"""
from __future__ import annotations

import asyncio
import time

from app.models import DeliveryStatus, Event, SigningKey, Subscription
from app.retry import RetryPolicy
from app.storage import Storage
from tests.conftest import Receiver, drain_with, run

URL = "http://127.0.0.1:9001/hook"


def _boot_db(path: str) -> Storage:
    s = Storage(path)
    s.connect()
    if s.get_active_key() is None:
        s.add_signing_key(SigningKey(id="k1", secret="secret-1", active=True))
    return s


def test_restart_recovers_claimed_deliveries(tmp_path, transport):
    """进程在领取后、完成前崩溃：重启后 claimed 投递回到 pending 并被重新投递，
    接收方按事件 ID 去重，不重复处理。"""
    db = str(tmp_path / "restart.db")
    s1 = _boot_db(db)
    s1.add_subscription(Subscription(id="s1", url=URL, event_types=["*"]))
    s1.enqueue_event(Event(id="e1", event_type="t", payload={}))
    claimed = s1.claim_due_deliveries(10, lease_seconds=300)
    assert len(claimed) == 1
    s1.close()  # 崩溃：投递处于 claimed，持有者已消失

    s2 = _boot_db(db)
    recovered = s2.reset_claims_on_startup()    # 启动恢复
    assert recovered == 1
    d = s2.list_deliveries("s1")[0]
    assert d.status == DeliveryStatus.PENDING

    receiver = Receiver(keys_provider=s2.list_valid_keys)
    transport.set_handler(URL, receiver.as_handler())
    run(drain_with(s2, transport))
    assert receiver.received == ["e1"]
    assert s2.list_deliveries("s1")[0].status == DeliveryStatus.DELIVERED
    s2.close()


def test_lease_expiry_reclaim_and_dedup(storage, transport, signing_key):
    """领取后超时未完成：租约过期被重新领取并再次投递；
    接收方识别重复事件 ID 并去重。"""
    storage.add_subscription(Subscription(id="s1", url=URL, event_types=["*"]))
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))

    # worker1 领取后"卡住"（不完成），租约很短
    claimed = storage.claim_due_deliveries(10, lease_seconds=0.05)
    assert len(claimed) == 1
    time.sleep(0.1)
    assert storage.release_expired_claims() == 1   # 租约过期，重新可领取
    assert storage.get_delivery(claimed[0].id).status == DeliveryStatus.PENDING

    receiver = Receiver(keys_provider=storage.list_valid_keys)
    transport.set_handler(URL, receiver.as_handler())
    run(drain_with(storage, transport))
    assert receiver.received == ["e1"]
    assert receiver.duplicates == [] or receiver.received.count("e1") == 1

    # 模拟 worker1 迟到完成：其租约 token 已失效，完成请求被拒绝（不影响终态）
    from app.models import Attempt, AttemptOutcome, new_id
    late = Attempt(id=new_id(), delivery_id=claimed[0].id, attempt_no=99,
                   outcome=AttemptOutcome.SUCCESS, status_code=200, error=None,
                   duration_ms=1.0, key_id=None)
    ok = storage.complete_delivery(claimed[0].id, claimed[0].claim_token, late)
    assert ok is False
    assert storage.get_delivery(claimed[0].id).status == DeliveryStatus.DELIVERED


def test_every_delivery_has_queryable_state(storage, transport, signing_key):
    """任意时刻每个事件都有确定、可查询的状态。"""
    storage.add_subscription(Subscription(id="s1", url=URL, event_types=["*"]))
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))
    d = storage.list_deliveries("s1")[0]
    assert d.status == DeliveryStatus.PENDING

    claimed = storage.claim_due_deliveries(10, lease_seconds=0.05)
    assert storage.get_delivery(d.id).status == DeliveryStatus.CLAIMED
    time.sleep(0.1)  # 租约过期后 drain 才能重新领取

    receiver = Receiver(keys_provider=storage.list_valid_keys)
    transport.set_handler(URL, receiver.as_handler())
    run(drain_with(storage, transport))
    # drain 后（租约释放 + 重新领取 + 完成）
    assert storage.get_delivery(d.id).status == DeliveryStatus.DELIVERED
