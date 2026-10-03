"""场景：死信存储、按订阅/时间范围查询、重放保持投递语义且不累计尝试次数、
重放与正常投递并发时结果确定。"""
from __future__ import annotations

import asyncio

from app.dispatcher import Dispatcher
from app.models import DeliveryStatus, Event, Subscription
from app.transport import TransportResult
from tests.conftest import TEST_POLICY, Receiver, drain_with, force_due, run

URL = "http://127.0.0.1:9001/hook"


def _make_dead(storage, transport, eid="e1"):
    storage.add_subscription(Subscription(id="s1", url=URL, event_types=["*"]))
    transport.set_handler(URL, lambda i, h, b: TransportResult(status_code=400))
    storage.enqueue_event(Event(id=eid, event_type="t", payload={"v": 1}))
    run(drain_with(storage, transport))
    return storage.list_deliveries("s1")[0]


def test_dead_letter_queryable_with_attempts(storage, transport, signing_key):
    dead = _make_dead(storage, transport)
    assert dead.status == DeliveryStatus.DEAD

    items = storage.list_dead_letters("s1")
    assert len(items) == 1
    assert items[0]["delivery_id"] == dead.id
    assert "permanent" in items[0]["reason"]
    attempts = storage.list_attempts(dead.id)
    assert len(attempts) == 1 and attempts[0].status_code == 400

    # 时间范围过滤
    assert storage.list_dead_letters("s1", since=dead.created_at + 100) == []
    assert storage.list_dead_letters("other-sub") == []


def test_replay_dead_letter(storage, transport, signing_key):
    """重放：新投递尝试次数从零开始、保持原事件与订阅、可成功送达。"""
    dead = _make_dead(storage, transport)
    receiver = Receiver(keys_provider=storage.list_valid_keys)
    transport.set_handler(URL, receiver.as_handler())

    replays = storage.replay_dead_letters([dead.id])
    assert len(replays) == 1
    replay = replays[0]
    assert replay.replay_of == dead.id
    assert replay.attempt_count == 0            # 不重复累计尝试次数
    assert replay.seq > dead.seq                # 保持订阅内顺序语义
    assert replay.event_id == dead.event_id

    run(drain_with(storage, transport))
    assert receiver.received == ["e1"]
    final = storage.get_delivery(replay.id)
    assert final.status == DeliveryStatus.DELIVERED
    assert final.attempt_count == 1             # 只算重放后的尝试
    # 原死信记录保留，且标记已重放
    item = storage.list_dead_letters("s1")[0]
    assert item["replayed_at"] is not None
    assert storage.get_delivery(dead.id).status == DeliveryStatus.DEAD


def test_replay_concurrent_with_normal_delivery_deterministic(storage, transport, signing_key):
    """重放与新事件入队并发：序号分配在同一事务机制下，结果确定、无重复序号。"""
    dead = _make_dead(storage, transport, eid="e-dead")
    receiver = Receiver(keys_provider=storage.list_valid_keys)
    transport.set_handler(URL, receiver.as_handler())

    async def concurrent():
        # 并发：一路重放死信，一路入队新事件
        replay_done, enqueue_done = asyncio.Event(), asyncio.Event()

        async def do_replay():
            r = storage.replay_dead_letters([dead.id])
            replay_done.set()
            return r

        async def do_enqueue():
            d = storage.enqueue_event(Event(id="e-new", event_type="t", payload={}))
            enqueue_done.set()
            return d

        (replays, _), _ = await asyncio.gather(
            asyncio.gather(do_replay(), do_enqueue()),
            asyncio.sleep(0))
        return replays

    replays = run(concurrent())
    deliveries = storage.list_deliveries("s1")
    seqs = [d.seq for d in deliveries]
    assert len(seqs) == len(set(seqs))          # 序号无冲突
    assert replays[0].event_id == "e-dead"

    run(drain_with(storage, transport))
    assert sorted(receiver.received) == ["e-dead", "e-new"]
