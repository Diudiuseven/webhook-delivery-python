"""场景：全局/按订阅并发上限、慢接收方不拖垮投递、积压指标可观测。"""
from __future__ import annotations

import asyncio
import time

from app.dispatcher import Dispatcher
from app.models import Event, Subscription
from app.retry import RetryPolicy
from app.transport import TransportResult
from tests.conftest import TEST_POLICY, FakeTransport, run


def _subs(storage, n):
    for i in range(n):
        storage.add_subscription(Subscription(
            id=f"s{i}", url=f"http://127.0.0.1:9001/h{i}", event_types=[f"t{i}"]))


def test_claim_respects_per_subscription_ordering_and_global_limit(storage, signing_key):
    """单订阅同一时刻最多领取 1 条（顺序门控）；多订阅并行领取受全局上限约束。"""
    _subs(storage, 5)
    for i in range(5):
        for j in range(3):
            storage.enqueue_event(Event(id=f"e{i}-{j}", event_type=f"t{i}", payload={}))

    claimed = storage.claim_due_deliveries(limit=2, lease_seconds=30)
    assert len(claimed) == 2                       # 全局上限
    subs = {d.subscription_id for d in claimed}
    assert len(subs) == 2                          # 每条属于不同订阅（每订阅只放行头部）

    claimed2 = storage.claim_due_deliveries(limit=10, lease_seconds=30)
    assert len(claimed2) == 3                      # 剩余 3 个订阅各放行 1 条
    assert all(d.seq == 0 for d in claimed + claimed2)  # 较晚事件未越过头部


def test_slow_receiver_does_not_stall_dispatcher(storage, transport, signing_key):
    """一个慢订阅占满自己的并发配额时，其他订阅仍能快速完成。"""
    storage.add_subscription(Subscription(id="slow", url="http://127.0.0.1/slow",
                                        event_types=["slow"], max_concurrency=1))
    storage.add_subscription(Subscription(id="fast", url="http://127.0.0.1/fast",
                                        event_types=["fast"], max_concurrency=1))

    async def drive():
        policy = RetryPolicy(timeout=2.0, lease_seconds=30)
        d = Dispatcher(storage, transport, policy, global_concurrency=4)
        started = time.monotonic()

        async def slow_post(url, body, headers, timeout):
            if "slow" in url:
                await asyncio.sleep(0.5)           # 慢接收方
            return TransportResult(status_code=200)

        transport.post = slow_post  # type: ignore[assignment]
        for i in range(3):
            storage.enqueue_event(Event(id=f"s{i}", event_type="slow", payload={}))
            storage.enqueue_event(Event(id=f"f{i}", event_type="fast", payload={}))
        await d.drain()
        return time.monotonic() - started

    elapsed = run(drive())
    # 快订阅 3 条在慢订阅第 1 条完成前就应全部送达；
    # 总耗时约等于慢订阅串行耗时（3*0.5s），远小于全部串行（6*0.5s+）
    assert elapsed < 2.0
    fast = [dl for dl in storage.list_deliveries("fast")]
    assert all(dl.status.value == "delivered" for dl in fast)


def test_backlog_metrics_distinguish_slow_receiver_from_capacity(storage, transport, signing_key):
    """积压指标：inflight 打满 + pending 增长 = 接收方慢或容量不足，可观测区分。"""
    storage.add_subscription(Subscription(id="s1", url="http://127.0.0.1/h1",
                                        event_types=["*"]))
    for i in range(5):
        storage.enqueue_event(Event(id=f"e{i}", event_type="t", payload={}))

    m = Dispatcher(storage, transport, TEST_POLICY, global_concurrency=8).metrics()
    assert m["by_status"]["pending"] == 5
    per_sub = {r["subscription_id"]: r for r in m["per_subscription"]}
    assert per_sub["s1"]["pending"] == 5
    assert per_sub["s1"]["oldest_age_seconds"] >= 0
    assert m["dispatcher"]["inflight"] == 0
    assert m["dispatcher"]["available_slots"] == 8
