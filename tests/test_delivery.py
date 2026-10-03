"""场景：崩溃不丢事件、部分重试失败、永久失败、按订阅顺序与故障隔离。"""
from __future__ import annotations

from app.dispatcher import Dispatcher
from app.models import AttemptOutcome, DeliveryStatus, Event, Subscription
from app.storage import Storage
from app.transport import TransportResult
from tests.conftest import TEST_POLICY, Receiver, drain_with, force_due, run

URL_A = "http://127.0.0.1:9001/hook-a"
URL_B = "http://127.0.0.1:9001/hook-b"


def _sub(sid: str, url: str, types: list[str] = ["*"]) -> Subscription:
    return Subscription(id=sid, url=url, event_types=types)


def test_crash_between_commit_and_delivery_no_event_loss(tmp_path, transport, receiver):
    """事件已提交但进程在投递前崩溃：重启后事件必须被投递，不得静默丢失。"""
    db = str(tmp_path / "crash.db")
    s1 = Storage(db)
    s1.connect()
    s1.add_subscription(_sub("sub-a", URL_A))
    from app.models import SigningKey
    s1.add_signing_key(SigningKey(id="k1", secret="secret-1", active=True))
    s1.enqueue_event(Event(id="e1", event_type="user.created", payload={"n": 1}))
    s1.close()  # 模拟崩溃：提交后、投递前进程消失

    # 重启：新的存储实例 + 新调度器，同一数据库文件
    s2 = Storage(db)
    s2.connect()
    recv = Receiver(s2.list_valid_keys)
    transport.set_handler(URL_A, recv.as_handler())
    run(drain_with(s2, transport))

    assert recv.received == ["e1"]
    d = s2.list_deliveries("sub-a")[0]
    assert d.status == DeliveryStatus.DELIVERED
    s2.close()


def test_partial_retry_then_success(storage, transport, receiver, signing_key):
    """前两次可重试失败（500），第三次成功；尝试记录完整且分类正确。"""
    storage.add_subscription(_sub("sub-a", URL_A))
    calls = {"n": 0}

    def flaky(idx, headers, body):
        if idx < 2:
            return TransportResult(status_code=500)
        return TransportResult(status_code=receiver.handle(headers, body))

    transport.set_handler(URL_A, flaky)
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))
    run(drain_with(storage, transport))

    assert receiver.received == ["e1"]
    d = storage.list_deliveries("sub-a")[0]
    assert d.status == DeliveryStatus.DELIVERED
    assert d.attempt_count == 3
    outcomes = [a.outcome for a in storage.list_attempts(d.id)]
    assert outcomes == [AttemptOutcome.RETRYABLE, AttemptOutcome.RETRYABLE,
                        AttemptOutcome.SUCCESS]


def test_permanent_failure_not_retried(storage, transport, signing_key):
    """4xx 为永久失败：立即进入死信，不再重试。"""
    storage.add_subscription(_sub("sub-a", URL_A))
    transport.set_handler(URL_A, lambda i, h, b: TransportResult(status_code=400))
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))
    run(drain_with(storage, transport))

    assert transport.count(URL_A) == 1  # 没有重试
    d = storage.list_deliveries("sub-a")[0]
    assert d.status == DeliveryStatus.DEAD
    dead = storage.list_dead_letters("sub-a")
    assert len(dead) == 1 and "permanent" in dead[0]["reason"]


def test_retry_exhaustion_goes_dead(storage, transport, signing_key):
    """可重试失败耗尽最大尝试次数后进入死信，保留全部尝试记录。"""
    storage.add_subscription(_sub("sub-a", URL_A))
    transport.set_handler(URL_A, lambda i, h, b: TransportResult(status_code=503))
    storage.enqueue_event(Event(id="e1", event_type="t", payload={}))
    run(drain_with(storage, transport))

    assert transport.count(URL_A) == TEST_POLICY.max_attempts
    d = storage.list_deliveries("sub-a")[0]
    assert d.status == DeliveryStatus.DEAD
    assert len(storage.list_attempts(d.id)) == TEST_POLICY.max_attempts
    assert "max attempts" in storage.list_dead_letters("sub-a")[0]["reason"]


def test_per_subscription_ordering_with_retry(storage, transport, receiver, signing_key):
    """较早事件重试期间，较晚事件不得越过它；恢复后按入队顺序送达。"""
    storage.add_subscription(_sub("sub-a", URL_A))
    first_attempt_of_e1 = {"done": False}

    def handler(idx, headers, body):
        import json
        eid = headers["X-Event-Id"]
        if eid == "e1" and not first_attempt_of_e1["done"]:
            first_attempt_of_e1["done"] = True
            return TransportResult(status_code=500)  # e1 首次失败，进入退避
        return TransportResult(status_code=receiver.handle(headers, body))

    transport.set_handler(URL_A, handler)
    for eid in ("e1", "e2", "e3"):
        storage.enqueue_event(Event(id=eid, event_type="t", payload={}))

    async def drive():
        d = Dispatcher(storage, transport, TEST_POLICY)
        await d.run_once()          # 只有 e1 可被领取（顺序门控），且失败
        assert receiver.received == []
        assert transport.count(URL_A) == 1
        force_due(storage)
        await d.run_once()          # e1 重试成功
        assert receiver.received == ["e1"]
        await d.run_once()          # e2
        await d.run_once()          # e3
        return d

    run(drive())
    assert receiver.received == ["e1", "e2", "e3"]


def test_failing_subscription_does_not_block_others(storage, transport, receiver, signing_key):
    """长期失败的订阅不阻塞其他订阅的投递。"""
    storage.add_subscription(_sub("sub-a", URL_A, ["a"]))
    storage.add_subscription(_sub("sub-b", URL_B, ["b"]))
    transport.set_handler(URL_A, lambda i, h, b: TransportResult(status_code=None, error="connection: refused"))
    transport.set_handler(URL_B, receiver.as_handler())

    storage.enqueue_event(Event(id="ea", event_type="a", payload={}))
    storage.enqueue_event(Event(id="eb", event_type="b", payload={}))

    async def drive():
        d = Dispatcher(storage, transport, TEST_POLICY)
        await d.run_once()
        return d

    run(drive())
    assert receiver.received == ["eb"]           # 健康订阅正常送达
    da = storage.list_deliveries("sub-a")[0]
    assert da.status == DeliveryStatus.PENDING   # 失败订阅进入退避等待重试
    assert da.attempt_count == 1
