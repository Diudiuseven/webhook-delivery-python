"""投递调度器。

- 全局并发上限（信号量）+ 按订阅并发上限（每订阅信号量），
  单个高流量订阅最多占用自己的配额，不会挤占其他订阅。
- 顺序保证来自存储层的领取门控：订阅内更早的投递未终态时，较晚投递不可领取，
  因此较早事件的重试会挡住较晚事件，且失败订阅不影响其他订阅的领取。
- 每次尝试现场签名（密钥轮换后自动用新密钥），结果按策略分类：
  成功 / 可重试（退避+抖动后重新入队）/ 永久失败或次数耗尽（死信）。
- 租约机制：领取后超时未完成的投递会被 release_expired_claims 重新放出，
  进程崩溃重启时 reset_claims_on_startup 兜底，事件不会永久卡死。
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

from .models import Attempt, AttemptOutcome, Delivery, new_id, now
from .retry import RetryPolicy, classify_status
from .signing import build_signature_header
from .storage import Storage
from .transport import Transport


class Dispatcher:
    def __init__(self, storage: Storage, transport: Transport,
                 policy: Optional[RetryPolicy] = None,
                 global_concurrency: int = 16, poll_interval: float = 0.05) -> None:
        self.storage = storage
        self.transport = transport
        self.policy = policy or RetryPolicy()
        self.global_concurrency = global_concurrency
        self.poll_interval = poll_interval
        self._sema = asyncio.Semaphore(global_concurrency)
        self._sub_semas: dict[str, asyncio.Semaphore] = {}
        self._inflight = 0
        self._tasks: set[asyncio.Task] = set()
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    # ---- 生命周期 ----
    async def start(self) -> None:
        recovered = self.storage.reset_claims_on_startup()
        if recovered:
            pass  # 崩溃遗留的 claimed 投递已回到 pending，等待重新领取
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopped.is_set():
            self.storage.release_expired_claims()
            n = self._claim_and_dispatch()
            if n == 0:
                await asyncio.sleep(self.poll_interval)
            else:
                await asyncio.sleep(0)

    # ---- 领取与派发 ----
    def _sub_semaphore(self, sub_id: str) -> asyncio.Semaphore:
        sem = self._sub_semas.get(sub_id)
        if sem is None:
            sub = self.storage.get_subscription(sub_id)
            limit = max(1, sub.max_concurrency) if sub else 1
            sem = asyncio.Semaphore(limit)
            self._sub_semas[sub_id] = sem
        return sem

    def _claim_and_dispatch(self) -> int:
        available = self.global_concurrency - self._inflight
        if available <= 0:
            return 0
        claimed = self.storage.claim_due_deliveries(available, self.policy.lease_seconds)
        for d in claimed:
            self._inflight += 1
            task = asyncio.create_task(self._guarded_deliver(d))
            self._tasks.add(task)
            task.add_done_callback(self._on_task_done)
        return len(claimed)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        self._inflight -= 1

    async def _guarded_deliver(self, delivery: Delivery) -> None:
        async with self._sema:
            async with self._sub_semaphore(delivery.subscription_id):
                await self._deliver(delivery)

    # ---- 单次投递 ----
    async def _deliver(self, delivery: Delivery) -> None:
        event = self.storage.get_event(delivery.event_id)
        sub = self.storage.get_subscription(delivery.subscription_id)
        attempt_no = delivery.attempt_count + 1
        key = self.storage.get_active_key()
        if event is None or sub is None:
            attempt = Attempt(id=new_id(), delivery_id=delivery.id, attempt_no=attempt_no,
                              outcome=AttemptOutcome.PERMANENT, status_code=None,
                              error="event or subscription missing", duration_ms=0.0,
                              key_id=key.id if key else None)
            self.storage.dead_letter_delivery(delivery.id, delivery.claim_token or "",
                                              attempt, reason="missing event or subscription")
            return

        body = json.dumps({
            "event_id": event.id,
            "delivery_id": delivery.id,
            "event_type": event.event_type,
            "payload": event.payload,
            "attempt": attempt_no,
            "replay_of": delivery.replay_of,
        }).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Event-Id": event.id,          # 接收方幂等去重键
            "X-Delivery-Id": delivery.id,
            "X-Event-Type": event.event_type,
        }
        if key is not None:
            ts, sig = build_signature_header(key, body)
            headers["X-Signature-Timestamp"] = ts
            headers["X-Signature"] = sig

        started = time.monotonic()
        result = await self.transport.post(sub.url, body, headers, self.policy.timeout)
        duration_ms = (time.monotonic() - started) * 1000.0

        outcome = classify_status(result.status_code)
        attempt = Attempt(id=new_id(), delivery_id=delivery.id, attempt_no=attempt_no,
                          outcome=outcome, status_code=result.status_code,
                          error=result.error, duration_ms=duration_ms,
                          key_id=key.id if key else None)
        token = delivery.claim_token or ""
        if outcome is AttemptOutcome.SUCCESS:
            self.storage.complete_delivery(delivery.id, token, attempt)
        elif outcome is AttemptOutcome.PERMANENT:
            self.storage.dead_letter_delivery(
                delivery.id, token, attempt,
                reason=f"permanent failure: status={result.status_code}")
        elif attempt_no >= self.policy.max_attempts:
            self.storage.dead_letter_delivery(
                delivery.id, token, attempt,
                reason=f"max attempts ({self.policy.max_attempts}) exhausted")
        else:
            delay = self.policy.next_delay(attempt_no)
            self.storage.reschedule_delivery(delivery.id, token, attempt, now() + delay)

    # ---- 测试/同步驱动接口 ----
    async def run_once(self) -> int:
        """同步执行一轮：回收过期租约、领取、并发投递并等待完成。返回领取数。"""
        self.storage.release_expired_claims()
        claimed = self.storage.claim_due_deliveries(self.global_concurrency,
                                                    self.policy.lease_seconds)
        if claimed:
            await asyncio.gather(*(self._guarded_deliver(d) for d in claimed))
        return len(claimed)

    async def drain(self, max_rounds: int = 100) -> int:
        """反复执行 run_once 直到没有可领取的投递，返回总领取数。"""
        total = 0
        for _ in range(max_rounds):
            n = await self.run_once()
            total += n
            if n == 0:
                break
        return total

    # ---- 可观测 ----
    def metrics(self) -> dict:
        m = self.storage.backlog_metrics()
        m["dispatcher"] = {
            "inflight": self._inflight,
            "global_concurrency": self.global_concurrency,
            "available_slots": self.global_concurrency - self._inflight,
        }
        return m
