"""重试策略：最大尝试次数、指数退避、抖动、单次投递超时、结果分类。"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional

from .models import AttemptOutcome


@dataclass
class RetryPolicy:
    max_attempts: int = 5              # 最大尝试次数（含首次）
    base_delay: float = 0.1            # 首次重试基础间隔（秒）
    max_delay: float = 30.0            # 退避上限（秒）
    backoff_multiplier: float = 2.0    # 指数退避倍数
    jitter: float = 0.2                # 抖动比例 [0,1]，在 ±jitter 内随机扰动
    timeout: float = 5.0               # 单次投递超时（秒）
    lease_seconds: float = 30.0        # 领取租约时长，超时未完成可被重新领取

    def next_delay(self, attempt_no: int, rng: Optional[random.Random] = None) -> float:
        """第 attempt_no 次尝试失败后的退避间隔（指数退避 + 抖动，封顶 max_delay）。"""
        rng = rng or random
        delay = min(self.base_delay * (self.backoff_multiplier ** max(0, attempt_no - 1)),
                    self.max_delay)
        spread = delay * self.jitter
        return max(0.0, delay + rng.uniform(-spread, spread))


def classify_status(status_code: Optional[int]) -> AttemptOutcome:
    """分类投递结果：
    - 2xx            -> 成功
    - 4xx（除 408/429）-> 永久失败，不再重试
    - 5xx / 408 / 429 / 无响应（连接异常、超时）-> 可重试失败
    """
    if status_code is None:
        return AttemptOutcome.RETRYABLE
    if 200 <= status_code < 300:
        return AttemptOutcome.SUCCESS
    if status_code in (408, 429) or status_code >= 500:
        return AttemptOutcome.RETRYABLE
    return AttemptOutcome.PERMANENT
