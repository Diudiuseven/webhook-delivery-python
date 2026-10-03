"""核心数据模型与枚举定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class DeliveryStatus(str, enum.Enum):
    PENDING = "pending"          # 待投递（含等待重试）
    CLAIMED = "claimed"          # 已被 worker 领取，投递中
    DELIVERED = "delivered"      # 投递成功
    DEAD = "dead"                # 重试耗尽，进入死信


class AttemptOutcome(str, enum.Enum):
    SUCCESS = "success"
    RETRYABLE = "retryable"      # 可重试失败（5xx / 超时 / 连接异常）
    PERMANENT = "permanent"      # 永久失败（4xx 等），不再重试


@dataclass
class Subscription:
    id: str
    url: str
    event_types: list[str]           # 订阅的事件类型，["*"] 表示全部
    max_concurrency: int = 1         # 按订阅并发上限（有序模式下有效并发为 1）
    created_at: float = field(default_factory=now)


@dataclass
class Event:
    id: str
    event_type: str
    payload: dict[str, Any]
    created_at: float = field(default_factory=now)


@dataclass
class Delivery:
    id: str
    event_id: str
    subscription_id: str
    seq: int                          # 订阅内单调递增序号，保证按入队顺序投递
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempt_count: int = 0
    next_attempt_at: float = 0.0
    claim_token: Optional[str] = None
    claim_expires_at: float = 0.0
    replay_of: Optional[str] = None   # 死信重放时指向原投递
    created_at: float = field(default_factory=now)


@dataclass
class Attempt:
    id: str
    delivery_id: str
    attempt_no: int
    outcome: AttemptOutcome
    status_code: Optional[int]
    error: Optional[str]
    duration_ms: float
    key_id: Optional[str]             # 本次签名使用的密钥
    created_at: float = field(default_factory=now)


@dataclass
class SigningKey:
    id: str
    secret: str
    active: bool                      # 当前用于签名的密钥
    valid_until: Optional[float] = None  # 轮换过渡期后旧密钥失效时间，None 表示长期有效
    created_at: float = field(default_factory=now)


@dataclass
class AuditRecord:
    id: str
    actor: str
    action: str
    target: str
    allowed: bool
    detail: str = ""
    created_at: float = field(default_factory=now)
