"""Configuration for the delivery layer, sourced from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


@dataclass
class Config:
    # Persistence
    db_path: str = "webhook.db"

    # Retry policy
    max_attempts: int = 5
    backoff_base: float = 1.0
    backoff_factor: float = 2.0
    backoff_max: float = 60.0
    jitter_ratio: float = 0.2
    delivery_timeout: float = 10.0

    # Concurrency / resource isolation
    global_concurrency: int = 10
    per_subscription_concurrency: int = 1
    scheduler_interval: float = 0.1
    claim_timeout: float = 30.0

    # Signing
    signature_tolerance: float = 300.0

    # Auth
    admin_api_key: str = "admin-dev-key"
    viewer_api_key: str = "viewer-dev-key"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            db_path=os.environ.get("WEBHOOK_DB_PATH", "webhook.db"),
            max_attempts=_env_int("WEBHOOK_MAX_ATTEMPTS", 5),
            backoff_base=_env_float("WEBHOOK_BACKOFF_BASE", 1.0),
            backoff_factor=_env_float("WEBHOOK_BACKOFF_FACTOR", 2.0),
            backoff_max=_env_float("WEBHOOK_BACKOFF_MAX", 60.0),
            jitter_ratio=_env_float("WEBHOOK_JITTER_RATIO", 0.2),
            delivery_timeout=_env_float("WEBHOOK_DELIVERY_TIMEOUT", 10.0),
            global_concurrency=_env_int("WEBHOOK_GLOBAL_CONCURRENCY", 10),
            per_subscription_concurrency=_env_int("WEBHOOK_PER_SUB_CONCURRENCY", 1),
            scheduler_interval=_env_float("WEBHOOK_SCHEDULER_INTERVAL", 0.1),
            claim_timeout=_env_float("WEBHOOK_CLAIM_TIMEOUT", 30.0),
            signature_tolerance=_env_float("WEBHOOK_SIGNATURE_TOLERANCE", 300.0),
            admin_api_key=os.environ.get("WEBHOOK_ADMIN_KEY", "admin-dev-key"),
            viewer_api_key=os.environ.get("WEBHOOK_VIEWER_KEY", "viewer-dev-key"),
        )
