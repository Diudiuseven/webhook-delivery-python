"""In-process webhook receiver used for tests and loopback delivery.

Verifies signatures (accepting rotated keys), deduplicates by delivery id so
that redelivered events are processed exactly once, and can be configured to
exhibit slow / retryable / permanent failure behaviour.

Behaviour knobs operate on *attempts* (each call is one attempt), so a
redelivery after a timeout is observed as a duplicate but still counts as an
attempt.  The delivery is recorded before any delay/failure so that a
cancelled (timed-out) attempt still leaves its dedup trace behind.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Optional

from .signing import (
    DELIVERY_ID_HEADER,
    verify_signature,
)
from .transport import DeliveryResult, PermanentDeliveryError, RetryableDeliveryError


@dataclass
class ReceivedEvent:
    delivery_id: str
    event_id: str
    event_type: str
    payload: dict
    key_id: str
    received_at: float
    duplicate: bool = False


@dataclass
class ReceiverBehavior:
    """Configurable failure behaviour for a receiver.

    ``fail_times`` retryable failures are raised before succeeding;
    ``permanent`` makes every attempt fail permanently; ``delay`` simulates a
    slow receiver; ``delay_times`` limits the delay to the first N attempts;
    ``fail_status`` controls the reported status code.
    """

    fail_times: int = 0
    permanent: bool = False
    delay: float = 0.0
    delay_times: int = 0
    fail_status: int = 503


class Receiver:
    def __init__(self, name: str, key_store, config, behavior: Optional[ReceiverBehavior] = None) -> None:
        self.name = name
        self.key_store = key_store
        self.config = config
        self.behavior = behavior or ReceiverBehavior()
        self.seen: set[str] = set()
        self.received: list[ReceivedEvent] = []
        self.attempt_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def handle(self, headers: dict, body: bytes) -> DeliveryResult:
        keys = await self.key_store.all()
        if not verify_signature(body, headers, keys, self.config.signature_tolerance):
            return DeliveryResult(
                outcome="permanent",
                status_code=401,
                error="invalid or expired webhook signature",
            )

        delivery_id = headers.get(DELIVERY_ID_HEADER, "")
        event_id = headers.get("X-Webhook-Event-Id", "")
        event_type = headers.get("X-Webhook-Event-Type", "")
        key_id = headers.get("X-Webhook-Key-Id", "")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return DeliveryResult(outcome="permanent", status_code=400, error="invalid JSON body")

        # Record before any delay/failure so a cancelled (timed-out) attempt
        # still leaves its dedup trace behind.
        async with self._lock:
            attempt = self.attempt_counts.get(delivery_id, 0) + 1
            self.attempt_counts[delivery_id] = attempt
            duplicate = delivery_id in self.seen
            if not duplicate:
                self.seen.add(delivery_id)
            self.received.append(
                ReceivedEvent(
                    delivery_id=delivery_id,
                    event_id=event_id,
                    event_type=event_type,
                    payload=payload,
                    key_id=key_id,
                    received_at=time.time(),
                    duplicate=duplicate,
                )
            )

        if self.behavior.delay > 0 and (
            self.behavior.delay_times == 0 or attempt <= self.behavior.delay_times
        ):
            await asyncio.sleep(self.behavior.delay)

        if self.behavior.permanent:
            raise PermanentDeliveryError(f"receiver {self.name!r} permanently rejects delivery")
        if attempt <= self.behavior.fail_times:
            raise RetryableDeliveryError(
                f"receiver {self.name!r} transient failure (status {self.behavior.fail_status})"
            )
        return DeliveryResult(outcome="success", status_code=200)
