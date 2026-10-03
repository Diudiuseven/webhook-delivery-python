"""Delivery transports.

Targets are either loopback HTTP(S) URLs or in-process receivers registered
with :class:`InProcessTransport`; no external services are involved.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass
class DeliveryResult:
    """Outcome of a single delivery attempt.

    ``outcome`` is one of:
    - ``success``          : receiver accepted the event
    - ``retryable``        : transient failure (5xx / timeout / connection error)
    - ``permanent``        : permanent failure (4xx) - no further attempts
    - ``interrupted``      : process crashed / claim timed out before a result
    """

    outcome: str
    status_code: Optional[int] = None
    error: Optional[str] = None
    latency_ms: float = 0.0


class PermanentDeliveryError(Exception):
    """Raised by a receiver to signal a permanent (non-retryable) failure."""


class RetryableDeliveryError(Exception):
    """Raised by a receiver to signal a transient failure."""


class Transport(Protocol):
    async def deliver(
        self,
        target: str,
        headers: dict,
        body: bytes,
        timeout: float,
    ) -> DeliveryResult: ...


class InProcessTransport:
    """Delivers to in-process receivers registered by name."""

    scheme = "inprocess://"

    def __init__(self) -> None:
        self._receivers: dict = {}

    def register(self, name: str, receiver) -> None:
        self._receivers[name] = receiver

    def unregister(self, name: str) -> None:
        self._receivers.pop(name, None)

    async def deliver(self, target: str, headers: dict, body: bytes, timeout: float) -> DeliveryResult:
        name = target[len(self.scheme):] if target.startswith(self.scheme) else target
        receiver = self._receivers.get(name)
        if receiver is None:
            return DeliveryResult(outcome="retryable", error=f"no receiver registered for {name!r}")
        started = time.monotonic()
        try:
            result = await asyncio.wait_for(receiver.handle(headers, body), timeout=timeout)
        except asyncio.TimeoutError:
            return DeliveryResult(
                outcome="retryable",
                error=f"delivery timed out after {timeout:.1f}s",
                latency_ms=(time.monotonic() - started) * 1000,
            )
        except asyncio.CancelledError:
            raise
        except PermanentDeliveryError as exc:
            return DeliveryResult(
                outcome="permanent",
                status_code=getattr(exc, "status_code", None),
                error=str(exc),
                latency_ms=(time.monotonic() - started) * 1000,
            )
        except Exception as exc:  # noqa: BLE001 - receiver errors must not kill the engine
            return DeliveryResult(
                outcome="retryable",
                error=f"{type(exc).__name__}: {exc}",
                latency_ms=(time.monotonic() - started) * 1000,
            )
        if result.latency_ms == 0.0:
            result.latency_ms = (time.monotonic() - started) * 1000
        return result


class HttpTransport:
    """Delivers over HTTP to loopback targets using httpx."""

    def __init__(self) -> None:
        import httpx

        self._httpx = httpx
        self._client = httpx.AsyncClient(timeout=None)

    async def deliver(self, target: str, headers: dict, body: bytes, timeout: float) -> DeliveryResult:
        started = time.monotonic()
        try:
            response = await self._client.post(
                target, content=body, headers=headers, timeout=timeout
            )
        except self._httpx.TimeoutException:
            return DeliveryResult(
                outcome="retryable",
                error=f"HTTP delivery timed out after {timeout:.1f}s",
                latency_ms=(time.monotonic() - started) * 1000,
            )
        except self._httpx.HTTPError as exc:
            return DeliveryResult(
                outcome="retryable",
                error=f"{type(exc).__name__}: {exc}",
                latency_ms=(time.monotonic() - started) * 1000,
            )
        latency = (time.monotonic() - started) * 1000
        if 200 <= response.status_code < 300:
            return DeliveryResult(outcome="success", status_code=response.status_code, latency_ms=latency)
        if 400 <= response.status_code < 500:
            return DeliveryResult(
                outcome="permanent", status_code=response.status_code,
                error=f"rejected with HTTP {response.status_code}", latency_ms=latency,
            )
        return DeliveryResult(
            outcome="retryable", status_code=response.status_code,
            error=f"retryable HTTP {response.status_code}", latency_ms=latency,
        )

    async def aclose(self) -> None:
        await self._client.aclose()


def select_transport(target: str, inproc: InProcessTransport, http: HttpTransport):
    if target.startswith(InProcessTransport.scheme):
        return inproc
    return http
