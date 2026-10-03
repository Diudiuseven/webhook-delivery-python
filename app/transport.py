"""可注入的投递传输层。默认走本地回环 HTTP，测试可注入进程内实现。

传输层只负责"发一次"，把超时/连接异常归一化为 status_code=None 的结果，
由调度器按重试策略分类处理；慢响应由单次超时兜底，不会拖垮投递进程。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass
class TransportResult:
    status_code: Optional[int]   # None 表示连接异常/超时
    error: Optional[str] = None


class Transport(Protocol):
    async def post(self, url: str, body: bytes, headers: dict[str, str],
                   timeout: float) -> TransportResult:
        ...


class HttpTransport:
    """基于 httpx 的 HTTP 传输（目标为本地回环地址或测试注入的 ASGI 应用）。"""

    def __init__(self, client=None) -> None:
        # client 可注入 httpx.AsyncClient；缺省按需创建
        self._client = client

    async def _get_client(self):
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient()
        return self._client

    async def post(self, url: str, body: bytes, headers: dict[str, str],
                   timeout: float) -> TransportResult:
        import httpx
        try:
            client = await self._get_client()
            resp = await client.post(url, content=body, headers=headers, timeout=timeout)
            return TransportResult(status_code=resp.status_code)
        except httpx.TimeoutException:
            return TransportResult(status_code=None, error="timeout")
        except httpx.TransportError as exc:
            return TransportResult(status_code=None, error=f"connection: {exc.__class__.__name__}")
