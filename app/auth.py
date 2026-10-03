"""运维操作的权限校验：token -> (actor, scopes) 静态映射，可注入。

约定 scope：
- "replay"      死信重放
- "keys:rotate" 密钥轮换
- "audit:read"  审计查询
- "*"           全部权限
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Principal:
    actor: str
    scopes: frozenset[str] = field(default_factory=frozenset)

    def has(self, scope: str) -> bool:
        return "*" in self.scopes or scope in self.scopes


class Authorizer:
    def __init__(self, tokens: dict[str, tuple[str, list[str]]]) -> None:
        # token -> (actor, scopes)
        self._tokens = dict(tokens)

    def authenticate(self, token: str | None) -> Principal | None:
        if not token:
            return None
        entry = self._tokens.get(token)
        if entry is None:
            return None
        actor, scopes = entry
        return Principal(actor=actor, scopes=frozenset(scopes))

    def require(self, principal: Principal | None, scope: str) -> bool:
        return principal is not None and principal.has(scope)
