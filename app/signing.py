"""投递签名与密钥轮换：HMAC-SHA256，过渡期内新旧密钥均可验签。

签名头格式：
  X-Signature-Timestamp: <unix 秒>
  X-Signature: t=<ts>,kid=<key_id>,v1=<hex hmac>
签名内容：HMAC_SHA256(secret, f"{ts}." + body)
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional

from .models import SigningKey


def sign_body(secret: str, timestamp: str, body: bytes) -> str:
    msg = timestamp.encode() + b"." + body
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def build_signature_header(key: SigningKey, body: bytes,
                           timestamp: Optional[str] = None) -> tuple[str, str]:
    """返回 (timestamp, signature_header)。每次尝试用当前活跃密钥现场签名，
    因此重试跨越轮换点后自动改用新密钥，旧签名也不会被复用到失效。"""
    ts = timestamp or str(int(time.time()))
    sig = sign_body(key.secret, ts, body)
    return ts, f"t={ts},kid={key.id},v1={sig}"


def _parse_header(header: str) -> dict[str, str]:
    parts = {}
    for item in header.split(","):
        if "=" in item:
            k, v = item.split("=", 1)
            parts[k.strip()] = v.strip()
    return parts


def verify_signature(keys: list[SigningKey], timestamp: str, signature_header: str,
                     body: bytes, tolerance_seconds: float = 300.0) -> bool:
    """接收方验签：任一有效密钥匹配即通过（覆盖轮换过渡期），并校验时间戳容差。"""
    try:
        ts_value = int(timestamp)
    except ValueError:
        return False
    if abs(time.time() - ts_value) > tolerance_seconds:
        return False
    parts = _parse_header(signature_header)
    kid, sig = parts.get("kid"), parts.get("v1")
    if not sig or parts.get("t") != timestamp:
        return False
    for key in keys:
        if kid is not None and key.id != kid:
            continue
        expected = sign_body(key.secret, timestamp, body)
        if hmac.compare_digest(expected, sig):
            return True
    return False
