"""Webhook payload signing and verification with key rotation support.

Each delivery is signed with the *currently active* key at attempt time, so an
event retried across a rotation point is re-signed with the new key.  The key
id is carried in the ``X-Webhook-Key-Id`` header; receivers accept signatures
from any key whose status is ``active`` or ``rotated`` and reject ``retired``
keys.  Rotation therefore invalidates neither in-flight nor future deliveries.
"""
from __future__ import annotations

import hmac
import hashlib
import secrets
import time
from dataclasses import dataclass
from typing import Optional


SIGNATURE_HEADER = "X-Webhook-Signature"
KEY_ID_HEADER = "X-Webhook-Key-Id"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
DELIVERY_ID_HEADER = "X-Webhook-Delivery-Id"
EVENT_ID_HEADER = "X-Webhook-Event-Id"
EVENT_TYPE_HEADER = "X-Webhook-Event-Type"


@dataclass
class SigningKey:
    key_id: str
    secret: str
    status: str  # active | rotated | retired
    created_at: float
    rotated_at: Optional[float] = None


def generate_secret() -> str:
    return secrets.token_hex(32)


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    """Return the ``v1=`` HMAC-SHA256 signature for ``timestamp.body``."""
    signed_payload = f"{timestamp}.".encode("ascii") + body
    digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"v1={digest}"


def sign_headers(secret: str, key_id: str, timestamp: int, body: bytes) -> dict:
    return {
        KEY_ID_HEADER: key_id,
        TIMESTAMP_HEADER: str(timestamp),
        SIGNATURE_HEADER: compute_signature(secret, timestamp, body),
    }


def verify_signature(
    body: bytes,
    headers: dict,
    keys: dict[str, SigningKey],
    tolerance: float,
    now: Optional[float] = None,
) -> bool:
    """Verify a webhook request.

    ``keys`` maps key id to SigningKey.  Returns True only when the key id is
    known, the key is not retired, the timestamp is within tolerance and the
    signature matches.
    """
    key_id = headers.get(KEY_ID_HEADER)
    timestamp_raw = headers.get(TIMESTAMP_HEADER)
    signature = headers.get(SIGNATURE_HEADER)
    if not key_id or not timestamp_raw or not signature:
        return False
    try:
        timestamp = int(timestamp_raw)
    except (TypeError, ValueError):
        return False
    if now is None:
        now = time.time()
    if abs(now - timestamp) > tolerance:
        return False
    key = keys.get(key_id)
    if key is None or key.status == "retired":
        return False
    expected = compute_signature(key.secret, timestamp, body)
    return hmac.compare_digest(expected, signature)


def _row_to_key(row) -> SigningKey:
    return SigningKey(
        key_id=row["key_id"],
        secret=row["secret"],
        status=row["status"],
        created_at=row["created_at"],
        rotated_at=row["rotated_at"],
    )


class KeyStore:
    """Persistent signing-key store backed by the Database."""

    def __init__(self, db) -> None:
        self.db = db

    async def initialize(self) -> SigningKey:
        """Ensure there is exactly one active key, creating one if needed."""
        row = await self.db.fetchone(
            "SELECT * FROM signing_keys WHERE status='active' ORDER BY created_at LIMIT 1"
        )
        if row is not None:
            return _row_to_key(row)
        key = SigningKey(
            key_id=f"key_{int(time.time() * 1000)}_{secrets.token_hex(4)}",
            secret=generate_secret(),
            status="active",
            created_at=time.time(),
        )
        await self.db.execute(
            "INSERT INTO signing_keys (key_id, secret, status, created_at) VALUES (?, ?, ?, ?)",
            (key.key_id, key.secret, "active", key.created_at),
        )
        return key

    async def get_active(self) -> SigningKey:
        row = await self.db.fetchone(
            "SELECT * FROM signing_keys WHERE status='active' ORDER BY created_at LIMIT 1"
        )
        if row is None:
            return await self.initialize()
        return _row_to_key(row)

    async def get(self, key_id: str) -> Optional[SigningKey]:
        row = await self.db.fetchone("SELECT * FROM signing_keys WHERE key_id=?", (key_id,))
        return _row_to_key(row) if row is not None else None

    async def all(self) -> dict[str, SigningKey]:
        rows = await self.db.fetchall("SELECT * FROM signing_keys ORDER BY created_at")
        return {row["key_id"]: _row_to_key(row) for row in rows}

    async def rotate(self) -> SigningKey:
        """Create a new active key; the previous active key becomes rotated."""
        now = time.time()
        async with self.db.transaction():
            await self.db._execute(
                "UPDATE signing_keys SET status='rotated', rotated_at=? WHERE status='active'",
                (now,),
            )
            new_key = SigningKey(
                key_id=f"key_{int(now * 1000)}_{secrets.token_hex(4)}",
                secret=generate_secret(),
                status="active",
                created_at=now,
            )
            await self.db._execute(
                "INSERT INTO signing_keys (key_id, secret, status, created_at) VALUES (?, ?, ?, ?)",
                (new_key.key_id, new_key.secret, "active", now),
            )
        return new_key

    async def retire(self, key_id: str) -> None:
        await self.db.execute(
            "UPDATE signing_keys SET status='retired' WHERE key_id=? AND status!='active'",
            (key_id,),
        )
