"""服务入口：装配存储、调度器、鉴权与 HTTP 应用。

投递目标使用本地回环地址；测试可通过依赖注入替换传输层与鉴权配置。
"""
import asyncio

import uvicorn

from app.api import create_app
from app.auth import Authorizer
from app.dispatcher import Dispatcher
from app.models import SigningKey, new_id
from app.retry import RetryPolicy
from app.storage import Storage
from app.transport import HttpTransport

import os
import secrets

DB_PATH = os.environ.get("WEBHOOK_DB", "webhook.db")
ADMIN_TOKEN = os.environ.get("WEBHOOK_ADMIN_TOKEN", "dev-admin-token")


def build() -> tuple[Storage, Dispatcher, object]:
    storage = Storage(DB_PATH)
    storage.connect()
    if storage.get_active_key() is None:
        storage.add_signing_key(SigningKey(id=new_id(), secret=secrets.token_hex(32), active=True))
    dispatcher = Dispatcher(storage, HttpTransport(), RetryPolicy(), global_concurrency=16)
    authorizer = Authorizer({ADMIN_TOKEN: ("admin", ["*"])})
    app = create_app(storage, dispatcher, authorizer)

    @app.on_event("startup")
    async def _start() -> None:
        await dispatcher.start()

    @app.on_event("shutdown")
    async def _stop() -> None:
        await dispatcher.stop()

    return storage, dispatcher, app


storage, dispatcher, app = build()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
