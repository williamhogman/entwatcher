import asyncio
import os
import traceback

import aredis
import orjson
from nats.aio.client import Client as NATS

from entwatcher.routing import NotificationRouter
from entwatcher.updates import UpdatesWorker

NOTIFY_TOPIC = "entity-updates-v1"
NOTIFY_QUEUE = "entwatcher-queue"
NOTIFY_UPDATE_ACCEPTED = "entity-updates-v1.accepted"

ACTION_TOPIC = "conthesis.action.entwatcher.UpdateWatchEntity"


class MessageHandler:
    nc: NATS

    def __init__(self, handler, action_handler, nc: NATS):
        self.nc = nc
        self.handler = handler
        self.action_handler = action_handler

    async def setup(self):
        await self.nc.connect(
            os.environ.get("NATS_URL", "nats://nats:4222"),
            loop=asyncio.get_running_loop(),
        )
        self.sub = await self.nc.subscribe(
            NOTIFY_TOPIC, cb=self.handle,  # queue=NOTIFY_QUEUE
            is_async=True
        )
        self.action_sub = await self.nc.subscribe(ACTION_TOPIC, cb=self.handle_action, is_async=True)

    async def shutdown(self):
        await self.nc.drain()

    async def handle(self, msg):
        try:
            res = await self.handler(msg.data)
            if res is not None:
                await self.nc.publish(NOTIFY_UPDATE_ACCEPTED, res)
        except Exception:
            traceback.print_exc()

    async def handle_action(self, msg):
        try:
            res = await self.action_handler(msg.data)
            await self.nc.publish(msg.reply, b"{}")
        except Exception:
            traceback.print_exc()


class Entwatcher:
    notification_router: NotificationRouter

    def __init__(self):
        self.shutdown_f = asyncio.get_running_loop().create_future()
        nats = NATS()
        redis = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
        self.notification_router = NotificationRouter(redis)
        self.updates = UpdatesWorker(nats, self.notification_router)
        self.message_handler = MessageHandler(self.handler, self.action_handler, nats)

    async def setup(self):
        await self.message_handler.setup()

    async def handler(self, data: bytes) -> bytes:
        parts = data.split(b"\0", 1)
        if len(parts) != 2:
            raise RuntimeError("Wrong size of parts")
        entity = parts[0].decode("utf-8")
        res = await self.updates.notify(data.decode("utf-8"), entity)
        if res:
            return data
        else:
            return None

    async def action_handler(self, data: bytes) -> bytes:
        params = orjson.loads(data)
        entity = params["entity"]
        name = params["name"]
        await self.notification_router.update_entity(name, entity)

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.message_handler.shutdown()
        finally:
            self.shutdown_f.set_result(True)
