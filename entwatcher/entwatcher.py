import asyncio
import os

import aredis
import httpx
from nats.aio.client import Client as NATS

from entwatcher.cas import CAS
from entwatcher.dcollect import DCollectClient
from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter
from entwatcher.subscription_updater import SubscriptionUpdater
from entwatcher.updates import UpdatesWorker

NOTIFY_TOPIC = "entity-updates-v1"
NOTIFY_QUEUE = "entwatcher-queue"
NOTIFY_UPDATE_ACCEPTED = "entity-updates-v1.accepted"


class MessageHandler:
    nc: NATS

    def __init__(self, handler, nc: NATS):
        self.nc = nc
        self.handler = handler

    async def setup(self):
        await self.nc.connect(
            os.environ.get("NATS_URL", "nats://nats:4222"),
            loop=asyncio.get_running_loop(),
        )
        self.sub = await self.nc.subscribe(
            NOTIFY_TOPIC, cb=self.handle,  # queue=NOTIFY_QUEUE
        )

    async def shutdown(self):
        await self.nc.unsubscribe(self.sub)
        await self.nc.drain()

    async def handle(self, msg):
        try:
            res = await self.handler(msg.data)
            if res is not None:
                await self.nc.publish(NOTIFY_UPDATE_ACCEPTED, res)
        except Exception as ex:
            print(ex)


class Entwatcher:
    http_client: httpx.AsyncClient
    redis: aredis.StrictRedis
    notification_router: NotificationRouter
    subscription_updater: SubscriptionUpdater

    def __init__(self):
        self.http_client = httpx.AsyncClient()
        self.shutdown_f = asyncio.get_running_loop().create_future()
        nats = NATS()
        dcollect = DCollectClient(self.http_client)
        cas = CAS(self.http_client)
        entity_fetcher = EntityFetcher(dcollect, cas)

        redis = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
        self.notification_router = NotificationRouter(redis)
        self.subscription_updater = SubscriptionUpdater(
            entity_fetcher, self.notification_router
        )
        self.updates = UpdatesWorker(
            nats, entity_fetcher, self.subscription_updater, self.notification_router,
        )
        self.message_handler = MessageHandler(self.handler, nats)

    async def setup(self):
        await self.message_handler.setup()

    async def handler(self, data: bytes) -> bytes:
        parts = data.split(b"\0")
        if len(parts) != 2:
            raise RuntimeError("Wrong size of parts")
        entity = parts[0].decode("utf-8")
        res = await self.updates.notify(entity)
        if res:
            return data
        else:
            return None

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.message_handler.shutdown()
            await self.http_client.aclose()
        finally:
            self.shutdown_f.set_result(True)
