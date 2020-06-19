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
            NOTIFY_TOPIC, cb=self.handle, queue=NOTIFY_QUEUE
        )

    async def shutdown(self):
        await self.nc.unsubscribe(self.sub)
        await self.nc.drain()

    async def handle(self, msg):
        try:
            res = await self.handler(msg.data)
            await self.nc.publish(msg.reply, res)
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
        dcollect = DCollectClient(self.http_client)
        cas = CAS(self.http_client)
        entity_fetcher = EntityFetcher(dcollect, cas)

        redis = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
        self.notification_router = NotificationRouter(redis)
        self.subscription_updater = SubscriptionUpdater(
            entity_fetcher, self.notification_router
        )
        self.updates = UpdatesWorker(
            entity_fetcher,
            self.subscription_updater,
            self.notification_router,
            self.http_client,
        )
        self.message_handler = MessageHandler(self.handler, NATS())

    async def setup(self):
        await self.message_handler.setup()

    async def handler(self, data: bytes) -> bytes:
        res = await self.updates.notify(data.decode("utf-8"))
        if res:
            return b"OK"
        else:
            return b"ERR"

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.message_handler.shutdown()
            await self.http_client.aclose()
        finally:
            self.shutdown_f.set_result(True)
