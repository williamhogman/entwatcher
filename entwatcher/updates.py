import asyncio

from nats.aio.client import Client as NATS

from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter
from entwatcher.subscription_updater import SubscriptionUpdater

ACTION_BY_ENTITY_TOPIC = "conthesis.actions.by-entity"


class UpdatesWorker:
    ef: EntityFetcher
    su: SubscriptionUpdater
    nr: NotificationRouter

    def __init__(
        self,
        nc: NATS,
        ef: EntityFetcher,
        su: SubscriptionUpdater,
        nr: NotificationRouter,
    ):
        self.nc = nc
        self.ef = ef
        self.su = su
        self.nr = nr

    async def trigger_action(self, action_id: bytes, updated_entity: str) -> bool:
        if action_id == b"_conthesis.UpdateWatcher":
            await self.su.update(updated_entity)
            return True

        res = await self.nc.request(ACTION_BY_ENTITY_TOPIC, action_id, timeout=5)
        return True

    async def notify(self, entity: str):
        matches = self.nr.matches(entity)
        actions = [
            self.trigger_action(action_id, entity) async for action_id in matches
        ]
        return all(await asyncio.gather(*actions))
