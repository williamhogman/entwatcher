import asyncio
import os

import httpx

from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter
from entwatcher.subscription_updater import SubscriptionUpdater

ACTIONS_BASE_URL = os.environ.get("ACTIONS_BASE_URL", "http://127.0.0.1:8002")
INTERNAL_ACTION = f"{ACTIONS_BASE_URL}/internal/compute"
ACTION_BY_ENTITY = f"{ACTIONS_BASE_URL}/internal/compute-entity"


class UpdatesWorker:
    ef: EntityFetcher
    su: SubscriptionUpdater
    nr: NotificationRouter

    def __init__(
        self,
        ef: EntityFetcher,
        su: SubscriptionUpdater,
        nr: NotificationRouter,
        http_client: httpx.AsyncClient,
    ):
        self.ef = ef
        self.su = su
        self.nr = nr
        self.http_client = http_client

    async def post_action(self, action_id: str):
        resp = await self.http_client.post(
            ACTION_BY_ENTITY, json={"action_id": action_id}
        )
        resp.raise_for_status()
        return resp.json()

    async def trigger_action(self, action_id: str, updated_entity: str) -> bool:
        if action_id == "_conthesis.UpdateWatcher":
            await self.su.update(updated_entity)
            return True

        await self.post_action(action_id)
        return True

    async def notify(self, entity: str):
        matches = self.nr.matches(entity)
        actions = [
            self.trigger_action(action_id.decode("utf-8"), entity)
            async for action_id in matches
        ]
        return all(await asyncio.gather(*actions))
