import asyncio
import os

import httpx

from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter
from entwatcher.subscription_updater import SubscriptionUpdater

ACTIONS_BASE_URL = os.environ.get("ACTIONS_BASE_URL", "http://127.0.0.1:8002")
INTERNAL_ACTION = f"{ACTIONS_BASE_URL}/internal/compute"


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

    async def post_action(self, body):
        resp = await self.http_client.post(INTERNAL_ACTION, json=body)
        resp.raise_for_status()
        return resp.json()

    async def trigger_action(self, action_id: str, updated_entity: str,) -> bool:
        if action_id == "_conthesis.UpdateWatcher":
            await self.su.update(updated_entity)
            return True

        action_data = await self.ef.fetch_json(action_id)
        if action_data is None:
            return False

        await self.post_action(action_data)
        return True

    async def notify(self, entity: str):
        matches = self.nr.matches(entity)
        actions = [
            self.trigger_action(action_id.decode("utf-8"), entity)
            async for action_id in matches
        ]
        return all(await asyncio.gather(*actions))
