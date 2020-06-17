from typing import Optional, Dict, Any

import entwatcher.deps as deps
from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter


class SubscriptionUpdater:
    ef: EntityFetcher
    nr: NotificationRouter

    def __init__(self, ef: EntityFetcher, nr: NotificationRouter):
        self.ef = ef
        self.nr = nr

    async def update(self, watcher_entity: str):
        watcher_data = await self.ef.fetch_json(watcher_entity)
        entities = watcher_data["properties"].values()
        await self.nr.add_many_for_action(entities, watcher_entity)
