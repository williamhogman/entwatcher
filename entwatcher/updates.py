import asyncio

import orjson
from nats.aio.client import Client as NATS

from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.routing import NotificationRouter
from entwatcher.subscription_updater import SubscriptionUpdater

ACTION_TOPIC = "conthesis.action.TriggerAction"


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
        trigger = None
        meta = {"updated_entity": updated_entity}

        if action_id == b"_conthesis.UpdateWatcher":
            trigger = {
                "meta": meta,
                "action_source": "LITERAL",
                "action": {
                    "kind": "entwatcher.UpdateWatchEntity",
                    "properties": [
                        {
                            "name": "entity",
                            "kind": "META_FIELD",
                            "value": "updated_entity",
                        }
                    ],
                },
            }
        else:
            trigger = {
                "meta": meta,
                "action_source": "ENTITY",
                "action": action_id.decode("utf-8"),
            }

        res = await self.nc.request(ACTION_TOPIC, orjson.dumps(trigger), timeout=5,)
        return True

    async def notify(self, entity: str):
        matches = self.nr.matches(entity)
        actions = [
            self.trigger_action(action_id, entity) async for action_id in matches
        ]
        return all(await asyncio.gather(*actions))
