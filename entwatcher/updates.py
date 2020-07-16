import asyncio

import orjson
from nats.aio.client import Client as NATS

from entwatcher.routing import NotificationRouter

ACTION_TOPIC = "conthesis.action.TriggerAsyncAction"

UPDATE_WATCHER_PROTO = {
    "kind": "entwatcher.UpdateWatchEntity",
    "properties": [
        {"name": "name", "kind": "META_FIELD", "value": "updated_entity"},
        {"name": "entity", "kind": "META_ENTITY", "value": "updated_entity",},
    ],
}


class UpdatesWorker:
    nc: NATS
    nr: NotificationRouter

    def __init__(
        self, nc: NATS, nr: NotificationRouter,
    ):
        self.nc = nc
        self.nr = nr

    async def trigger_action(self, jid: str, action_id: bytes, updated_entity: str) -> bool:
        trigger = None
        meta = {"updated_entity": updated_entity}

        act_id = action_id.decode("utf-8")
        trigger = {
            "jid": f"{jid}/{act_id}",
            "meta": meta,
            "action_source": "ENTITY",
            "action": act_id,
        }

        res = await self.nc.request(ACTION_TOPIC, orjson.dumps(trigger), timeout=5,)
        return True

    async def notify(self, jid: str, entity: str):
        matches = self.nr.matches(entity)
        actions = [
            self.trigger_action(jid, action_id, entity) async for action_id in matches
        ]
        return all(await asyncio.gather(*actions))
