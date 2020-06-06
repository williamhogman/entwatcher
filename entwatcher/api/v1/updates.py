import asyncio

from fastapi import APIRouter, Depends
from fastapi.responses import Response
from pydantic import BaseModel

import entwatcher.deps as deps
from entwatcher.dcollect import DCollectClient

router = APIRouter()


class UpdateNotification(BaseModel):
    entity: str


class Command(BaseModel):
    kind: str
    properties: dict


URL_MAP = {"TriggerDAG": "http://compgraph:8000/triggerProcess"}


async def trigger_action(dc: DCollectClient, http_client, action_id: str):
    ent = await dc.get_entity(action_id)
    if ent is None:
        return ent
    cmd = Command(**ent)

    url = URL_MAP.get(cmd.kind)
    if url is None:
        return None
    entities_data = {
        k: await v if k[0] == "$" else dc.get_entity(v)
        for (k, v) in cmd.properties.values()
    }
    http_client.post(url, json=entities_data)


@router.post("/updates")
async def notify(
    data: UpdateNotification,
    dc: DCollectClient = Depends(deps.dcollect),
    notification_router=Depends(deps.notification_router),
    http_client=Depends(deps.http_client),
):
    matches = notification_router.matches(data.entity)
    actions = [trigger_action(dc, http_client, action_id) async for action_id in matches]
    await asyncio.gather(*actions)
