import asyncio
import os
import orjson

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


ACTIONS_BASE_URL = os.environ.get("ACTIONS_BASE_URL", "http://127.0.0.1:8002")


INTERNAL_ACTION = f"{ACTIONS_BASE_URL}/internal/compute"
# maybe use action_id as key instead
EXTERNAL_URL_MAP = {"TriggerDAG": "http://compgraph:8000/triggerProcess"}


async def fetch(cas, dc, ptr):
    return await cas.get(await dc.get_pointer(ptr))


async def trigger_action(cas, dc: DCollectClient, http_client, action_id: str):
    ent = orjson.loads(await fetch(cas, dc, action_id))
    if ent is None:
        return
    cmd = Command(**ent)

    entities_data = {
        k: await fetch(cas, dc, ptr)
        for (k, v) in cmd.properties.items()
    }
    url = EXTERNAL_URL_MAP.get(cmd.kind)
    if url is not None:
        body = entities_data
    else:
        url = INTERNAL_ACTION
        body = {'kind': cmd.kind, 'properties': entities_data}

    resp = await http_client.post(url, json=body)
    resp.raise_for_status()
    return resp.json()


@router.post("/updates")
async def notify(
    data: UpdateNotification,
    dc: DCollectClient = Depends(deps.dcollect),
    cas = Depends(deps.cas),
    notification_router=Depends(deps.notification_router),
    http_client=Depends(deps.http_client),
):
    matches = notification_router.matches(data.entity)
    actions = [trigger_action(cas, dc, http_client, action_id) async for action_id in matches]
    await asyncio.gather(*actions)
