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


async def fetch(cas, dc, ptr):
    ptr = await dc.get_pointer(ptr)
    if ptr is None:
        return None
    return await cas.get(ptr)


async def trigger_action(cas, dc: DCollectClient, http_client, action_id: str):
    ent = orjson.loads(await fetch(cas, dc, action_id))
    if ent is None:
        return
    cmd = Command(**ent)

    entities_data = {
        k: orjson.loads(await fetch(cas, dc, v)) for (k, v) in cmd.properties.items()
    }
    body = {"kind": cmd.kind, "properties": entities_data}
    resp = await http_client.post(INTERNAL_ACTION, json=body)
    resp.raise_for_status()
    return resp.json()


@router.post("/updates")
async def notify(
    data: UpdateNotification,
    dc: DCollectClient = Depends(deps.dcollect),
    cas=Depends(deps.cas),
    notification_router=Depends(deps.notification_router),
    http_client=Depends(deps.http_client),
):
    matches = notification_router.matches(data.entity)
    actions = [
        trigger_action(cas, dc, http_client, action_id.decode("utf-8"))
        async for action_id in matches
    ]
    await asyncio.gather(*actions)
