import asyncio
import os

import orjson
from fastapi import APIRouter, Depends
from fastapi.responses import Response
from pydantic import BaseModel

import entwatcher.deps as deps
from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.subscription_updater import SubscriptionUpdater

router = APIRouter()


class UpdateNotification(BaseModel):
    entity: str


class Command(BaseModel):
    kind: str
    properties: dict


ACTIONS_BASE_URL = os.environ.get("ACTIONS_BASE_URL", "http://127.0.0.1:8002")
INTERNAL_ACTION = f"{ACTIONS_BASE_URL}/internal/compute"


async def trigger_action(
    ef: EntityFetcher,
    su: SubscriptionUpdater,
    http_client,
    action_id: str,
    updated_entity: str,
):
    if action_id == "_conthesis.UpdateWatcher":
        await su.update(updated_entity)
        return

    cmd = Command(**(await ef.fetch_json(action_id)))
    entities_data = {
        k: orjson.loads(await ef.fetch(v)) for (k, v) in cmd.properties.items()
    }
    body = {"kind": cmd.kind, "properties": entities_data}
    resp = await http_client.post(INTERNAL_ACTION, json=body)
    resp.raise_for_status()
    return resp.json()


@router.post("/updates")
async def notify(
    data: UpdateNotification,
    ef: EntityFetcher = Depends(deps.entity_fetcher),
    su: SubscriptionUpdater = Depends(deps.subscription_updater),
    notification_router=Depends(deps.notification_router),
    http_client=Depends(deps.http_client),
):
    matches = notification_router.matches(data.entity)
    actions = [
        trigger_action(ef, su, http_client, action_id.decode("utf-8"), data.entity)
        async for action_id in matches
    ]
    await asyncio.gather(*actions)
