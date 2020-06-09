import os
from typing import Any, Dict, Iterable, List

from fastapi import APIRouter, Depends
from fastapi.responses import Response

import entwatcher.deps as deps
from entwatcher.dcollect import DCollectClient
from entwatcher.model import SubscribeRequest
from entwatcher.routing import RoutingTableEntry, NotificationRouter

ENTWATCHER_BASE_URL = os.environ.get("ENTWATCHER_BASE_URL", "http://127.0.0.1:8001")

router = APIRouter()


def assemble_watch_request(
    url: str, entities: Iterable[str]
) -> Dict[str, List[Dict[str, str]]]:
    to_watch = [{"url": url, "entity": entity} for entity in entities]
    return {"to_watch": to_watch}


@router.post("/unsubscribe/{watcher}")
async def unsubscribe_to_watch(
    watcher: str,
    entities: List[str],
    dc=Depends(deps.dcollect),
    http_client=Depends(deps.http_client),
):
    raise RuntimeError("Not yet implemented")


@router.post("/subscribe/{watcher}")
async def subscribe_to_watch(
        watcher: str, subscribe_request: SubscribeRequest, dc=Depends(deps.dcollect),
        nr: NotificationRouter = Depends(deps.notification_router)
):
    action = {
        "kind": subscribe_request.trigger_url,
        "properties": subscribe_request.entities
    }
    action_ent = f"entwatcher.{watcher}"
    await dc.store_entity(watcher, action)

    routing_entries = [RoutingTableEntry(path=val, action_id=watcher, is_absolute=True) for val in subscribe_request.entities.values()]

    await nr.add_many(routing_entries)
