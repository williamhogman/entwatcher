import os
from typing import Any, Dict, Iterable, List

import orjson
from fastapi import APIRouter, Depends
from fastapi.responses import Response

import entwatcher.deps as deps
from entwatcher.dcollect import DCollectClient
from entwatcher.routing import RoutingTableEntry, NotificationRouter

ENTWATCHER_BASE_URL = os.environ.get("ENTWATCHER_BASE_URL", "http://127.0.0.1:8001")

router = APIRouter()


@router.post("/subscribe/{watcher}")
async def subscribe_to_watch(
    watcher: str,
    dc=Depends(deps.dcollect),
    nr: NotificationRouter = Depends(deps.notification_router),
    cas=Depends(deps.cas),
):
    ptr = await dc.get_pointer(watcher)
    data = orjson.loads(await cas.get(ptr))
    entities = data["properties"].values()
    routing_entries = [
        RoutingTableEntry(path=val, action_id=watcher, is_absolute=True)
        for val in entities
    ]
    await nr.add_many(routing_entries)
