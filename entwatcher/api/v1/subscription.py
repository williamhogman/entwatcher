import os
from typing import Any, Dict, Iterable, List

from fastapi import APIRouter, Depends
from fastapi.responses import Response

import entwatcher.deps as deps
from entwatcher.dcollect import DCollectClient
from entwatcher.model import SubscribeRequest

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
    sub_data = await dc.read_watcher_entity(dc, watcher)
    if sub_data is None:
        return Response(status_code=500)

    body_url = f"{ENTWATCHER_BASE_URL}/v1/notify/{watcher}"
    dc.unwatch_multiple(assemble_watch_request(body_url, sub_data.entities.values()))


@router.post("/subscribe/{watcher}")
async def subscribe_to_watch(
    watcher: str, subscribe_request: SubscribeRequest, dc=Depends(deps.dcollect)
):
    body_url = f"{ENTWATCHER_BASE_URL}/v1/notify/{watcher}"
    data = assemble_watch_request(body_url, subscribe_request.entities.values())
    await dc.watch_multiple(data)
    await dc.store_entity(watcher, subscribe_request.dict())
