from fastapi import APIRouter, Depends
from fastapi.responses import Response

import entwatcher.deps as deps
from entwatcher.dcollect import DCollectClient

router = APIRouter()


@router.post("/notify/{watcher}")
async def notify(
    watcher: str,
    dc: DCollectClient = Depends(deps.dcollect),
    http_client=Depends(deps.http_client),
):
    sub_data = await dc.read_watcher_entity(watcher)
    if sub_data is None:
        return Response(status_code=500)

    entities_data = {k: await dc.get_entity(v) for (k, v) in sub_data.entities.items()}

    if sub_data.trigger_url is None:
        print(watcher, "Missing trigger_url")
        return

    resp = await http_client.post(sub_data.trigger_url, json=entities_data)
    resp.raise_for_status()
