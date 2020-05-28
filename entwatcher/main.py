import httpx
from fastapi import FastAPI, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
from typing import Any, List, Optional, Tuple, Dict
import orjson

app = FastAPI()

http_client = httpx.AsyncClient()

from environs import Env

env = Env()

DCOLLECT_BASE_URL = env("DCOLLECT_BASE_URL", "http://127.0.0.1:8000")
ENTWATCHER_BASE_URL = env("ENTWATCHER_BASE_URL", "http://127.0.0.1:8001")


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()


async def get_entity(entity: str):
    resp = await http_client.get(f"{DCOLLECT_BASE_URL}/entity/{entity}")
    resp.raise_for_status()
    return resp.json()


async def store_watcher_entity(watcher: str, data):
    url = f"{DCOLLECT_BASE_URL}/entity/{watcher}"
    res = await http_client.post(url, json=data)
    res.raise_for_status()
    return res


async def read_watcher_entity(watcher: str) -> Optional[SubscribeRequest]:
    watcher_data = await get_entity(watcher)
    try:
        return SubscribeRequest(**watcher_data)
    except:
        return None


class SubscribeRequest(BaseModel):
    entities: Dict[str, str]
    trigger_url: str


@app.post("/subscribe/{watcher}")
async def subscribe_to_watch(watcher: str, subscribe_request: SubscribeRequest):
    body_url = f"{ENTWATCHER_BASE_URL}/notify/{watcher}"
    to_watch = [
        {"url": body_url, "entity": entity}
        for entity in subscribe_request.entities.values()
    ]
    data = {"to_watch": to_watch}
    url = f"{DCOLLECT_BASE_URL}/watchMultiple"
    resp = await client.post(url=url, json=data)
    resp.raise_for_status()
    await store_watcher_entity(watcher, subscribe_request.dict())


async def assemble_data(data: Dict[str, str]) -> Dict[str, Any]:
    return {k: await get_entity(v) for (k, v) in data.items()}


@app.post("/notify/{watcher}")
async def notify(watcher: str):
    sub_data = await read_watcher_entity(watcher)
    if sub_data is None:
        return Response(status_code=500)

    entities_data = await assembled_data(sub_data.entities)

    if sub_data.trigger_url is None:
        print(watcher, "Missing trigger_url")
        return

    resp = await http_client.post(sub_data.trigger_url, json=entities_data)
    resp.raise_for_status()


@app.get("/list/{watcher}")
async def list_entities(watcher: str):
    return await get_entity(watcher)


@app.post("/unsubscribe/{watcher}")
async def unsubscribe_to_watch(watcher: str, entities: List[str]):
    sub_data = await read_watcher_entity(watcher)
    if sub_data is None:
        return Response(status_code=500)

    body_url = f"{ENTWATCHER_BASE_URL}/unwatchMultiple"
    to_watch = [
        {"url": body_url, "entity": entity} for entity in sub_data.entities.values()
    ]
    for entity in entities:
        url = f"/entity/{entity}/unwatch"
        resp = await http_client.post(url, json={"to_watch": to_watch})
        resp.raise_for_status()
