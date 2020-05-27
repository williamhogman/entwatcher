import httpx
from fastapi import FastAPI, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
import model
from typing import Any, List, Optional, Tuple
import orjson

app = FastAPI()

http_client = None

from environs import Env

env = Env()

DCOLLECT_BASE_URL = env("DCOLLECT_BASE_URL", "http://127.0.0.1:8000")
ENTWATCHER_BASE_URL = env("ENTWATCHER_BASE_URL", "http://127.0.0.1:8001")


@app.on_event("startup")
async def startup():
    http_client = httpx.AsyncClient()
    await model.setup()


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    await model.teardown()


async def get_entity(entity: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{DCOLLECT_BASE_URL}/entity/{entity}")
    # data
    return resp.json()


async def store_watcher_entity(watcher: str, entities: List[str]):
    # data = {'entities': hash(orjson.dumps(entities))}
    async with httpx.AsyncClient() as client:
        # ingest
        url = f"{DCOLLECT_BASE_URL}/entity/{watcher}"
        data = {"entities": entities}
        return await client.post(url, json=data)


class SubscribeRequest(BaseModel):
    entities: List[str]


@app.post("/subscribe/{watcher}")
async def subscribe_to_watch(watcher: str, subscribe_request: SubscribeRequest):
    async with httpx.AsyncClient() as client:
        body_url = f"{ENTWATCHER_BASE_URL}/notify/{watcher}"
        data = {"url": body_url}
        for entity in subscribe_request.entities:
            url = f"{DCOLLECT_BASE_URL}/entity/{entity}/watch"
            resp = await client.post(url=url, json=data)
    await store_watcher_entity(watcher, subscribe_request.entities)


@app.post("/notify/{watcher}")
async def notify(watcher: str):
    watcher_data = await get_entity(watcher)

    entities_data = []
    for entity in watcher_data["entities"]:
        entities_data.append(await get_entity(entity))

    # do something with entities_data
    print(entities_data)


@app.get("/list/{watcher}")
async def list_entities(watcher: str):
    return await get_entity(watcher)


# TODO: Need to implement delete or forget entity in dcollect first
# @app.post("/unsubscribe/{watcher}")
# async def unsubscribe_to_watch(watcher: str, entities: List[str]):
#     baseurl = ''
#     body_url = f'{baseurl}/notify/{watcher}'
#     body = {'url': body_url}

#     for entity in entities:
#         # TODO: add url
#         url = f'/entity/{entity}/unwatch'
#         resp = await http_client.post(url, body=body)
