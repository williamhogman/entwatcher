import os
from typing import Dict, Optional

import httpx

from entwatcher.model import SubscribeRequest


class DCollectClient:
    http_client: httpx.AsyncClient
    url: str

    def __init__(self, http_client: httpx.AsyncClient, url: str = None):
        self.http_client = http_client
        if url is None:
            self.url = os.environ.get("DCOLLECT_BASE_URL", "http://127.0.0.1:8000")
        else:
            self.url = url

    async def get_entity(self, entity: str) -> dict:
        resp = await self.http_client.get(f"{self.url}/entity/{entity}")
        resp.raise_for_status()
        return resp.json()

    async def store_entity(self, entity: str, data: dict):
        url = f"{self.url}/entity/{entity}"
        res = await self.http_client.post(url, json=data)
        res.raise_for_status()
        return res

    async def read_watcher_entity(self, watcher: str) -> Optional[SubscribeRequest]:
        watcher_data = await self.get_entity(watcher)
        try:
            return SubscribeRequest(**watcher_data)
        except:
            return None
