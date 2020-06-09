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

    async def get_pointer(self, entity: str) -> bytes:
        resp = await self.http_client.get(f"{self.url}/entity-ptr/{entity}")
        resp.raise_for_status()
        return await resp.aread()


    async def store_entity(self, entity: str, data: dict):
        url = f"{self.url}/entity/{entity}"
        res = await self.http_client.post(url, json=data)
        res.raise_for_status()
        return res
