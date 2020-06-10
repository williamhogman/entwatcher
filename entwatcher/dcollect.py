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
        if resp.status_code == 404:
            return None
        return await resp.aread()

    async def store_ptr(self, entity: str, ptr: bytes):
        if not ptr:
            raise RuntimeError("ptr is empty or None")
        url = f"{self.url}/entity-ptr/{entity}"
        res = await self.http_client.post(url, data=ptr)
        res.raise_for_status()
        return res
