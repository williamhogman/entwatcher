from typing import Iterable, Tuple, AsyncGenerator, List

from pydantic import BaseModel

class RoutingTableEntry(BaseModel):
    path: str
    action_id: str
    is_absolute: bool

def _absolute_router_key(path: str) -> bytes:
    return b"entwatcher_absolute_routes:" + path.encode("utf-8")

def _wildcard_router_key(path:str) -> bytes:
    return b"entwatcher_absolute_routes:" + path.encode("utf-8")

def path_prefixes(path: str):
    segments = path.split(".")
    for i in range(segments):
        yield ".".join(segement[0:i])

def get_path_keys(path: str):
    pfxs = list(path_prefixes(path))
    last = pfxs[-1]
    but_last = pfxs[:-1]
    for x in but_last:
        yield _wildcard_router_key(x)
    yield _absolute_router_key(x)


class NotificationRouter:
    def __init__(self, redis):
        self.redis = redis

    async def setup(self):
        self.table.update(await self.persister.fetch_latest())

    async def add(self, entry: RoutingTableEntry):
        async with await self.redis.pipeline(transaction=True) as pipe:
            key = _absolute_router_key(entry.path) if entry.is_absolute else _wildcard_router_key(entry.path)
            pipe.sadd(key, entry.action_id)

    async def add_many(self, entries: Iterable[RoutingTableEntry]):
        for ent in entries:
            await self.add(ent)

    async def matches(self, path: str) -> AsyncGenerator[str, None]:
        for x in await self.redis.sunion(list(get_path_keys(entry))):
            yield x
