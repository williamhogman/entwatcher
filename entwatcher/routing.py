from typing import AsyncGenerator, Iterable, List, Tuple

from pydantic import BaseModel


class RoutingTableEntry(BaseModel):
    path: str
    action_id: str
    is_absolute: bool


def _absolute_router_key(path: str) -> bytes:
    return b"entwatcher_absolute_routes:" + path.encode("utf-8")


def _wildcard_router_key(path: str) -> bytes:
    return b"entwatcher_wildcard_routes:" + path.encode("utf-8")


def path_prefixes(path: str):
    segments = path.split(".")
    for i in range(len(segments)):
        yield ".".join(segments[0:i])


def get_path_keys(path: str):
    pfxs = list(path_prefixes(path))
    last = pfxs[-1]
    but_last = pfxs[:-1]
    for x in but_last:
        yield _wildcard_router_key(x)
    yield _absolute_router_key(path)


class NotificationRouter:
    def __init__(self, redis):
        self.redis = redis

    async def add(self, entry: RoutingTableEntry):
        key = (
            _absolute_router_key(entry.path)
            if entry.is_absolute
            else _wildcard_router_key(entry.path)
        )
        await self.redis.sadd(key, entry.action_id)

    async def add_many(self, entries: Iterable[RoutingTableEntry]):
        for ent in entries:
            await self.add(ent)

    async def add_many_for_action(self, entities: Iterable[str], action_id: str):
        """Adds watches between the entities listed and the action_id provided"""
        await self.add_many(
            RoutingTableEntry(path=val, action_id=action_id, is_absolute=True)
            for val in entities
        )

    async def matches(self, path: str) -> AsyncGenerator[bytes, None]:
        if path.startswith("_conthesis.watcher."):
            yield b"_conthesis.UpdateWatcher"
        for x in await self.redis.sunion(list(get_path_keys(path))):
            yield x
