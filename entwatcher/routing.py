import itertools
from typing import AsyncGenerator, Iterable

from pydantic import BaseModel


class RoutingTableEntry(BaseModel):
    path: str
    action_id: str
    is_absolute: bool

    @classmethod
    def many_of(
        cls, action_id: str, is_absolute: bool, xs: Iterable[str]
    ) -> Iterable["RoutingTableEntry"]:
        for x in xs:
            yield cls(path=x, action_id=action_id, is_absolute=is_absolute)


def _absolute_router_key(path: str) -> bytes:
    return b"entwatcher_absolute_routes:" + path.encode("utf-8")


def _wildcard_router_key(path: str) -> bytes:
    return b"entwatcher_wildcard_routes:" + path.encode("utf-8")


def path_prefixes(path: str):
    segments = path.split(".")
    for i in range(1, len(segments) + 1):
        yield ".".join(segments[0:i])


def get_path_keys(path: str):
    pfxs = list(path_prefixes(path))
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

    async def update_entity(self, entity_name: str, watcher_data: dict):
        if watcher_data is None:
            return False
        props = watcher_data.get("properties", [])
        wildcard_triggers = watcher_data.get("wildcard_triggers", [])

        absolute = RoutingTableEntry.many_of(
            entity_name,
            True,
            (
                prop["value"]
                for prop in props
                if prop.get("kind") == "ENTITY" and "value" in prop
            ),
        )

        wildcards = RoutingTableEntry.many_of(
            entity_name, False, (x for x in wildcard_triggers if x)
        )
        await self.add_many(itertools.chain(absolute, wildcards))

    async def matches(self, path: str) -> AsyncGenerator[bytes, None]:
        for x in await self.redis.sunion(list(get_path_keys(path))):
            yield x
