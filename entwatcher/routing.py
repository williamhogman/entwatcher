import asyncio
import itertools
import secrets
from typing import Iterable, Tuple, AsyncGenerator, List

from pygtrie import Trie # type: ignore
from pydantic import BaseModel

FIELD_SEP = b"\x01"


class RoutingTableEntry(BaseModel):
    path: str
    action_id: str
    is_absolute: bool

    def to_bin(self) -> bytes:
        return FIELD_SEP.join(
            [self.path.encode("utf-8"), self.action_id.encode("utf-8")]
        )

    @classmethod
    def from_bin(cls, x: bytes) -> "RoutingTableEntry":
        fields = x.split(FIELD_SEP)
        if len(fields) != 2:
            raise RuntimeError("Wrong number of fields")
        return cls(path=fields[0].decode("utf-8"), action_id=fields[1].decode("utf-8"), is_true=True)


Path = Tuple[str, ...]


def to_path(orig: str) -> Path:
    return tuple(orig.split("."))


def from_path(path: Path) -> str:
    return ".".join(path)


class Table:
    def __init__(self):
        self.t = Trie()

    def replace(self, entries: List[RoutingTableEntry]):
        self.t = Trie()
        self.update(entries)

    def matches(self, path: str) -> Iterable[str]:
        return itertools.chain.from_iterable(v for k, v in self.t.prefixes(to_path(path)))

    def add(self, entry: RoutingTableEntry) -> None:
        if not self.t.has_key(entry):
            self.t[to_path(entry.path)] = [entry.action_id]
        else:
            self.t[to_path(entry.path)].append(entry.action_id)

    def update(self, entries: List[RoutingTableEntry]) -> None:
        for entry in entries:
            self.add(entry)


ROUTING_TABLE_KEY = "entwatcher_routing_table"
ROUTING_TABLE_EVENT = "entwatcher_routing_table_updated"

def _absolute_router_key(path: str) -> bytes:
    return b"entwatcher_absolute_routes:" + path.encode("utf-8")



class Persister:
    def __init__(self, redis):
        self.redis = redis
        self.should_run = True
        self.client_id = secrets.token_bytes(16)

    async def get_updates(self) -> AsyncGenerator[List[RoutingTableEntry], None]:
        ps = self.redis.pubsub()
        try:
            await ps.subscribe(ROUTING_TABLE_EVENT)
            while self.should_run:
                msg = await ps.get_message()
                if msg["type"] == "message" and msg["data"] != self.client_id:
                    yield await self.fetch_latest()
        finally:
            ps.close()

    async def add(self, entry: RoutingTableEntry):
        async with await self.redis.pipeline(transaction=True) as pipe:
            if entry.is_absolute:
                pipe.sadd(_absolute_router_key(entry.path), entry.action_id)
            else:
                await pipe.sadd(ROUTING_TABLE_KEY, entry.to_bin())
                await pipe.publish(ROUTING_TABLE_EVENT)

    async def get_absolute_entries(self, entry: str):
        return await self.redis.smembers(_absolute_router_key(entry))


    async def fetch_latest(self) -> List[RoutingTableEntry]:
        return await self.redis.smembers(ROUTING_TABLE_KEY)


class NotificationRouter:
    def __init__(self, redis):
        self.persister = Persister(redis)
        self.table = Table()
        self.update_task = None

    async def setup(self):
        self.table.update(await self.persister.fetch_latest())
        self.update_task = asyncio.create_task(self.update_loop())

    async def update_loop(self):
        async for updated in self.persister.get_updates():
            self.table.replace(updated)

    async def add(self, entry: RoutingTableEntry):
        # TODO: Remove from table on to avoid items being added to the
        # active table but not the local one
        self.table.add(entry)
        await self.table.add(entry)

    async def matches(self, path: str) -> AsyncGenerator[str, None]:
        # Start with the local prefix matches
        for x in self.table.matches(path):
            yield x

        entries = await self.persister.get_absolute_entries(path)
        for ent in entries:
            yield ent
