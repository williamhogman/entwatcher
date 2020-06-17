import os
from typing import Optional

import httpx
from fastapi import Depends

import aredis  # type: ignore
from entwatcher.dcollect import DCollectClient
from entwatcher.routing import NotificationRouter
from entwatcher.cas import CAS
from entwatcher.entity_fetcher import EntityFetcher
from entwatcher.subscription_updater import SubscriptionUpdater

dcollect_: Optional[DCollectClient] = None
http_client_: Optional[httpx.AsyncClient] = None
redis_: Optional[aredis.StrictRedis] = None
notification_router_: Optional[NotificationRouter] = None
cas_: Optional[CAS] = None
entity_fetcher_: Optional[EntityFetcher] = None
subscription_updater_: Optional[SubscriptionUpdater] = None


def http_client() -> httpx.AsyncClient:
    global http_client_
    if http_client_ is None:
        http_client_ = httpx.AsyncClient()
    return http_client_


def dcollect(httpc=Depends(http_client)):
    global dcollect_
    if dcollect_ is None:
        dcollect_ = DCollectClient(httpc)
    return dcollect_


def redis() -> aredis.StrictRedis:
    global redis_
    if redis_ is None:
        redis_ = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
    return redis_


async def notification_router(
    redis: aredis.StrictRedis = Depends(redis),
) -> NotificationRouter:
    global notification_router_
    if notification_router_ is None:
        notification_router_ = NotificationRouter(redis)
    return notification_router_


async def cas(http_client: httpx.AsyncClient = Depends(http_client)) -> CAS:
    global cas_
    if cas_ is None:
        cas_ = CAS(http_client)
    return cas_


async def entity_fetcher(
    dc: DCollectClient = Depends(dcollect), cas: CAS = Depends(cas)
) -> EntityFetcher:
    global entity_fetcher_
    if entity_fetcher_ is None:
        entity_fetcher_ = EntityFetcher(dc, cas)
    return entity_fetcher_


async def subscription_updater(
    ef: EntityFetcher = Depends(entity_fetcher),
    nr: NotificationRouter = Depends(notification_router),
):
    global subscription_updater_
    if subscription_updater_ is None:
        subscription_updater_ = SubscriptionUpdater(ef, nr)
    return subscription_updater_
