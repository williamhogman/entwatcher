from typing import Optional

import httpx
from fastapi import Depends

from entwatcher.dcollect import DCollectClient

dcollect_: Optional[DCollectClient] = None
http_client_: Optional[httpx.AsyncClient] = None


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
