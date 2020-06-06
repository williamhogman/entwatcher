""""""
from fastapi import APIRouter

from .notify import router as notify
from .status import router as status
from .subscription import router as subscription
from .updates import router as updates

router = APIRouter()

router.include_router(status, prefix="/status", tags=["status"])
router.include_router(subscription, tags=["subscription"])
router.include_router(notify, tags=["notify"])
router.include_router(updates, tags=["updates"])
