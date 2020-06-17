""""""
from fastapi import APIRouter

from .status import router as status
from .updates import router as updates

router = APIRouter()

router.include_router(status, prefix="/status", tags=["status"])
router.include_router(updates, tags=["updates"])
