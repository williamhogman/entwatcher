from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
async def healthz():
    return {"status": True}


@router.get("/readyz")
async def readyz():
    return {"status": True}
