from fastapi import FastAPI

import entwatcher.hooks as hooks
from entwatcher.api import router as api

app = FastAPI()
app.include_router(api)

app.on_event("shutdown")(hooks.shutdown)
