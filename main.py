import httpx
from fastapi import FastAPI, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
import model

app = FastAPI()

http_client = None


@app.on_event("startup")
async def startup():
    http_client = httpx.AsyncClient()
    await model.setup()


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    await model.teardown()
