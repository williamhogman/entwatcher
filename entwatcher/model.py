from typing import Dict

from pydantic import BaseModel


class SubscribeRequest(BaseModel):
    entities: Dict[str, str]
    trigger_url: str
