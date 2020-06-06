import os
import uuid

import pytest
from fastapi.testclient import TestClient

import entwatcher.main as main

client = TestClient(main.app)


def test_healthz():
    res = client.get("/v1/status/healthz")
    assert res.status_code == 200


def test_readyz():
    res = client.get("/v1/status/readyz")
    assert res.status_code == 200
