import os
import uuid

import pytest
from fastapi.testclient import TestClient

import entwatcher.main as main

client = TestClient(main.app)


def test_healthz():
    res = client.get("/status/healthz")
    assert res.status_code == 200


def test_readyz():
    res = client.get("/status/readyz")
    assert res.status_code == 200
