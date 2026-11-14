import time

import pytest

import httpperf

NOW = int(time.time())


@pytest.mark.parametrize(
    "expiration, expired", [(0, False), (NOW + 3600, False), (NOW - 3600, True)], ids=["session", "future", "past"]
)
def test_has_expired(expiration, expired):
    cookie = httpperf.Cookie(False, "example.com", False, "/", False, expiration, "name", "value")
    assert cookie.has_expired == expired
