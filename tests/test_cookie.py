import time

import httpperf


def cookie(expiration):
    return httpperf.Cookie(False, "example.com", False, "/", False, expiration, "name", "value")


def test_has_expired():
    now = int(time.time())
    assert not cookie(0).has_expired
    assert not cookie(now + 3600).has_expired
    assert cookie(now - 3600).has_expired
