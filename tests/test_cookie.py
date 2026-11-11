import time

import acurl


def cookie(expiration):
    return acurl.Cookie(False, "example.com", False, "/", False, expiration, "name", "value")


def test_has_expired():
    now = int(time.time())
    assert not cookie(0).has_expired
    assert not cookie(now + 3600).has_expired
    assert cookie(now - 3600).has_expired
