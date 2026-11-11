import asyncio
from urllib.parse import urlencode

import pytest

import acurl


@pytest.fixture(autouse=True)
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


def session():
    el = acurl.EventLoop()
    return el.session()


def _await(awaitable):
    return asyncio.get_event_loop().run_until_complete(awaitable)


def test_get(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/ip"))
    r.status_code
    r.headers
    r.json


def test_cookies(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/cookies/set?name=value"))
    assert r.cookies == {"name": "value"}


def test_session_cookies(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/cookies/set?name=value"))
    cookie_list = _await(s.get_cookie_list())
    assert len(cookie_list) == 1
    assert cookie_list[0].name == "name"
    _await(s.erase_all_cookies())
    cookie_list = _await(s.get_cookie_list())
    assert len(cookie_list) == 0


def test_set_cookies(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/cookies/set?name=value"))
    r = _await(s.get(httpbin.url + "/cookies/set?name2=value", cookies={"name3": "value"}))
    assert r.cookies == {"name": "value", "name2": "value", "name3": "value"}


def test_basic_auth(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/basic-auth/user/password", auth=("user", "password")))
    assert r.status_code == 200


def test_failed_basic_auth(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/basic-auth/user/password", auth=("notuser", "notpassword")))
    assert r.status_code == 401


def test_redirect(httpbin):
    s = session()
    url = httpbin.url + "/ip"
    r = _await(s.get(httpbin.url + "/redirect-to?" + urlencode({"url": url})))
    assert r.url == url


def test_cookies_kwarg_sent(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/cookies", cookies={"name": "value"}))
    assert r.json()["cookies"] == {"name": "value"}
