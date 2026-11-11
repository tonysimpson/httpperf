import asyncio
import base64
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
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/json"
    assert r.json()["origin"] == "127.0.0.1"


def test_cookies(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/cookies/set?name=value"))
    assert r.cookies == {"name": "value"}


def test_session_cookies(httpbin):
    s = session()
    _await(s.get(httpbin.url + "/cookies/set?name=value"))
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


def test_set_encoding(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/encoding/utf8"))
    assert r.text == r.body.decode("utf-8")
    r.encoding = "latin1"
    assert r.encoding == "latin1"
    assert r.text == r.body.decode("latin1")


def test_redirect_history(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/redirect/3"))
    assert [h.url for h in r.history] == [
        httpbin.url + "/redirect/3",
        httpbin.url + "/relative-redirect/2",
        httpbin.url + "/relative-redirect/1",
    ]


def test_head(httpbin):
    s = session()
    r = _await(s.head(httpbin.url + "/get"))
    assert r.status_code == 200
    assert r.body == b""
    assert int(r.headers["Content-Length"]) > 0
    r = _await(s.head(httpbin.url + "/redirect/1"))
    assert r.status_code == 302


def test_post_binary(httpbin):
    s = session()
    data = bytes(range(256))
    r = _await(s.post(httpbin.url + "/post", data=data, headers={"Content-Type": "application/octet-stream"}))
    assert r.json()["data"] == "data:application/octet-stream;base64," + base64.b64encode(data).decode()
    assert (r.upload_size, r.download_size) == (256, len(r.body))
    assert isinstance(r.upload_size, int) and isinstance(r.download_size, int)


def test_client_lag(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/delay/0"))
    assert r.start_time <= r.end_time
    assert 0 <= r.client_lag < r.end_time - r.start_time


@pytest.mark.parametrize(
    "kwargs",
    [{"auth": ("\ud800", "password")}, {"headers": {"X-Test": "\ud800"}}, {"cookies": {"name": "\ud800"}}],
)
def test_unencodable_strings_raise(httpbin, kwargs):
    s = session()
    with pytest.raises(UnicodeEncodeError):
        _await(s.get(httpbin.url + "/get", **kwargs))
    assert _await(s.get(httpbin.url + "/get")).status_code == 200
