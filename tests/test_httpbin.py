import asyncio
import base64
import gc
import json
import time
from urllib.parse import urlencode

import pytest

import httpperf


@pytest.fixture(autouse=True)
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


def session():
    el = httpperf.Engine()
    return httpperf.Session(engine=el)


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
    assert r.history[0].cookies == {"name": "value"}
    assert r.cookies == {}


def test_session_cookies(httpbin):
    s = session()
    _await(s.get(httpbin.url + "/cookies/set?name=value"))
    cookie_list = s.get_cookie_list()
    assert len(cookie_list) == 1
    assert cookie_list[0].name == "name"
    assert s.cookies() == {"name": "value"}
    s.erase_all_cookies()
    cookie_list = s.get_cookie_list()
    assert len(cookie_list) == 0
    assert s.cookies() == {}


def test_set_cookies(httpbin):
    s = session()
    _await(s.get(httpbin.url + "/cookies/set?name=value"))
    _await(s.get(httpbin.url + "/cookies/set?name2=value", cookies={"name3": "value"}))
    assert s.cookies() == {"name": "value", "name2": "value", "name3": "value"}


def test_add_and_erase_session_cookies(httpbin):
    s = session()
    lasting = httpperf.session_cookie_for_url(httpbin.url, "lasting", "1")
    lasting = httpperf.Cookie(False, lasting.domain, True, "/", False, int(time.time()) + 3600, "lasting", "1")
    s.add_cookie_list([httpperf.session_cookie_for_url(httpbin.url, "brief", "1"), lasting])
    assert _await(s.get(httpbin.url + "/cookies")).json()["cookies"] == {"brief": "1", "lasting": "1"}
    s.erase_session_cookies()
    assert s.cookies() == {"lasting": "1"}
    s.close()
    with pytest.raises(RuntimeError):
        s.erase_all_cookies()


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


@pytest.mark.parametrize("method", ["put", "post", "patch", "delete"])
def test_methods_with_body(httpbin, method):
    s = session()
    r = _await(getattr(s, method)(httpbin.url + "/" + method, json={"a": 1}))
    assert r.status_code == 200
    assert r.json()["json"] == {"a": 1}


def test_custom_json_functions(httpbin):
    calls = []

    def dumps(obj):
        calls.append("engine dumps")
        return json.dumps(obj).encode()

    def loads(body):
        calls.append("session loads")
        return json.loads(body)

    engine = httpperf.Engine(json_dumps=dumps, json_loads=lambda body: calls.append("engine loads"))
    s = httpperf.Session(engine=engine, json_loads=loads)
    r = _await(s.post(httpbin.url + "/post", json={"a": 1}))
    assert r.json()["json"] == {"a": 1}
    assert calls == ["engine dumps", "session loads"]
    engine.close()


def test_post_binary(httpbin):
    s = session()
    data = bytes(range(256))
    r = _await(s.post(httpbin.url + "/post", data=data, headers={"Content-Type": "application/octet-stream"}))
    assert r.json()["data"] == "data:application/octet-stream;base64," + base64.b64encode(data).decode()
    assert (r.upload_size, r.download_size) == (256, len(r.body))
    assert isinstance(r.upload_size, int) and isinstance(r.download_size, int)


@pytest.mark.parametrize("connections", ["shared", "session", "none"])
def test_connections(httpbin, connections):
    s = httpperf.Session(connections=connections)
    for _ in range(2):
        assert _await(s.get(httpbin.url + "/get")).status_code == 200


def test_connections_invalid():
    with pytest.raises(ValueError):
        httpperf.Session(connections="pooled")


def test_tls_invalid():
    with pytest.raises(ValueError):
        httpperf.Session(tls="pooled")


def test_connection_limits_invalid():
    with pytest.raises(ValueError):
        httpperf.Engine(max_host_connections=-1)


@pytest.mark.parametrize("options", [{"idle_timeout": -1}, {"max_connection_age": -1}])
def test_expiry_invalid(options):
    with pytest.raises(ValueError):
        httpperf.Session(**options)


def test_max_idle_connections_invalid():
    with pytest.raises(ValueError):
        httpperf.Engine(max_idle_connections=0)


@pytest.mark.parametrize("options", [{"max_host_connections": 2}, {"connections": "session", "max_connections": -1}])
def test_session_connection_limits_invalid(options):
    with pytest.raises(ValueError):
        httpperf.Session(**options)


def test_http_version_invalid():
    with pytest.raises(ValueError):
        httpperf.Session(http_version="HTTP/0.9")


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


def test_params(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/get?a=1#frag", params={"b": "x y", "c": ["1", "2"]}))
    assert r.json()["args"] == {"a": "1", "b": "x y", "c": ["1", "2"]}
    assert _await(s.get(httpbin.url + "/get", params="raw=1")).json()["args"] == {"raw": "1"}


def test_form_data(httpbin):
    s = session()
    r = _await(s.post(httpbin.url + "/post", data={"a": "1", "b": ["2", "3"]}))
    assert r.json()["form"] == {"a": "1", "b": ["2", "3"]}
    assert r.json()["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    r = _await(s.post(httpbin.url + "/post", data=[("a", "1")], headers={"content-type": "text/plain"}))
    assert r.json()["headers"]["Content-Type"] == "text/plain"


def test_files(httpbin, tmp_path):
    path = tmp_path / "upload.txt"
    path.write_bytes(b"from disk")
    s = session()
    with open(path, "rb") as f:
        files = {"disk": f, "inline": ("a.csv", "x,y", "text/csv"), "raw": ("b.bin", b"\x00\x01")}
        r = _await(s.post(httpbin.url + "/post", data={"field": "value"}, files=files))
    body = r.json()
    assert body["form"] == {"field": "value"}
    assert body["files"] == {"disk": "from disk", "inline": "x,y", "raw": "\x00\x01"}
    assert body["headers"]["Content-Type"].startswith("multipart/form-data; boundary=")
    with pytest.raises(ValueError):
        _await(s.post(httpbin.url + "/post", json={}, files=files))


def test_status_helpers(httpbin):
    s = session()
    r = _await(s.get(httpbin.url + "/get"))
    assert (r.ok, r.reason) == (True, "OK")
    assert r.elapsed.total_seconds() == r.total_time
    r.raise_for_status()
    r = _await(s.get(httpbin.url + "/status/404"))
    assert (r.ok, r.reason) == (False, "NOT FOUND")
    with pytest.raises(httpperf.HTTPError) as e:
        r.raise_for_status()
    assert e.value.response is r
    assert str(e.value).startswith("404 Client Error: NOT FOUND for url: ")


def test_reused_handles_do_not_leak_between_sessions(httpbin):
    el = httpperf.Engine()
    first = httpperf.Session(engine=el)
    for _ in range(3):
        _await(first.get(httpbin.url + "/cookies/set?secret=1"))
        _await(first.get(httpbin.url + "/headers", auth=("user", "pass"), headers={"X-Private": "1"}))
    gc.collect()
    _await(asyncio.sleep(0.05))
    second = httpperf.Session(engine=el)
    for _ in range(3):
        assert _await(second.get(httpbin.url + "/cookies")).json()["cookies"] == {}
        headers = _await(second.get(httpbin.url + "/headers")).json()["headers"]
        assert "Authorization" not in headers and "X-Private" not in headers
    assert _await(first.get(httpbin.url + "/cookies")).json()["cookies"] == {"secret": "1"}
