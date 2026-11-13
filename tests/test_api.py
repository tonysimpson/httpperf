import asyncio
import datetime
import json

import pytest

import httpperf
from httpperf.version import __version__


def call(method, url, **kwargs):
    async def main():
        async with httpperf.Session() as session:
            return await getattr(session, method)(url, **kwargs)

    return asyncio.run(main())


def with_session(fn):
    async def main():
        async with httpperf.Session() as session:
            return await fn(session)

    return asyncio.run(main())


def test_version():
    assert isinstance(__version__, str)


@pytest.mark.parametrize("method", ["get", "put", "patch", "post", "delete"])
def test_methods(httpbin, method):
    r = call(method, httpbin.url + "/anything")
    assert r.status_code == 200
    assert r.json()["method"] == method.upper()
    assert r.request.method == method.upper()


def test_head(httpbin):
    r = call("head", httpbin.url + "/get")
    assert (r.status_code, r.body, r.request.method) == (200, b"", "HEAD")


def test_options(httpbin):
    r = call("options", httpbin.url + "/anything")
    assert (r.status_code, r.request.method) == (200, "OPTIONS")
    assert "OPTIONS" in r.headers["Allow"]


def test_request_with_custom_method(httpbin):
    r = with_session(lambda s: s.request("PROPFIND", httpbin.url + "/anything"))
    assert (r.status_code, r.request.method) == (405, "PROPFIND")


def test_request_without_options(httpbin):
    r = call("get", httpbin.url + "/get")
    req = r.request
    assert req.method == "GET"
    assert req.url == httpbin.url + "/get"
    assert req.header_list is None
    assert req.headers == {}
    assert req.cookie_list is None
    assert req.cookies == {}
    assert req.auth is None
    assert req.data is None


def test_request_with_options(httpbin):
    r = call(
        "post",
        httpbin.url + "/post",
        headers={"X-A": "1"},
        cookies={"c": "v"},
        auth=("user", "pass"),
        data="body",
    )
    req = r.request
    assert req.header_list == ("X-A: 1",)
    assert req.headers == {"X-A": "1"}
    assert [c.name for c in req.cookie_list] == ["c"]
    assert req.cookies == {"c": "v"}
    assert req.auth == ("user", "pass")
    assert req.data == "body"


def test_request_headers_without_a_space(httpbin):
    r = call("get", httpbin.url + "/headers", headers_list=["X-A:1", "X-B:  2"])
    assert r.request.headers == {"X-A": "1", "X-B": "2"}
    assert r.json()["headers"]["X-A"] == "1"


@pytest.mark.parametrize("headers", [None, {}])
@pytest.mark.parametrize("headers_list", [None, [], ()])
def test_empty_headers(httpbin, headers, headers_list):
    r = call("get", httpbin.url + "/headers", headers=headers, headers_list=headers_list)
    assert r.status_code == 200
    assert r.request.headers == {}


@pytest.mark.parametrize("cookies", [None, {}])
@pytest.mark.parametrize("cookie_list", [None, [], ()])
def test_empty_cookies(httpbin, cookies, cookie_list):
    r = call("get", httpbin.url + "/cookies", cookies=cookies, cookie_list=cookie_list)
    assert r.json()["cookies"] == {}
    assert r.request.cookies == {}


def test_empty_bodies(httpbin):
    for data in [None, "", b""]:
        r = call("post", httpbin.url + "/post", data=data)
        assert r.json()["data"] == ""


def test_headers_and_headers_list(httpbin):
    r = call("get", httpbin.url + "/headers", headers={"X-A": "1"}, headers_list=["X-B: 2"])
    sent = r.json()["headers"]
    assert (sent["X-A"], sent["X-B"]) == ("1", "2")


def test_header_tuples_are_accepted(httpbin):
    r = call("get", httpbin.url + "/headers", headers_list=("X-B: 2",))
    assert r.json()["headers"]["X-B"] == "2"


def test_arguments_are_not_modified(httpbin):
    headers_list = ["X-B: 2"]
    cookie_list = [httpperf.session_cookie_for_url(httpbin.url, "a", "1")]
    headers = {"X-A": "1"}
    cookies = {"b": "2"}
    for _ in range(2):
        r = call(
            "post",
            httpbin.url + "/anything",
            headers=headers,
            headers_list=headers_list,
            cookies=cookies,
            cookie_list=cookie_list,
            json={},
        )
        assert r.json()["headers"]["X-A"] == "1"
    assert headers_list == ["X-B: 2"]
    assert len(cookie_list) == 1
    assert headers == {"X-A": "1"} and cookies == {"b": "2"}


def test_cookie_list_kwarg(httpbin):
    cookie = httpperf.session_cookie_for_url(httpbin.url, "a", "1")
    r = call("get", httpbin.url + "/cookies", cookie_list=[cookie], cookies={"b": "2"})
    assert r.json()["cookies"] == {"a": "1", "b": "2"}


@pytest.mark.parametrize(
    "url, params, query",
    [
        ("/get", {"a": "1"}, {"a": "1"}),
        ("/get", {"a": ["1", "2"]}, {"a": ["1", "2"]}),
        ("/get", [("a", "1"), ("b", "2")], {"a": "1", "b": "2"}),
        ("/get", "a=1", {"a": "1"}),
        ("/get?x=0", {"a": "1"}, {"x": "0", "a": "1"}),
        ("/get#frag", {"a": "1"}, {"a": "1"}),
        ("/get", {}, {}),
    ],
)
def test_params(httpbin, url, params, query):
    r = call("get", httpbin.url + url, params=params)
    assert r.json()["args"] == query


@pytest.mark.parametrize(
    "data, form, raw",
    [
        ({"a": "1"}, {"a": "1"}, ""),
        ([("a", "1"), ("a", "2")], {"a": ["1", "2"]}, ""),
        ("text", {}, "text"),
        (b"\x00\xff", {}, "data:application/octet-stream;base64,AP8="),
    ],
)
def test_data(httpbin, data, form, raw):
    headers = None if isinstance(data, (dict, list)) else {"Content-Type": "application/octet-stream"}
    body = call("post", httpbin.url + "/post", data=data, headers=headers).json()
    assert body["form"] == form
    assert body["data"] == raw


def test_json_kwarg(httpbin):
    body = call("post", httpbin.url + "/post", json={"a": [1, None]}).json()
    assert body["json"] == {"a": [1, None]}
    assert body["headers"]["Content-Type"] == "application/json"


def test_json_kwarg_falsy_values(httpbin):
    for value in [{}, [], 0, False, ""]:
        assert call("post", httpbin.url + "/post", json=value).json()["json"] == value


def test_explicit_content_type_wins(httpbin):
    body = call("post", httpbin.url + "/post", json={"a": 1}, headers={"content-type": "application/vnd+json"}).json()
    assert body["headers"]["Content-Type"] == "application/vnd+json"


def test_json_with_data_raises(httpbin):
    with pytest.raises(ValueError):
        call("post", httpbin.url + "/post", json={}, data="x")


def test_files_without_data(httpbin):
    body = call("post", httpbin.url + "/post", files={"f": ("name.txt", b"abc", "text/plain")}).json()
    assert body["files"] == {"f": "abc"}


def test_auth(httpbin):
    r = call("get", httpbin.url + "/basic-auth/user/pass", auth=("user", "pass"))
    assert r.json() == {"authenticated": True, "user": "user"}


def test_allow_redirects(httpbin):
    r = call("get", httpbin.url + "/redirect/1", allow_redirects=False)
    assert r.status_code == 302
    assert r.redirect_url == httpbin.url + "/get"
    assert r.history == []


def test_no_redirect_url(httpbin):
    assert call("get", httpbin.url + "/get").redirect_url is None


def test_max_redirects(httpbin):
    with pytest.raises(httpperf.RequestError, match="Max Redirects"):
        call("get", httpbin.url + "/redirect/3", max_redirects=2)
    assert call("get", httpbin.url + "/redirect/2", max_redirects=2).status_code == 200


@pytest.mark.parametrize("status, method", [(301, "GET"), (302, "GET"), (303, "GET"), (307, "POST"), (308, "POST")])
def test_redirect_method(httpbin, status, method):
    r = call(
        "post",
        httpbin.url + "/redirect-to",
        params={"url": "/anything", "status_code": status},
        data="x",
        headers={"Content-Type": "text/plain"},
    )
    assert r.json()["method"] == method
    assert r.json()["data"] == ("x" if method == "POST" else "")
    assert [h.status_code for h in r.history] == [status]


def test_redirects_keep_headers(httpbin):
    r = call("get", httpbin.url + "/redirect/1", headers={"X-A": "1"})
    assert r.json()["headers"]["X-A"] == "1"


def test_request_timeout(httpbin):
    with pytest.raises(httpperf.Timeout) as e:
        call("get", httpbin.url + "/delay/2", timeout=0.2)
    assert e.value.code == httpperf.CURLE_OPERATION_TIMEDOUT
    assert isinstance(e.value, httpperf.RequestError)


def test_session_timeout(httpbin):
    async def main():
        async with httpperf.Session(timeout=0.2) as s:
            with pytest.raises(httpperf.Timeout):
                await s.get(httpbin.url + "/delay/2")
            r = await s.get(httpbin.url + "/delay/1", timeout=5)
            return r.status_code

    assert asyncio.run(main()) == 200


def test_connection_refused():
    with pytest.raises(httpperf.RequestError) as e:
        call("get", "http://127.0.0.1:9/")
    assert e.value.code == 7
    assert not isinstance(e.value, httpperf.Timeout)


def test_response_properties(httpbin):
    r = call("get", httpbin.url + "/response-headers", params={"X-Test": "a"})
    assert r.status_code == r.response_code == 200
    assert r.ok and r.reason == "OK"
    assert isinstance(r.elapsed, datetime.timedelta)
    assert r.url.startswith(httpbin.url + "/response-headers")
    assert r.start_time <= r.end_time
    assert r.queue_time >= 0
    assert isinstance(r.client_lag, float)
    times = [
        r.namelookup_time,
        r.connect_time,
        r.pretransfer_time,
        r.starttransfer_time,
        r.total_time,
    ]
    assert times == sorted(times)
    assert r.appconnect_time == 0
    assert r.posttransfer_time >= 0
    assert r.upload_size == 0
    assert r.download_size == len(r.body)
    assert r.primary_ip == "127.0.0.1"
    assert r.http_version == "HTTP/1.1"
    assert r.encoding == "utf-8"
    assert r.text == r.body.decode()
    assert r.json() is r.json()
    assert r.headers["x-test"] == "a"
    assert ("X-Test", "a") in r.headers_tuple
    assert r.header.startswith("HTTP/1.")
    assert r.cookielist == [] and r.cookies == {}
    assert r.history == []


def test_empty_response(httpbin):
    r = call("get", httpbin.url + "/status/204")
    assert (r.status_code, r.body, r.text, r.download_size) == (204, b"", "", 0)
    assert r.reason == "NO CONTENT"


def test_upload_size(httpbin):
    assert call("post", httpbin.url + "/post", data=b"x" * 100).upload_size == 100


def test_response_cookies(httpbin):
    async def main(s):
        r = await s.get(httpbin.url + "/cookies/set", params={"a": "1"}, allow_redirects=False)
        return r.status_code, r.cookies, [c.name for c in r.cookielist]

    assert with_session(main) == (302, {"a": "1"}, ["a"])


def test_response_callback(httpbin):
    seen = []

    async def main(s):
        s.set_response_callback(seen.append)
        return await s.get(httpbin.url + "/redirect/2")

    r = with_session(main)
    assert seen == r.history + [r]


def test_session_cookie_methods(httpbin):
    async def main(s):
        assert s.cookies() == {} and s.get_cookie_list() == []
        await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
        assert s.cookies() == {"a": "1"}
        s.add_cookie_list([httpperf.session_cookie_for_url(httpbin.url, "b", "2")])
        assert sorted(c.name for c in s.get_cookie_list()) == ["a", "b"]
        sent = (await s.get(httpbin.url + "/cookies")).json()["cookies"]
        s.erase_session_cookies()
        assert s.cookies() == {}
        s.add_cookie_list([httpperf.session_cookie_for_url(httpbin.url, "c", "3")])
        s.erase_all_cookies()
        return sent, s.cookies()

    assert with_session(main) == ({"a": "1", "b": "2"}, {})


def test_session_close_twice():
    s = httpperf.Session()
    s.close()
    s.close()


def test_session_sync_context_manager():
    with httpperf.Session() as s:
        assert isinstance(s, httpperf.Session)


def test_session_with_engine(httpbin):
    async def main():
        async with httpperf.Engine() as engine:
            async with httpperf.Session(engine=engine) as s:
                return await s.get(httpbin.url + "/get")

    assert asyncio.run(main()).status_code == 200


def test_engine_json_functions_reach_sessions(httpbin):
    async def main():
        with httpperf.Engine(json_dumps=lambda o: '{"x":1}', json_loads=lambda b: "loaded") as engine:
            async with httpperf.Session(engine=engine) as s:
                r = await s.post(httpbin.url + "/post", json={})
                return r.json(), json.loads(r.body)["json"]

    assert asyncio.run(main()) == ("loaded", {"x": 1})


def test_session_keyword_only():
    with pytest.raises(TypeError):
        httpperf.Session(None)


def test_cookie():
    c = httpperf.Cookie(True, ".example.com", True, "/p", True, 5, "n", "v")
    assert (c.http_only, c.domain, c.include_subdomains, c.path, c.is_secure, c.expiration, c.name, c.value) == (
        True,
        ".example.com",
        True,
        "/p",
        True,
        5,
        "n",
        "v",
    )
    assert c.format() == "#HttpOnly_.example.com\tTRUE\t/p\tTRUE\t5\tn\tv"
    assert str(c) == repr(c)
    assert "name=n" in repr(c)
    assert c.has_expired


def test_parse_cookie_string():
    c = httpperf.parse_cookie_string("example.com\tFALSE\t/\tFALSE\t0\tn\tv\n")
    assert (c.http_only, c.domain, c.include_subdomains, c.is_secure, c.expiration, c.name, c.value) == (
        False,
        "example.com",
        False,
        False,
        0,
        "n",
        "v",
    )
    assert httpperf.parse_cookie_string(c.format()).format() == c.format()
    assert httpperf.parse_cookie_string("example.com\tFALSE\t/\tFALSE\t0\tn").value == ""


def test_parse_cookie_list_string():
    cookies = httpperf.parse_cookie_list_string("a.com\tFALSE\t/\tFALSE\t0\ta\t1\n\nb.com\tFALSE\t/\tFALSE\t0\tb\t2\n")
    assert [(c.domain, c.name) for c in cookies] == [("a.com", "a"), ("b.com", "b")]
    assert httpperf.parse_cookie_list_string("") == []


def test_session_cookie_for_url():
    c = httpperf.session_cookie_for_url("https://example.com/a/b?q", "n", "v")
    assert (c.domain, c.path, c.include_subdomains, c.is_secure, c.http_only, c.expiration) == (
        ".example.com",
        "/",
        True,
        False,
        False,
        0,
    )
    c = httpperf.session_cookie_for_url(
        "https://example.com/a/b",
        "n",
        "v",
        http_only=True,
        include_subdomains=False,
        is_secure=True,
        include_url_path=True,
    )
    assert (c.path, c.include_subdomains, c.is_secure, c.http_only) == ("/a/b", False, True, True)


def test_headers_mapping():
    h = httpperf.Headers([("Content-Type", "text/plain"), ("Set-Cookie", "a"), ("set-cookie", "b")])
    assert h["content-type"] == "text/plain"
    assert h["SET-COOKIE"] == "a, b"
    assert "Content-Type" in h and "missing" not in h and 1 not in h
    assert list(h) == ["Content-Type", "Set-Cookie"]
    assert len(h) == 2
    assert h.get("missing") is None
    assert dict(h) == {"Content-Type": "text/plain", "Set-Cookie": "a, b"}
    assert repr(h) == repr(dict(h))
    assert len(httpperf.Headers([])) == 0


def test_exceptions():
    assert issubclass(httpperf.Timeout, httpperf.RequestError)
    assert issubclass(httpperf.HTTPError, httpperf.RequestError)
    assert issubclass(httpperf.RequestError, Exception)
    assert httpperf.RequestError("m").code is None
    assert httpperf.RequestError("m", 7).code == 7
    assert httpperf.HTTPError("m", "resp").response == "resp"
