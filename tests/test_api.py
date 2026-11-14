import copy
import datetime
import json
import time

import pytest

import httpperf
from httpperf import version


def fields(obj, *names):
    return {name: getattr(obj, name) for name in names}


def test_version():
    assert httpperf.__version__ == version.__version__


@pytest.mark.parametrize("method", ["get", "put", "patch", "post", "delete"])
async def test_the_server_receives_the_method(httpbin, s, method):
    assert (await getattr(s, method)(httpbin.url + "/anything")).json()["method"] == method.upper()


@pytest.mark.parametrize("method", ["get", "put", "patch", "post", "delete", "head", "options"])
async def test_the_request_records_the_method(httpbin, s, method):
    assert (await getattr(s, method)(httpbin.url + "/anything")).request.method == method.upper()


async def test_options_returns_the_allowed_methods(httpbin, s):
    assert "OPTIONS" in (await s.options(httpbin.url + "/anything")).headers["Allow"]


async def test_a_custom_method_reaches_the_server(httpbin, s):
    assert (await s.request("PROPFIND", httpbin.url + "/anything")).status_code == 405


@pytest.mark.parametrize(
    "attribute, expected",
    [
        ("method", "GET"),
        ("header_list", None),
        ("headers", {}),
        ("cookie_list", None),
        ("cookies", {}),
        ("auth", None),
        ("data", None),
    ],
)
async def test_a_request_without_options(httpbin, s, attribute, expected):
    assert getattr((await s.get(httpbin.url + "/get")).request, attribute) == expected


async def test_the_request_records_its_url(httpbin, s):
    assert (await s.get(httpbin.url + "/get")).request.url == httpbin.url + "/get"


@pytest.mark.parametrize(
    "attribute, expected",
    [
        ("header_list", ("X-A: 1",)),
        ("headers", {"X-A": "1"}),
        ("cookies", {"c": "v"}),
        ("auth", ("user", "pass")),
        ("data", "body"),
    ],
)
async def test_a_request_with_options(httpbin, s, attribute, expected):
    r = await s.post(
        httpbin.url + "/post", headers={"X-A": "1"}, cookies={"c": "v"}, auth=("user", "pass"), data="body"
    )
    assert getattr(r.request, attribute) == expected


async def test_the_request_records_its_cookie_list(httpbin, s):
    r = await s.get(httpbin.url + "/get", cookies={"c": "v"})
    assert [c.name for c in r.request.cookie_list] == ["c"]


async def test_headers_without_a_space_are_recorded(httpbin, s):
    r = await s.get(httpbin.url + "/headers", headers_list=["X-A:1", "X-B:  2"])
    assert r.request.headers == {"X-A": "1", "X-B": "2"}


async def test_headers_without_a_space_are_sent(httpbin, s):
    r = await s.get(httpbin.url + "/headers", headers_list=["X-A:1"])
    assert r.json()["headers"]["X-A"] == "1"


@pytest.mark.parametrize("headers", [None, {}])
@pytest.mark.parametrize("headers_list", [None, [], ()])
async def test_empty_headers(httpbin, s, headers, headers_list):
    r = await s.get(httpbin.url + "/headers", headers=headers, headers_list=headers_list)
    assert r.request.headers == {}


@pytest.mark.parametrize("cookies", [None, {}])
@pytest.mark.parametrize("cookie_list", [None, [], ()])
async def test_empty_cookies(httpbin, s, cookies, cookie_list):
    r = await s.get(httpbin.url + "/cookies", cookies=cookies, cookie_list=cookie_list)
    assert r.json()["cookies"] == {}


@pytest.mark.parametrize("data", [None, "", b""])
async def test_empty_bodies(httpbin, s, data):
    assert (await s.post(httpbin.url + "/post", data=data)).json()["data"] == ""


async def test_headers_and_headers_list_are_both_sent(httpbin, s):
    r = await s.get(httpbin.url + "/headers", headers={"X-A": "1"}, headers_list=["X-B: 2"])
    sent = r.json()["headers"]
    assert {"X-A": sent["X-A"], "X-B": sent["X-B"]} == {"X-A": "1", "X-B": "2"}


async def test_a_headers_list_can_be_a_tuple(httpbin, s):
    r = await s.get(httpbin.url + "/headers", headers_list=("X-B: 2",))
    assert r.json()["headers"]["X-B"] == "2"


async def test_arguments_are_not_modified(httpbin, s):
    arguments = {
        "headers": {"X-A": "1"},
        "headers_list": ["X-B: 2"],
        "cookies": {"b": "2"},
        "cookie_list": [httpperf.session_cookie_for_url(httpbin.url, "a", "1")],
    }
    original = {name: copy.copy(value) for name, value in arguments.items()}
    for _ in range(2):
        await s.post(httpbin.url + "/anything", json={}, **arguments)
    assert arguments == original


async def test_cookie_list_and_cookies_are_both_sent(httpbin, s):
    cookie = httpperf.session_cookie_for_url(httpbin.url, "a", "1")
    r = await s.get(httpbin.url + "/cookies", cookie_list=[cookie], cookies={"b": "2"})
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
async def test_params(httpbin, s, url, params, query):
    assert (await s.get(httpbin.url + url, params=params)).json()["args"] == query


@pytest.mark.parametrize(
    "data, form, raw",
    [
        ({"a": "1"}, {"a": "1"}, ""),
        ([("a", "1"), ("a", "2")], {"a": ["1", "2"]}, ""),
        ("text", {}, "text"),
        (b"\x00\xff", {}, "data:application/octet-stream;base64,AP8="),
    ],
)
async def test_data(httpbin, s, data, form, raw):
    headers = None if isinstance(data, (dict, list)) else {"Content-Type": "application/octet-stream"}
    body = (await s.post(httpbin.url + "/post", data=data, headers=headers)).json()
    assert {"form": body["form"], "data": body["data"]} == {"form": form, "data": raw}


@pytest.mark.parametrize("data", ["text", b"bytes", ""])
async def test_raw_data_has_no_content_type(httpbin, s, data):
    assert "Content-Type" not in (await s.post(httpbin.url + "/anything", data=data)).json()["headers"]


@pytest.mark.parametrize("data, received", [("text", "text"), (b"bytes", "bytes"), ("", "")])
async def test_raw_data_is_sent_as_is(httpbin, s, data, received):
    assert (await s.post(httpbin.url + "/anything", data=data)).json()["data"] == received


@pytest.mark.parametrize("header", ["content-type: text/plain", "Content-Type:text/plain", "CONTENT-TYPE:  text/plain"])
async def test_raw_data_keeps_a_set_content_type(httpbin, s, header):
    body = (await s.post(httpbin.url + "/anything", data="x", headers_list=[header])).json()
    assert body["headers"]["Content-Type"] == "text/plain"


@pytest.mark.parametrize("value", [{"a": [1, None]}, {}, [], 0, False, ""])
async def test_json_is_sent(httpbin, s, value):
    assert (await s.post(httpbin.url + "/post", json=value)).json()["json"] == value


async def test_json_sets_the_content_type(httpbin, s):
    body = (await s.post(httpbin.url + "/post", json={"a": 1})).json()
    assert body["headers"]["Content-Type"] == "application/json"


async def test_an_explicit_content_type_beats_json(httpbin, s):
    r = await s.post(httpbin.url + "/post", json={"a": 1}, headers={"content-type": "application/vnd+json"})
    assert r.json()["headers"]["Content-Type"] == "application/vnd+json"


async def test_json_with_data_raises(httpbin, s):
    with pytest.raises(ValueError):
        await s.post(httpbin.url + "/post", json={}, data="x")


async def test_files_without_data(httpbin, s):
    r = await s.post(httpbin.url + "/post", files={"f": ("name.txt", b"abc", "text/plain")})
    assert r.json()["files"] == {"f": "abc"}


async def test_auth(httpbin, s):
    r = await s.get(httpbin.url + "/basic-auth/user/pass", auth=("user", "pass"))
    assert r.json() == {"authenticated": True, "user": "user"}


async def unfollowed_redirect(s, url):
    return await s.get(url + "/redirect/1", allow_redirects=False)


async def test_an_unfollowed_redirect_returns_its_status(httpbin, s):
    assert (await unfollowed_redirect(s, httpbin.url)).status_code == 302


async def test_an_unfollowed_redirect_gives_its_url(httpbin, s):
    assert (await unfollowed_redirect(s, httpbin.url)).redirect_url == httpbin.url + "/get"


async def test_an_unfollowed_redirect_has_no_history(httpbin, s):
    assert (await unfollowed_redirect(s, httpbin.url)).history == []


async def test_no_redirect_url_without_a_redirect(httpbin, s):
    assert (await s.get(httpbin.url + "/get")).redirect_url is None


async def test_too_many_redirects_raise(httpbin, s):
    with pytest.raises(httpperf.RequestError, match="Max Redirects"):
        await s.get(httpbin.url + "/redirect/3", max_redirects=2)


async def test_redirects_up_to_the_limit_are_followed(httpbin, s):
    assert (await s.get(httpbin.url + "/redirect/2", max_redirects=2)).status_code == 200


REDIRECT_METHODS = [(301, "GET"), (302, "GET"), (303, "GET"), (307, "POST"), (308, "POST")]


async def post_and_redirect(s, url, status):
    params = {"url": "/anything", "status_code": status}
    return await s.post(url + "/redirect-to", params=params, data="x", headers={"Content-Type": "text/plain"})


@pytest.mark.parametrize("status, method", REDIRECT_METHODS)
async def test_redirect_method(httpbin, s, status, method):
    assert (await post_and_redirect(s, httpbin.url, status)).json()["method"] == method


@pytest.mark.parametrize("status, method", REDIRECT_METHODS)
async def test_a_redirect_keeps_the_body_only_with_post(httpbin, s, status, method):
    body = (await post_and_redirect(s, httpbin.url, status)).json()["data"]
    assert body == ("x" if method == "POST" else "")


@pytest.mark.parametrize("status", [status for status, _ in REDIRECT_METHODS])
async def test_the_history_records_the_redirect(httpbin, s, status):
    assert [h.status_code for h in (await post_and_redirect(s, httpbin.url, status)).history] == [status]


async def test_redirects_keep_headers(httpbin, s):
    r = await s.get(httpbin.url + "/redirect/1", headers={"X-A": "1"})
    assert r.json()["headers"]["X-A"] == "1"


async def test_a_timeout_has_curls_timeout_code(httpbin, s):
    with pytest.raises(httpperf.Timeout) as e:
        await s.get(httpbin.url + "/delay/2", timeout=0.2)
    assert e.value.code == httpperf.CURLE_OPERATION_TIMEDOUT


async def test_a_refused_connection_is_not_a_timeout(s):
    with pytest.raises(httpperf.RequestError) as e:
        await s.get("http://127.0.0.1:9/")
    assert not isinstance(e.value, httpperf.Timeout)


async def echo_header(s, url):
    return await s.get(url + "/response-headers", params={"X-Test": "a"})


@pytest.mark.parametrize(
    "attribute, expected",
    [
        ("status_code", 200),
        ("response_code", 200),
        ("ok", True),
        ("reason", "OK"),
        ("appconnect_time", 0),
        ("upload_size", 0),
        ("primary_ip", "127.0.0.1"),
        ("http_version", "HTTP/1.1"),
        ("encoding", "utf-8"),
        ("cookielist", []),
        ("cookies", {}),
        ("history", []),
    ],
)
async def test_response_attributes(httpbin, s, attribute, expected):
    assert getattr(await echo_header(s, httpbin.url), attribute) == expected


@pytest.mark.parametrize("attribute, kind", [("elapsed", datetime.timedelta), ("client_lag", float)])
async def test_response_attribute_types(httpbin, s, attribute, kind):
    assert isinstance(getattr(await echo_header(s, httpbin.url), attribute), kind)


async def test_the_response_url_is_the_requested_one(httpbin, s):
    assert (await echo_header(s, httpbin.url)).url.startswith(httpbin.url + "/response-headers")


async def test_a_response_starts_before_it_ends(httpbin, s):
    r = await echo_header(s, httpbin.url)
    assert r.start_time <= r.end_time


async def test_queue_time_is_never_negative(httpbin, s):
    assert (await echo_header(s, httpbin.url)).queue_time >= 0


async def test_curls_timings_are_in_order(httpbin, s):
    r = await echo_header(s, httpbin.url)
    times = [r.namelookup_time, r.connect_time, r.pretransfer_time, r.starttransfer_time, r.total_time]
    assert times == sorted(times)


async def test_posttransfer_time_is_none_or_not_negative(httpbin, s):
    posttransfer_time = (await echo_header(s, httpbin.url)).posttransfer_time
    assert posttransfer_time is None or posttransfer_time >= 0


async def test_download_size_is_the_body_size(httpbin, s):
    r = await echo_header(s, httpbin.url)
    assert r.download_size == len(r.body)


async def test_text_is_the_decoded_body(httpbin, s):
    r = await echo_header(s, httpbin.url)
    assert r.text == r.body.decode()


async def test_json_is_parsed_once(httpbin, s):
    r = await echo_header(s, httpbin.url)
    assert r.json() is r.json()


async def test_headers(httpbin, s):
    assert (await echo_header(s, httpbin.url)).headers["x-test"] == "a"


async def test_headers_tuple(httpbin, s):
    assert ("X-Test", "a") in (await echo_header(s, httpbin.url)).headers_tuple


async def test_the_raw_header(httpbin, s):
    assert (await echo_header(s, httpbin.url)).header.startswith("HTTP/1.")


@pytest.mark.parametrize(
    "attribute, expected",
    [("status_code", 204), ("body", b""), ("text", ""), ("download_size", 0), ("reason", "NO CONTENT")],
)
async def test_an_empty_response(httpbin, s, attribute, expected):
    assert getattr(await s.get(httpbin.url + "/status/204"), attribute) == expected


async def test_upload_size(httpbin, s):
    assert (await s.post(httpbin.url + "/post", data=b"x" * 100)).upload_size == 100


async def test_a_response_cookie_in_netscape_format(httpbin, s):
    r = await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    assert r.history[0].cookielist[0].format() == "127.0.0.1\tFALSE\t/\tFALSE\t0\ta\t1"


async def test_response_cookies_are_only_that_responses(httpbin, s):
    await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    r = await s.get(httpbin.url + "/response-headers", params={"Set-Cookie": "b=2"})
    assert r.cookies == {"b": "2"}


async def test_the_session_has_every_responses_cookies(httpbin, s):
    await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    await s.get(httpbin.url + "/response-headers", params={"Set-Cookie": "b=2"})
    assert s.cookies() == {"a": "1", "b": "2"}


SET_COOKIES = [
    ("Set-Cookie", "plain=1"),
    ("Set-Cookie", "full=2; Domain=.example.com; Path=/p; Secure; HttpOnly; Max-Age=60"),
    ("Set-Cookie", "dated=3; Expires=Wed, 21 Oct 2037 07:28:00 GMT"),
    ("Set-Cookie", "gone=4; Max-Age=0"),
    ("Set-Cookie", "empty="),
    ("Set-Cookie", "invalid"),
]
SCOPE = ("domain", "include_subdomains", "path", "is_secure", "http_only")


async def set_cookies(s, url):
    r = await s.get(url + "/response-headers", params=SET_COOKIES)
    return r, {c.name: c for c in r.cookielist}


async def test_invalid_set_cookies_are_skipped(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert list(cookies) == ["plain", "full", "dated", "gone", "empty"]


async def test_a_plain_cookie_is_scoped_to_its_host(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert fields(cookies["plain"], *SCOPE) == {
        "domain": "127.0.0.1",
        "include_subdomains": False,
        "path": "/",
        "is_secure": False,
        "http_only": False,
    }


async def test_a_plain_cookie_lasts_for_the_session(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert cookies["plain"].expiration == 0


async def test_cookie_attributes_set_the_scope(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert fields(cookies["full"], *SCOPE) == {
        "domain": ".example.com",
        "include_subdomains": True,
        "path": "/p",
        "is_secure": True,
        "http_only": True,
    }


async def test_max_age_sets_the_expiration(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert cookies["full"].expiration == pytest.approx(time.time() + 60, abs=2)


async def test_expires_sets_the_expiration(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert cookies["dated"].expiration == 2139722880


async def test_a_max_age_of_zero_has_expired(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert cookies["gone"].has_expired


async def test_a_cookie_can_be_empty(httpbin, s):
    _, cookies = await set_cookies(s, httpbin.url)
    assert cookies["empty"].value == ""


async def test_response_cookies_as_a_dict(httpbin, s):
    r, _ = await set_cookies(s, httpbin.url)
    assert r.cookies == {"plain": "1", "full": "2", "dated": "3", "gone": "4", "empty": ""}


async def test_the_response_callback_sees_every_response(httpbin, s):
    seen = []
    s.set_response_callback(seen.append)
    r = await s.get(httpbin.url + "/redirect/2")
    assert seen == r.history + [r]


def test_a_new_session_has_no_cookies(s):
    assert s.cookies() == {}


def test_a_new_session_has_an_empty_cookie_list(s):
    assert s.get_cookie_list() == []


async def test_add_cookie_list(httpbin, s):
    await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    s.add_cookie_list([httpperf.session_cookie_for_url(httpbin.url, "b", "2")])
    assert sorted(c.name for c in s.get_cookie_list()) == ["a", "b"]


async def test_set_and_added_cookies_are_sent(httpbin, s):
    await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    s.add_cookie_list([httpperf.session_cookie_for_url(httpbin.url, "b", "2")])
    assert (await s.get(httpbin.url + "/cookies")).json()["cookies"] == {"a": "1", "b": "2"}


async def test_erase_session_cookies(httpbin, s):
    await s.get(httpbin.url + "/cookies/set", params={"a": "1"})
    s.erase_session_cookies()
    assert s.cookies() == {}


def test_a_session_can_be_closed_twice(s):
    s.close()
    s.close()


def test_with_gives_the_session(s):
    with s as entered:
        assert entered is s


async def test_a_session_with_its_own_engine(httpbin):
    async with httpperf.Engine() as engine:
        async with httpperf.Session(engine=engine) as s:
            assert (await s.get(httpbin.url + "/get")).status_code == 200


async def post_with_engine_json_functions(url):
    with httpperf.Engine(json_dumps=lambda o: '{"x":1}', json_loads=lambda b: "loaded") as engine:
        async with httpperf.Session(engine=engine) as s:
            return await s.post(url + "/post", json={})


async def test_the_engines_json_dumps_reaches_sessions(httpbin):
    assert json.loads((await post_with_engine_json_functions(httpbin.url)).body)["json"] == {"x": 1}


async def test_the_engines_json_loads_reaches_sessions(httpbin):
    assert (await post_with_engine_json_functions(httpbin.url)).json() == "loaded"


COOKIE = httpperf.Cookie(True, ".example.com", True, "/p", True, 5, "n", "v")


def test_cookie_fields():
    assert fields(COOKIE, *SCOPE, "expiration", "name", "value") == {
        "domain": ".example.com",
        "include_subdomains": True,
        "path": "/p",
        "is_secure": True,
        "http_only": True,
        "expiration": 5,
        "name": "n",
        "value": "v",
    }


def test_cookie_format():
    assert COOKIE.format() == "#HttpOnly_.example.com\tTRUE\t/p\tTRUE\t5\tn\tv"


def test_cookie_str_is_its_repr():
    assert str(COOKIE) == repr(COOKIE)


def test_cookie_repr_shows_its_name():
    assert "name=n" in repr(COOKIE)


def test_a_cookie_that_expired_in_1970_has_expired():
    assert COOKIE.has_expired


def test_parse_cookie_string():
    c = httpperf.parse_cookie_string("example.com\tFALSE\t/\tFALSE\t0\tn\tv\n")
    assert fields(c, *SCOPE, "expiration", "name", "value") == {
        "domain": "example.com",
        "include_subdomains": False,
        "path": "/",
        "is_secure": False,
        "http_only": False,
        "expiration": 0,
        "name": "n",
        "value": "v",
    }


def test_parse_cookie_string_reads_format():
    assert httpperf.parse_cookie_string(COOKIE.format()).format() == COOKIE.format()


def test_parse_cookie_string_without_a_value():
    assert httpperf.parse_cookie_string("example.com\tFALSE\t/\tFALSE\t0\tn").value == ""


def test_parse_cookie_list_string():
    cookies = httpperf.parse_cookie_list_string("a.com\tFALSE\t/\tFALSE\t0\ta\t1\n\nb.com\tFALSE\t/\tFALSE\t0\tb\t2\n")
    assert [(c.domain, c.name) for c in cookies] == [("a.com", "a"), ("b.com", "b")]


def test_parse_an_empty_cookie_list_string():
    assert httpperf.parse_cookie_list_string("") == []


def test_session_cookie_for_url_defaults():
    c = httpperf.session_cookie_for_url("https://example.com/a/b?q", "n", "v")
    assert fields(c, *SCOPE, "expiration") == {
        "domain": ".example.com",
        "include_subdomains": True,
        "path": "/",
        "is_secure": False,
        "http_only": False,
        "expiration": 0,
    }


def test_session_cookie_for_url_options():
    c = httpperf.session_cookie_for_url(
        "https://example.com/a/b",
        "n",
        "v",
        http_only=True,
        include_subdomains=False,
        is_secure=True,
        include_url_path=True,
    )
    assert fields(c, "path", "include_subdomains", "is_secure", "http_only") == {
        "path": "/a/b",
        "include_subdomains": False,
        "is_secure": True,
        "http_only": True,
    }


HEADERS = httpperf.Headers([("Content-Type", "text/plain"), ("Set-Cookie", "a"), ("set-cookie", "b")])


def test_header_names_ignore_case():
    assert HEADERS["content-type"] == "text/plain"


def test_repeated_headers_are_joined():
    assert HEADERS["SET-COOKIE"] == "a, b"


@pytest.mark.parametrize("name, present", [("Content-Type", True), ("missing", False), (1, False)])
def test_header_membership(name, present):
    assert (name in HEADERS) == present


def test_iterating_headers_gives_each_name_once():
    assert list(HEADERS) == ["Content-Type", "Set-Cookie"]


def test_headers_length_counts_names():
    assert len(HEADERS) == 2


def test_no_headers():
    assert len(httpperf.Headers([])) == 0


def test_header_get_without_a_default():
    assert HEADERS.get("missing") is None


def test_headers_as_a_dict():
    assert dict(HEADERS) == {"Content-Type": "text/plain", "Set-Cookie": "a, b"}


def test_headers_repr_is_a_dicts():
    assert repr(HEADERS) == repr(dict(HEADERS))


@pytest.mark.parametrize(
    "error, base",
    [
        (httpperf.Timeout, httpperf.RequestError),
        (httpperf.HTTPError, httpperf.RequestError),
        (httpperf.RequestError, Exception),
    ],
)
def test_exception_hierarchy(error, base):
    assert issubclass(error, base)


@pytest.mark.parametrize("arguments, code", [(("m",), None), (("m", 7), 7)])
def test_request_error_code(arguments, code):
    assert httpperf.RequestError(*arguments).code == code


def test_http_error_response():
    assert httpperf.HTTPError("m", "resp").response == "resp"


@pytest.mark.parametrize(
    "url, path",
    [("http://h/a/b/c", "/a/b"), ("http://h/a", "/"), ("http://h", "/"), ("http://h/a/", "/a")],
)
def test_response_cookie_default_path(url, path):
    assert httpperf._parse_set_cookie("n=v", url).path == path


def test_a_relative_cookie_path_is_ignored():
    assert httpperf._parse_set_cookie("n=v; Path=relative", "http://h/a/b/c").path == "/a/b"
