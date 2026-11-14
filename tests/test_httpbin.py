import asyncio
import base64
import gc
import json
import time
from urllib.parse import urlencode

import pytest

import httpperf


@pytest.fixture
def s():
    session = httpperf.Session()
    yield session
    session.close()


async def test_get(httpbin, s):
    assert (await s.get(httpbin.url + "/ip")).json()["origin"] == "127.0.0.1"


async def test_a_response_has_the_cookies_it_set(httpbin, s):
    r = await s.get(httpbin.url + "/cookies/set?name=value")
    assert r.history[0].cookies == {"name": "value"}


async def test_a_response_that_set_no_cookies_has_none(httpbin, s):
    r = await s.get(httpbin.url + "/cookies/set?name=value")
    assert r.cookies == {}


async def test_the_session_keeps_cookies(httpbin, s):
    await s.get(httpbin.url + "/cookies/set?name=value")
    assert s.cookies() == {"name": "value"}


async def test_the_session_lists_its_cookies(httpbin, s):
    await s.get(httpbin.url + "/cookies/set?name=value")
    assert [c.name for c in s.get_cookie_list()] == ["name"]


async def test_erase_all_cookies(httpbin, s):
    await s.get(httpbin.url + "/cookies/set?name=value")
    s.erase_all_cookies()
    assert s.cookies() == {}


async def test_request_cookies_join_the_sessions(httpbin, s):
    await s.get(httpbin.url + "/cookies/set?name=value")
    await s.get(httpbin.url + "/cookies/set?name2=value", cookies={"name3": "value"})
    assert s.cookies() == {"name": "value", "name2": "value", "name3": "value"}


async def test_request_cookies_are_sent(httpbin, s):
    r = await s.get(httpbin.url + "/cookies", cookies={"name": "value"})
    assert r.json()["cookies"] == {"name": "value"}


def brief_and_lasting_cookies(url):
    brief = httpperf.session_cookie_for_url(url, "brief", "1")
    lasting = httpperf.Cookie(False, brief.domain, True, "/", False, int(time.time()) + 3600, "lasting", "1")
    return [brief, lasting]


async def test_added_cookies_are_sent(httpbin, s):
    s.add_cookie_list(brief_and_lasting_cookies(httpbin.url))
    assert (await s.get(httpbin.url + "/cookies")).json()["cookies"] == {"brief": "1", "lasting": "1"}


def test_erase_session_cookies_keeps_lasting_ones(httpbin, s):
    s.add_cookie_list(brief_and_lasting_cookies(httpbin.url))
    s.erase_session_cookies()
    assert s.cookies() == {"lasting": "1"}


def test_a_closed_sessions_cookies_cannot_be_changed(s):
    s.close()
    with pytest.raises(RuntimeError):
        s.erase_all_cookies()


@pytest.mark.parametrize("auth, status", [(("user", "password"), 200), (("notuser", "notpassword"), 401)])
async def test_basic_auth(httpbin, s, auth, status):
    assert (await s.get(httpbin.url + "/basic-auth/user/password", auth=auth)).status_code == status


async def test_redirects_are_followed(httpbin, s):
    url = httpbin.url + "/ip"
    assert (await s.get(httpbin.url + "/redirect-to?" + urlencode({"url": url}))).url == url


async def test_redirect_history(httpbin, s):
    r = await s.get(httpbin.url + "/redirect/3")
    assert [h.url for h in r.history] == [
        httpbin.url + "/redirect/3",
        httpbin.url + "/relative-redirect/2",
        httpbin.url + "/relative-redirect/1",
    ]


async def test_text_uses_the_responses_encoding(httpbin, s):
    r = await s.get(httpbin.url + "/encoding/utf8")
    assert r.text == r.body.decode("utf-8")


async def test_setting_the_encoding_changes_the_text(httpbin, s):
    r = await s.get(httpbin.url + "/encoding/utf8")
    r.encoding = "latin1"
    assert r.text == r.body.decode("latin1")


async def test_head_has_no_body(httpbin, s):
    assert (await s.head(httpbin.url + "/get")).body == b""


async def test_head_keeps_the_content_length(httpbin, s):
    assert int((await s.head(httpbin.url + "/get")).headers["Content-Length"]) > 0


async def test_head_does_not_follow_redirects(httpbin, s):
    assert (await s.head(httpbin.url + "/redirect/1")).status_code == 302


@pytest.mark.parametrize("method", ["put", "post", "patch", "delete"])
async def test_methods_send_json(httpbin, s, method):
    r = await getattr(s, method)(httpbin.url + "/" + method, json={"a": 1})
    assert r.json()["json"] == {"a": 1}


async def test_custom_json_functions(httpbin):
    calls = []

    def dumps(obj):
        calls.append("engine dumps")
        return json.dumps(obj).encode()

    def loads(body):
        calls.append("session loads")
        return json.loads(body)

    async with httpperf.Engine(json_dumps=dumps, json_loads=lambda body: calls.append("engine loads")) as engine:
        r = await httpperf.Session(engine=engine, json_loads=loads).post(httpbin.url + "/post", json={"a": 1})
        r.json()
    assert calls == ["engine dumps", "session loads"]


BINARY = bytes(range(256))


async def post_binary(s, url):
    return await s.post(url + "/post", data=BINARY, headers={"Content-Type": "application/octet-stream"})


async def test_a_binary_body_is_sent_unchanged(httpbin, s):
    r = await post_binary(s, httpbin.url)
    assert r.json()["data"] == "data:application/octet-stream;base64," + base64.b64encode(BINARY).decode()


async def test_upload_size(httpbin, s):
    assert (await post_binary(s, httpbin.url)).upload_size == 256


async def test_download_size(httpbin, s):
    r = await post_binary(s, httpbin.url)
    assert r.download_size == len(r.body)


async def test_transfer_sizes_are_ints(httpbin, s):
    r = await post_binary(s, httpbin.url)
    assert [type(r.upload_size), type(r.download_size)] == [int, int]


@pytest.mark.parametrize("connections", ["shared", "session", "none"])
async def test_connections(httpbin, connections):
    async with httpperf.Session(connections=connections) as s:
        assert [(await s.get(httpbin.url + "/get")).status_code for _ in range(2)] == [200, 200]


@pytest.mark.parametrize(
    "options",
    [
        {"connections": "pooled"},
        {"tls": "pooled"},
        {"idle_timeout": -1},
        {"max_connection_age": -1},
        {"max_host_connections": 2},
        {"connections": "session", "max_connections": -1},
        {"http_version": "HTTP/0.9"},
    ],
)
def test_invalid_session_options(options):
    with pytest.raises(ValueError):
        httpperf.Session(**options)


@pytest.mark.parametrize("options", [{"max_host_connections": -1}, {"max_idle_connections": 0}])
def test_invalid_engine_options(options):
    with pytest.raises(ValueError):
        httpperf.Engine(**options)


async def test_client_lag_is_part_of_the_request_time(httpbin, s):
    r = await s.get(httpbin.url + "/delay/0")
    assert 0 <= r.client_lag < r.end_time - r.start_time


UNENCODABLE = [{"auth": ("\ud800", "password")}, {"headers": {"X-Test": "\ud800"}}, {"cookies": {"name": "\ud800"}}]


@pytest.mark.parametrize("options", UNENCODABLE)
async def test_unencodable_strings_raise(httpbin, s, options):
    with pytest.raises(UnicodeEncodeError):
        await s.get(httpbin.url + "/get", **options)


@pytest.mark.parametrize("options", UNENCODABLE)
async def test_the_session_works_after_an_unencodable_string(httpbin, s, options):
    with pytest.raises(UnicodeEncodeError):
        await s.get(httpbin.url + "/get", **options)
    assert (await s.get(httpbin.url + "/get")).status_code == 200


async def test_params_join_the_urls_query(httpbin, s):
    r = await s.get(httpbin.url + "/get?a=1#frag", params={"b": "x y", "c": ["1", "2"]})
    assert r.json()["args"] == {"a": "1", "b": "x y", "c": ["1", "2"]}


async def test_params_as_a_string(httpbin, s):
    assert (await s.get(httpbin.url + "/get", params="raw=1")).json()["args"] == {"raw": "1"}


async def test_form_data(httpbin, s):
    r = await s.post(httpbin.url + "/post", data={"a": "1", "b": ["2", "3"]})
    assert r.json()["form"] == {"a": "1", "b": ["2", "3"]}


async def test_form_data_sets_the_content_type(httpbin, s):
    r = await s.post(httpbin.url + "/post", data={"a": "1"})
    assert r.json()["headers"]["Content-Type"] == "application/x-www-form-urlencoded"


async def test_form_data_keeps_the_users_content_type(httpbin, s):
    r = await s.post(httpbin.url + "/post", data=[("a", "1")], headers={"content-type": "text/plain"})
    assert r.json()["headers"]["Content-Type"] == "text/plain"


@pytest.fixture
def files(tmp_path):
    path = tmp_path / "upload.txt"
    path.write_bytes(b"from disk")
    with open(path, "rb") as f:
        yield {"disk": f, "inline": ("a.csv", "x,y", "text/csv"), "raw": ("b.bin", b"\x00\x01")}


async def upload(s, url, files):
    return (await s.post(url + "/post", data={"field": "value"}, files=files)).json()


async def test_files_are_uploaded(httpbin, s, files):
    assert (await upload(s, httpbin.url, files))["files"] == {"disk": "from disk", "inline": "x,y", "raw": "\x00\x01"}


async def test_form_fields_are_sent_with_files(httpbin, s, files):
    assert (await upload(s, httpbin.url, files))["form"] == {"field": "value"}


async def test_files_are_sent_as_multipart(httpbin, s, files):
    content_type = (await upload(s, httpbin.url, files))["headers"]["Content-Type"]
    assert content_type.startswith("multipart/form-data; boundary=")


async def test_files_cannot_be_sent_with_json(httpbin, s, files):
    with pytest.raises(ValueError):
        await s.post(httpbin.url + "/post", json={}, files=files)


@pytest.mark.parametrize("path, ok, reason", [("/get", True, "OK"), ("/status/404", False, "NOT FOUND")])
async def test_ok_and_reason(httpbin, s, path, ok, reason):
    r = await s.get(httpbin.url + path)
    assert (r.ok, r.reason) == (ok, reason)


async def test_elapsed_is_the_total_time(httpbin, s):
    r = await s.get(httpbin.url + "/get")
    assert r.elapsed.total_seconds() == r.total_time


async def test_raise_for_status_passes_success(httpbin, s):
    (await s.get(httpbin.url + "/get")).raise_for_status()


async def test_raise_for_status_raises_for_errors(httpbin, s):
    r = await s.get(httpbin.url + "/status/404")
    with pytest.raises(httpperf.HTTPError) as e:
        r.raise_for_status()
    assert e.value.response is r


async def test_the_http_error_message(httpbin, s):
    r = await s.get(httpbin.url + "/status/404")
    with pytest.raises(httpperf.HTTPError, match="^404 Client Error: NOT FOUND for url: "):
        r.raise_for_status()


async def first_then_second_session(url):
    engine = httpperf.Engine()
    first = httpperf.Session(engine=engine)
    for _ in range(3):
        await first.get(url + "/cookies/set?secret=1")
        await first.get(url + "/headers", auth=("user", "pass"), headers={"X-Private": "1"})
    gc.collect()
    await asyncio.sleep(0.05)
    return first, httpperf.Session(engine=engine)


async def test_reused_handles_do_not_leak_cookies(httpbin):
    _, second = await first_then_second_session(httpbin.url)
    assert [(await second.get(httpbin.url + "/cookies")).json()["cookies"] for _ in range(3)] == [{}] * 3


async def test_reused_handles_do_not_leak_headers(httpbin):
    _, second = await first_then_second_session(httpbin.url)
    sent = [(await second.get(httpbin.url + "/headers")).json()["headers"] for _ in range(3)]
    assert [("Authorization" in h, "X-Private" in h) for h in sent] == [(False, False)] * 3


async def test_a_session_keeps_its_cookies_after_handles_are_reused(httpbin):
    first, second = await first_then_second_session(httpbin.url)
    await second.get(httpbin.url + "/cookies")
    assert (await first.get(httpbin.url + "/cookies")).json()["cookies"] == {"secret": "1"}
