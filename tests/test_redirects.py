from urllib.parse import quote

import pytest

import httpperf


def redirect(httpbin, to, status=302):
    return "%s/redirect-to?status_code=%d&url=%s" % (httpbin.url, status, quote(to, safe=""))


def other_host(httpbin):
    return httpbin.url.replace("127.0.0.1", "localhost")


async def test_authorization_is_kept_on_the_same_host(httpbin, s):
    r = await s.get(redirect(httpbin, httpbin.url + "/anything"), headers={"Authorization": "Bearer x"})
    assert r.json()["headers"].get("Authorization") == "Bearer x"


async def test_authorization_is_dropped_on_another_host(httpbin, s):
    r = await s.get(redirect(httpbin, other_host(httpbin) + "/anything"), headers={"Authorization": "Bearer x"})
    assert "Authorization" not in r.json()["headers"]


async def test_auth_is_dropped_on_another_host(httpbin, s):
    r = await s.get(redirect(httpbin, other_host(httpbin) + "/anything"), auth=("user", "pw"))
    assert "Authorization" not in r.json()["headers"]


async def test_other_headers_are_kept_on_another_host(httpbin, s):
    r = await s.get(redirect(httpbin, other_host(httpbin) + "/anything"), headers={"X-A": "1"})
    assert r.json()["headers"]["X-A"] == "1"


async def test_a_redirect_to_another_host_goes_there(httpbin, s):
    r = await s.get(redirect(httpbin, other_host(httpbin) + "/anything"))
    assert r.json()["headers"]["Host"] == other_host(httpbin).split("//")[1]


@pytest.mark.parametrize("status", [301, 302, 303])
async def test_a_post_turned_into_a_get_drops_the_body_headers(httpbin, s, status):
    r = await s.post(redirect(httpbin, "/anything", status), json={"a": 1})
    assert "Content-Type" not in r.json()["headers"]


@pytest.mark.parametrize("status", [301, 302])
async def test_a_put_is_kept_on_301_and_302(httpbin, s, status):
    r = await s.put(redirect(httpbin, "/anything", status), data=b"x")
    assert (r.json()["method"], r.json()["data"]) == ("PUT", "x")


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_a_followed_head_stays_a_head(httpbin, s, status):
    r = await s.head(redirect(httpbin, "/get", status), allow_redirects=True)
    assert r.request.method == "HEAD"


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("status", [307, 308])
async def test_307_and_308_keep_the_method_and_body(httpbin, s, status, method):
    r = await s.request(method, redirect(httpbin, "/anything", status), data=b"x")
    assert (r.json()["method"], r.json()["data"]) == (method, "x")


async def test_files_are_sent_again_on_307(httpbin, s):
    r = await s.post(redirect(httpbin, "/anything", 307), files={"f": ("a.txt", b"hello")})
    assert r.json()["files"] == {"f": "hello"}


@pytest.mark.parametrize(
    "location, path",
    [("anything/x", "/anything/x"), ("/anything?a=1", "/anything?a=1"), ("?a=1", "/redirect-to?a=1")],
)
async def test_relative_locations(httpbin, s, location, path):
    r = await s.get(redirect(httpbin, location), allow_redirects=False)
    assert r.redirect_url == httpbin.url + path


async def test_a_protocol_relative_location(httpbin, s):
    r = await s.get(redirect(httpbin, "//" + other_host(httpbin).split("//")[1] + "/get"))
    assert r.url == other_host(httpbin) + "/get"


async def test_a_location_resolves_against_a_url_with_brackets(httpbin, s):
    r = await s.get(httpbin.url + "/redirect-to?url=%2Fget&x=[1]")
    assert r.url == httpbin.url + "/get"


async def test_a_location_with_another_scheme_is_an_error(httpbin, s):
    with pytest.raises(httpperf.RequestError):
        await s.get(redirect(httpbin, "market://details?id=x"))


async def test_cookies_set_by_a_redirect_are_sent_on_the_next_hop(httpbin, s):
    r = await s.get(httpbin.url + "/cookies/set?a=1")
    assert r.json()["cookies"] == {"a": "1"}


@pytest.mark.parametrize(
    "a, b, same",
    [
        ("http://h/", "http://h:80/x", True),
        ("https://h/", "https://H:443/", True),
        ("http://h/", "https://h/", False),
        ("http://h/", "http://h:8080/", False),
    ],
)
def test_origins_compare_with_default_ports(a, b, same):
    assert (httpperf._origin(a) == httpperf._origin(b)) == same
