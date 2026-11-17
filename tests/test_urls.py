import pytest

import httpperf


@pytest.mark.parametrize("value", ["a%b", "x+y", "b c", "a&b=c", "http://x/y", "#frag", "é"])
async def test_a_param_value_round_trips(httpbin, s, value):
    assert (await s.get(httpbin.url + "/get", params={"v": value})).json()["args"] == {"v": value}


@pytest.mark.parametrize("path", ["/c/a%2Fb", "/x?q=%2B%3D%20", "/x?a=b%26c"])
async def test_a_urls_own_encoding_is_sent_unchanged(wire, s, path):
    await s.get(wire.url + path)
    assert wire.target() == path


async def test_params_keep_a_valueless_flag_in_the_url(wire, s):
    await s.get(wire.url + "/x?flag", params={"a": "1"})
    assert wire.target() == "/x?flag&a=1"


async def test_params_with_bytes(wire, s):
    await s.get(wire.url + "/x", params={b"k": b"v"})
    assert wire.target() == "/x?k=v"


async def test_a_param_of_none_is_left_out(wire, s):
    await s.get(wire.url + "/x", params={"a": None, "b": "1"})
    assert wire.target() == "/x?b=1"


async def test_an_ipv6_literal_url(s):
    import socket
    import threading

    server = socket.create_server(("::1", 0), family=socket.AF_INET6)

    def answer():
        conn, _ = server.accept()
        conn.recv(65536)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        conn.close()

    threading.Thread(target=answer, daemon=True).start()
    try:
        assert (await s.get("http://[::1]:%d/" % server.getsockname()[1])).body == b"ok"
    finally:
        server.close()


@pytest.mark.parametrize("user, password", [("user", "pw"), ("üser", "pässwörd")])
async def test_credentials_in_the_url_are_basic_auth(httpbin, s, user, password):
    from urllib.parse import quote

    user, password = quote(user), quote(password)
    url = httpbin.url.replace("//", "//" + user + ":" + password + "@") + "/basic-auth/" + user + "/" + password
    assert (await s.get(url)).status_code == 200


@pytest.mark.parametrize("stream", [False, True])
async def test_a_responses_url_leaves_out_the_credentials(httpbin, s, stream):
    r = await s.get(httpbin.url.replace("//", "//user:pw@") + "/get", stream=stream)
    assert r.url == httpbin.url + "/get"


async def test_cookies_are_sent_to_a_url_with_credentials(httpbin, s):
    await s.get(httpbin.url + "/cookies/set?a=1")
    url = httpbin.url.replace("//", "//user:pw@") + "/cookies"
    assert (await s.get(url)).json()["cookies"] == {"a": "1"}


@pytest.mark.parametrize("url", [" {}/get", "{}/get ", "\t{}/get\n"])
async def test_whitespace_around_a_url(httpbin, s, url):
    with pytest.raises((httpperf.RequestError, ValueError)):
        await s.get(url.format(httpbin.url))
