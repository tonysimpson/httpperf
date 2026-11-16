import gzip
import zlib

import pytest


def reply(wire, head, body=b""):
    wire.reply = head.replace(b"\n", b"\r\n") + b"\r\n" + body


@pytest.mark.parametrize(
    "content_type, encoding",
    [
        ('text/html; charset="utf-8"', "utf-8"),
        ("text/html; Charset=UTF-8", "UTF-8"),
        ("text/html; charset=", "latin1"),
        ("text/html; charset", "latin1"),
    ],
)
async def test_unusual_charsets(wire, s, content_type, encoding):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Type: %s\nContent-Length: 0\n" % content_type.encode())
    assert (await s.get(wire.url)).encoding == encoding


async def test_a_non_ascii_reason_phrase(wire, s):
    reply(wire, b"HTTP/1.1 200 \xc3\xa9t\xe9\nContent-Length: 0\n")
    r = await s.get(wire.url)
    assert (r.ok, isinstance(r.reason, str)) == (True, True)


async def test_json_is_parsed_whatever_the_content_type(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: 8\n", b'{"a": 1}')
    assert (await s.get(wire.url)).json() == {"a": 1}


async def test_text_of_a_binary_body_does_not_raise(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Type: application/pdf\nContent-Length: 4\n", b"\xff\xfe\x00\x81")
    assert len((await s.get(wire.url)).text) == 4


async def test_repeated_identical_headers(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nServer: a\nServer: a\nContent-Length: 0\n")
    assert (await s.get(wire.url)).headers["Server"] == "a, a"


@pytest.mark.parametrize("encoding", ["gzip", "br", "zstd"])
async def test_a_head_with_a_content_encoding_has_no_body(wire, s, encoding):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Encoding: %s\nContent-Length: 100\n" % encoding.encode())
    assert (await s.head(wire.url)).body == b""


@pytest.mark.parametrize("status", [204, 304])
async def test_an_empty_response_with_a_content_encoding(wire, s, status):
    reply(wire, b"HTTP/1.1 %d X\nContent-Encoding: gzip\n" % status)
    assert (await s.get(wire.url)).body == b""


async def test_an_empty_gzip_body(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Encoding: gzip\nContent-Length: 0\n")
    assert (await s.get(wire.url)).body == b""


async def test_a_body_gzipped_twice(wire, s):
    body = gzip.compress(gzip.compress(b"hello"))
    reply(wire, b"HTTP/1.1 200 OK\nContent-Encoding: gzip, gzip\nContent-Length: %d\n" % len(body), body)
    assert (await s.get(wire.url)).body == b"hello"


@pytest.mark.parametrize("wbits", [zlib.MAX_WBITS, -zlib.MAX_WBITS])
async def test_deflate_with_and_without_a_zlib_header(wire, s, wbits):
    c = zlib.compressobj(wbits=wbits)
    body = c.compress(b"hello") + c.flush()
    reply(wire, b"HTTP/1.1 200 OK\nContent-Encoding: deflate\nContent-Length: %d\n" % len(body), body)
    assert (await s.get(wire.url)).body == b"hello"


async def test_a_head_with_a_length_leaves_the_connection_usable(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Length: 5\n")
    await s.head(wire.url)
    reply(wire, b"HTTP/1.1 200 OK\nContent-Length: 2\n", b"ok")
    assert (await s.get(wire.url)).body == b"ok"


async def test_a_102_before_the_response(wire, s):
    reply(wire, b"HTTP/1.1 102 Processing\n\nHTTP/1.1 200 OK\nContent-Length: 2\n", b"ok")
    r = await s.get(wire.url)
    assert (r.status_code, r.body) == (200, b"ok")


async def test_a_204_with_a_content_length(wire, s):
    reply(wire, b"HTTP/1.1 204 No Content\nContent-Length: 5\n", b"stray")
    assert (await s.get(wire.url)).body == b""


async def test_a_connection_the_server_closed_while_idle_is_not_used(wire, s):
    reply(wire, b"HTTP/1.1 200 OK\nContent-Length: 2\nConnection: keep-alive\n", b"ok")
    await s.get(wire.url)
    wire.close_after_reply = True
    await s.get(wire.url)
    assert (await s.get(wire.url)).body == b"ok"
