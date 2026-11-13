import random

import pytest

import httpperf


class FakeResponse:
    def __init__(self, content_type, body):
        self._header = "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n%s\r\n\r\n" % (len(body), content_type)
        self._body = body

    def get_header(self):
        return self._header.encode("ascii")

    def get_body(self):
        return self._body


def response(content_type, body=b""):
    return httpperf.Response(None, FakeResponse(content_type, body), 0, 0)


@pytest.mark.parametrize(
    "content_type, encoding",
    [
        ("Content-Type: text/html; charset=utf-8", "utf-8"),
        ("content-type: text/html; charset=utf-8", "utf-8"),
        ('Content-Type: text/html; charset="UTF-8"; foo=bar', "UTF-8"),
        ("Content-Type: text/html", "latin1"),
        ("Content-Type: application/json", "utf-8"),
        ("Content-Type: application/problem+json", "utf-8"),
        ("Content-Type: application/octet-stream", "latin1"),
        ("X-Other: x", "latin1"),
    ],
)
def test_encoding_from_headers(content_type, encoding):
    assert response(content_type).encoding == encoding


def test_json_without_charset_is_utf8():
    r = response("Content-Type: application/json", '{"name": "café"}'.encode("utf-8"))
    assert r.json() == {"name": "café"}


class RawResponse:
    def __init__(self, header):
        self._header = header

    def get_header(self):
        return self._header


def test_headers():
    r = httpperf.Response(
        None,
        RawResponse(
            b"HTTP/1.1 100 Continue\r\n\r\n"
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Name: caf\xe9\r\n"
            b"X-Dup: a\r\nx-dup: b\r\nX-Tight:v\r\n\r\n"
        ),
        0,
        0,
    )
    assert r.headers_tuple == (
        ("Content-Type", "text/plain"),
        ("X-Name", "café"),
        ("X-Dup", "a"),
        ("x-dup", "b"),
        ("X-Tight", "v"),
    )
    assert r.headers["content-type"] == r.headers["CONTENT-TYPE"] == "text/plain"
    assert r.headers["x-dup"] == "a, b"
    assert "x-name" in r.headers and "missing" not in r.headers and 1 not in r.headers
    assert list(r.headers) == ["Content-Type", "X-Name", "X-Dup", "X-Tight"]
    assert r.headers.get("missing", "default") == "default"
    assert dict(r.headers) == {"Content-Type": "text/plain", "X-Name": "café", "X-Dup": "a, b", "X-Tight": "v"}


def python_parse_headers(header):
    blocks = [b for b in header.decode("latin-1").split("\r\n\r\n") if b]
    block = blocks[-1] if blocks else ""
    return tuple((name, value.strip()) for name, _, value in (line.partition(":") for line in block.split("\r\n")[1:]))


@pytest.mark.parametrize(
    "header",
    [
        b"",
        b"\r\n\r\n",
        b"HTTP/1.1 200 OK",
        b"HTTP/1.1 200 OK\r\n",
        b"HTTP/1.1 200 OK\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nA: b\r\n\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nA: b\r\n\r\n\r\n\r\nHTTP/1.1 204 No Content\r\nC:d\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nNo colon\r\n:\r\nA:\r\n x: folded\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nA: \t\x0b\x0c\x1c\x1f\x85\xa0 v \xa0\x85\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nA: b\rc\nd\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nA: b:c\r\n\r",
    ],
)
def test_parse_headers_matches_python(header):
    assert httpperf._httpperf.parse_headers(header) == python_parse_headers(header)


def test_parse_headers_matches_python_on_random_input():
    rng = random.Random(1)
    alphabet = b"\r\n\r\n:: \t\x0b\x1c\x85\xa0aZ"
    for _ in range(20000):
        header = bytes(rng.choice(alphabet) for _ in range(rng.randrange(40)))
        assert httpperf._httpperf.parse_headers(header) == python_parse_headers(header), header
