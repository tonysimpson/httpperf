import pytest

import acurl


class FakeResponse:
    def __init__(self, content_type, body):
        self._header = "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n%s\r\n\r\n" % (len(body), content_type)
        self._body = body

    def get_header(self):
        return [self._header.encode("ascii")]

    def get_body(self):
        return [self._body]


def response(content_type, body=b""):
    return acurl.Response(None, FakeResponse(content_type, body), 0, 0)


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
        return [self._header]


def test_headers():
    r = acurl.Response(
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
