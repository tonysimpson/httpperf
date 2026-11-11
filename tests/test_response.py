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
    return acurl.Response(None, FakeResponse(content_type, body), 0)


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
