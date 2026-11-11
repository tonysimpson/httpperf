import asyncio
import http.server
import threading
import time

import pytest

import acurl


class CountingHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    open_connections = 0

    def setup(self):
        super().setup()
        CountingHandler.open_connections += 1

    def finish(self):
        super().finish()
        CountingHandler.open_connections -= 1

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.send_header("Set-Cookie", "name=value")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture
def url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CountingHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield "http://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


def wait_for_connections(n):
    deadline = time.monotonic() + 2
    while CountingHandler.open_connections != n and time.monotonic() < deadline:
        time.sleep(0.01)
    return CountingHandler.open_connections


def test_close_closes_connections(url):
    loop = asyncio.new_event_loop()
    el = acurl.EventLoop(loop=loop)
    s = el.session(connections="session")
    r = loop.run_until_complete(s.get(url))
    assert r.cookies == {"name": "value"}
    assert wait_for_connections(1) == 1
    s.close()
    assert wait_for_connections(0) == 0
    assert r.status_code == 200
    with pytest.raises(RuntimeError):
        loop.run_until_complete(s.get(url))
    el.stop()
    loop.close()
