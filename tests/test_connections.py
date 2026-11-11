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
        if self.path == "/slow":
            time.sleep(0.05)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.send_header("Set-Cookie", "name=value")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
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


@pytest.mark.parametrize("connections, new", [("session", [True, False]), ("none", [True, True])])
def test_connection_reuse(url, connections, new):
    loop = asyncio.new_event_loop()
    el = acurl.EventLoop(loop=loop)
    s = el.session(connections=connections)
    assert [loop.run_until_complete(s.get(url)).connect_time > 0 for _ in new] == new
    s.close()
    el.stop()
    loop.close()


@pytest.mark.parametrize("limits, connects", [({"max_host_connections": 2}, 2), ({"max_total_connections": 3}, 3)])
def test_event_loop_connection_limits(url, limits, connects):
    loop = asyncio.new_event_loop()
    el = acurl.EventLoop(loop=loop, **limits)
    s = el.session(http_version="HTTP/1.1")

    async def main():
        return await asyncio.gather(*(s.get(url + "slow") for _ in range(6)))

    responses = loop.run_until_complete(main())
    assert sum(r.connect_time > 0 for r in responses) == connects
    el.stop()
    loop.close()
