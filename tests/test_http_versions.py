import asyncio
import os
import socket
import sys
import threading

import pytest
import pytest_httpbin.certs
from hypercorn.asyncio import serve
from hypercorn.config import Config

import acurl


async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await asyncio.sleep(0.05)
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": scope["http_version"].encode()})


@pytest.fixture(scope="module")
def h2_url():
    certs = os.path.dirname(pytest_httpbin.certs.where())
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    config = Config()
    config.bind = ["127.0.0.1:%d" % port]
    if HTTP3:
        config.quic_bind = ["127.0.0.1:%d" % port]
    config.graceful_timeout = 0.1
    config.certfile = os.path.join(certs, "server.pem")
    config.keyfile = os.path.join(certs, "server.key")
    loop = asyncio.new_event_loop()
    started = threading.Event()
    stop = []

    async def run():
        stop.append(asyncio.Event())
        task = asyncio.ensure_future(serve(app, config, shutdown_trigger=stop[0].wait))
        while True:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                await asyncio.sleep(0.01)
        started.set()
        await task

    thread = threading.Thread(target=loop.run_until_complete, args=(run(),), daemon=True)
    thread.start()
    started.wait(5)
    yield "https://127.0.0.1:%d/" % port
    loop.call_soon_threadsafe(stop[0].set)
    thread.join(5)
    loop.close()


def libcurl_has_http3():
    loop = asyncio.new_event_loop()
    el = acurl.Engine()
    try:
        acurl.Session(engine=el, http_version="HTTP/3")
        return True
    except ValueError:
        return False
    finally:
        el.close()
        loop.close()


HTTP3 = sys.version_info >= (3, 9) and libcurl_has_http3()
needs_http3 = pytest.mark.skipif(
    not HTTP3, reason="needs libcurl with HTTP/3, and Python 3.9+ (hypercorn's HTTP/3 server hangs on 3.8)"
)


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro(acurl.Engine()))
    finally:
        loop.close()


@pytest.mark.parametrize(
    "version, body", [pytest.param("HTTP/3", "3", marks=needs_http3), ("HTTP/2", "2"), ("HTTP/1.1", "1.1")]
)
def test_http_version(h2_url, version, body):
    async def main(el):
        r = await acurl.Session(engine=el, http_version=version).get(h2_url)
        el.close()
        return r

    r = run(main)
    assert (r.http_version, r.text) == (version, body)
    assert r.headers["Content-Type"] == "text/plain"


@pytest.mark.parametrize(
    "version, connects", [pytest.param("HTTP/3", 1, marks=needs_http3), ("HTTP/2", 1), ("HTTP/1.1", 5)]
)
def test_concurrent_requests_share_a_connection(h2_url, version, connects):
    async def main(el):
        s = acurl.Session(engine=el, connections="session", http_version=version)
        responses = await asyncio.gather(*(s.get(h2_url) for _ in range(5)))
        el.close()
        return responses

    assert sum(r.connect_time > 0 for r in run(main)) == connects


def test_session_host_limit_with_multiplexing(h2_url):
    async def main(el):
        s = acurl.Session(engine=el, connections="session", max_host_connections=2)
        first = await asyncio.gather(*(s.get(h2_url) for _ in range(6)))
        second = await asyncio.gather(*(s.get(h2_url) for _ in range(6)))
        el.close()
        return first, second

    first, second = run(main)
    assert sum(r.connect_time > 0 for r in first + second) == 1
    assert sum(r.queue_time > 0 for r in first) == 4
    assert all(r.queue_time == 0 for r in second)
