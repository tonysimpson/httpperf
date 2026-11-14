import asyncio
import os
import socket
import sys
import threading

import pytest
import pytest_httpbin.certs
from helpers import until
from hypercorn.asyncio import serve
from hypercorn.config import Config

import httpperf


async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await asyncio.sleep(0.5 if scope["path"] == "/slow" else 0.05)
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
    try:
        httpperf.Session(http_version="HTTP/3")
    except ValueError:
        return False
    return True


HTTP3 = sys.version_info >= (3, 9) and libcurl_has_http3()
needs_http3 = pytest.mark.skipif(
    not HTTP3, reason="needs libcurl with HTTP/3, and Python 3.9+ (hypercorn's HTTP/3 server hangs on 3.8)"
)
ALL_VERSIONS = [pytest.param("HTTP/3", marks=needs_http3), "HTTP/2", "HTTP/1.1"]
MULTIPLEXED = [pytest.param("HTTP/3", marks=needs_http3), "HTTP/2"]


async def get_with(url, version):
    async with httpperf.Session(http_version=version) as s:
        return await s.get(url)


async def get_together(session, url, n):
    return await asyncio.gather(*(session.get(url) for _ in range(n)))


def limiter_host(owner):
    state = (owner._session if isinstance(owner, httpperf.Session) else owner._ae_loop)._limiter_state()
    (host,) = state["hosts"].values()
    usage = (1 if host["active"] else 0) if host["multiplexed"] else host["active"]
    assert state["connections"] == usage
    return host


@pytest.mark.parametrize(
    "version, server_saw", [pytest.param("HTTP/3", "3", marks=needs_http3), ("HTTP/2", "2"), ("HTTP/1.1", "1.1")]
)
async def test_requests_use_the_chosen_version(h2_url, version, server_saw):
    assert (await get_with(h2_url, version)).text == server_saw


@pytest.mark.parametrize("version", ALL_VERSIONS)
async def test_responses_report_their_version(h2_url, version):
    assert (await get_with(h2_url, version)).http_version == version


@pytest.mark.parametrize("version", ALL_VERSIONS)
async def test_headers_arrive_over_every_version(h2_url, version):
    assert (await get_with(h2_url, version)).headers["Content-Type"] == "text/plain"


@pytest.mark.parametrize(
    "version, new_connections", [pytest.param("HTTP/3", 1, marks=needs_http3), ("HTTP/2", 1), ("HTTP/1.1", 5)]
)
async def test_concurrent_requests_share_a_connection(h2_url, version, new_connections):
    async with httpperf.Session(connections="session", http_version=version) as s:
        responses = await get_together(s, h2_url, 5)
    assert sum(r.connect_time > 0 for r in responses) == new_connections


async def two_batches_under_a_host_limit_of_2(url):
    async with httpperf.Session(connections="session", max_host_connections=2) as s:
        return await get_together(s, url, 6), await get_together(s, url, 6)


async def test_a_multiplexed_host_under_a_limit_uses_one_connection(h2_url):
    first, second = await two_batches_under_a_host_limit_of_2(h2_url)
    assert sum(r.connect_time > 0 for r in first + second) == 1


async def test_requests_queue_until_the_host_is_known_to_multiplex(h2_url):
    first, _ = await two_batches_under_a_host_limit_of_2(h2_url)
    assert sum(r.queue_time > 0 for r in first) == 6 - 2


async def test_requests_to_a_multiplexed_host_do_not_queue(h2_url):
    _, second = await two_batches_under_a_host_limit_of_2(h2_url)
    assert sum(r.queue_time > 0 for r in second) == 0


@pytest.mark.parametrize("version", MULTIPLEXED)
async def test_cancelling_one_stream_keeps_the_connection(h2_url, version):
    async with httpperf.Session(connections="session", http_version=version) as s:
        tasks = [asyncio.create_task(s.get(h2_url + "slow")) for _ in range(6)]
        await asyncio.sleep(0.3)
        for task in tasks[::2]:
            task.cancel()
        responses = await asyncio.gather(*tasks[1::2]) + [await s.get(h2_url)]
    assert sum(r.connect_time > 0 for r in responses) == 0


@pytest.mark.parametrize("version", MULTIPLEXED)
async def test_a_multiplexed_host_takes_one_limiter_slot(h2_url, version):
    async with httpperf.Session(connections="session", http_version=version, max_host_connections=2) as s:
        await s.get(h2_url)
        tasks = [asyncio.create_task(s.get(h2_url + "slow")) for _ in range(6)]
        assert await until(lambda: limiter_host(s) == {"active": 6, "waiting": 0, "multiplexed": True})
        await asyncio.gather(*tasks)


async def test_a_failed_request_does_not_stop_multiplexing(h2_url):
    async with httpperf.Session(connections="session", max_host_connections=2) as s:
        await s.get(h2_url)
        with pytest.raises(httpperf.Timeout):
            await s.get(h2_url + "slow", timeout=0.1)
        assert limiter_host(s)["multiplexed"] is True


async def test_the_limiter_follows_a_host_back_to_http1(h2_url):
    async with httpperf.Engine(max_host_connections=4) as engine:
        await httpperf.Session(engine=engine).get(h2_url)
        await httpperf.Session(engine=engine, http_version="HTTP/1.1").get(h2_url)
        assert limiter_host(engine)["multiplexed"] is False
