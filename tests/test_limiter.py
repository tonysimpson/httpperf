import asyncio
import socket
from urllib.parse import urlsplit

import pytest
from helpers import until

import httpperf
from httpperf import _httpperf

IDLE = {"active": 0, "waiting": 0, "multiplexed": False}


def limiter_state(owner):
    state = (owner._session if isinstance(owner, httpperf.Session) else owner._ae_loop)._limiter_state()
    in_use = sum((1 if h["active"] else 0) if h["multiplexed"] else h["active"] for h in state["hosts"].values())
    assert state["connections"] == in_use
    return state


def host_key(url):
    return "{0.scheme}://{0.netloc}".format(urlsplit(url))


def host(owner, url):
    return limiter_state(owner)["hosts"].get(host_key(url))


def waiting(owner, url):
    return (host(owner, url) or IDLE)["waiting"]


def limited_session(limit, **options):
    return httpperf.Session(connections="session", max_host_connections=limit, **options)


def slow_requests(session, url, n):
    return [asyncio.create_task(session.get(url + "/slow")) for _ in range(n)]


def refused_urls(n):
    sockets = [socket.socket() for _ in range(n)]
    for sock in sockets:
        sock.bind(("127.0.0.1", 0))
    urls = ["http://127.0.0.1:%d/" % sock.getsockname()[1] for sock in sockets]
    for sock in sockets:
        sock.close()
    return urls


async def fail(session, url):
    with pytest.raises(httpperf.RequestError):
        await session.get(url)


@pytest.fixture
def short_host_idle():
    _httpperf._set_host_idle_seconds(0.1)
    yield
    _httpperf._set_host_idle_seconds(60)


def test_no_limiter_without_limits():
    assert httpperf.Session()._session._limiter_state() is None


def test_no_engine_limiter_without_limits():
    assert httpperf.Engine()._ae_loop._limiter_state() is None


async def test_requests_over_the_host_limit_wait(server):
    async with limited_session(2) as s:
        tasks = slow_requests(s, server.url, 5)
        assert await until(lambda: waiting(s, server.url) == 3)
        assert host(s, server.url)["active"] == 2
        server.release.set()
        await asyncio.gather(*tasks)


async def test_finished_requests_release_their_slots(server):
    async with limited_session(2) as s:
        tasks = slow_requests(s, server.url, 5)
        await until(lambda: waiting(s, server.url) == 3)
        server.release.set()
        await asyncio.gather(*tasks)
        assert host(s, server.url) == IDLE


async def test_waiting_requests_report_their_queue_time(server):
    async with limited_session(2) as s:
        tasks = slow_requests(s, server.url, 5)
        await until(lambda: waiting(s, server.url) == 3)
        server.release.set()
        responses = await asyncio.gather(*tasks)
    assert sum(r.queue_time > 0 for r in responses) == 3


async def test_the_total_limit_spans_hosts(make_server):
    a, b = make_server(), make_server()
    async with httpperf.Engine(max_total_connections=3) as engine:
        s = httpperf.Session(engine=engine)
        first = slow_requests(s, a.url, 3)
        await until(lambda: limiter_state(engine)["connections"] == 3)
        second = [asyncio.create_task(s.get(b.url)) for _ in range(2)]
        assert await until(lambda: waiting(engine, b.url) == 2)
        a.release.set()
        await asyncio.gather(*first, *second)


async def test_failed_requests_release_their_slots():
    url = refused_urls(1)[0]
    async with limited_session(1) as s:
        await asyncio.gather(*(fail(s, url) for _ in range(5)))
        assert host(s, url) == IDLE


async def test_a_cancelled_request_gives_its_slot_to_a_waiting_one(server):
    async with limited_session(2) as s:
        tasks = slow_requests(s, server.url, 4)
        await until(lambda: waiting(s, server.url) == 2)
        tasks[0].cancel()
        assert await until(lambda: waiting(s, server.url) == 1)
        server.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_a_cancelled_waiting_request_leaves_the_queue(server):
    async with limited_session(1) as s:
        tasks = slow_requests(s, server.url, 3)
        await until(lambda: waiting(s, server.url) == 2)
        tasks[2].cancel()
        assert await until(lambda: waiting(s, server.url) == 1)
        server.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_hosts_take_turns_under_the_total_limit(server):
    other = server.url.replace("127.0.0.1", "localhost")
    async with httpperf.Engine(max_total_connections=1) as engine:
        s = httpperf.Session(engine=engine)
        first = asyncio.create_task(s.get(server.url + "/slow"))
        await until(lambda: limiter_state(engine)["connections"] == 1)
        tasks = [asyncio.create_task(s.get(server.url + "/a%d" % i)) for i in range(3)]
        await until(lambda: waiting(engine, server.url) == 3)
        tasks += [asyncio.create_task(s.get(other + "/b%d" % i)) for i in range(3)]
        await until(lambda: waiting(engine, other) == 3)
        server.release.set()
        await asyncio.gather(first, *tasks)
    assert server.started == ["/slow", "/a0", "/b0", "/a1", "/b1", "/a2", "/b2"]


async def test_idle_host_records_are_freed(short_host_idle):
    async with limited_session(1) as s:
        for url in refused_urls(100):
            await fail(s, url)
        await asyncio.sleep(0.2)
        url = refused_urls(1)[0]
        for _ in range(60):
            await fail(s, url)
        assert list(limiter_state(s)["hosts"]) == [host_key(url)]


async def test_busy_host_records_are_kept(server, short_host_idle):
    async with limited_session(1) as s:
        tasks = slow_requests(s, server.url, 2)
        await until(lambda: waiting(s, server.url) == 1)
        await asyncio.sleep(0.2)
        for url in refused_urls(10):
            await fail(s, url)
        assert host(s, server.url) == {"active": 1, "waiting": 1, "multiplexed": False}
        server.release.set()
        await asyncio.gather(*tasks)


async def test_spellings_of_a_host_share_its_slots(server):
    port = server.url.rsplit(":", 1)[1]
    spellings = [
        "HTTP://127.0.0.1:%s/slow?a=1" % port,
        "http://user:pw@127.0.0.1:%s/slow" % port,
        "http://127.0.0.1:%s?b=2#c" % port,
    ]
    async with limited_session(1) as s:
        tasks = slow_requests(s, server.url, 1)
        tasks += [asyncio.create_task(s.get(url)) for url in spellings]
        tasks += slow_requests(s, server.url, 2)
        assert await until(lambda: waiting(s, server.url) == 5)
        server.release.set()
        await asyncio.gather(*tasks)


async def test_host_keys_of_unusual_urls():
    urls = [
        "http://[::1]:9/",
        "http://127.0.0.1:9?q=1",
        "http://127.0.0.1:9#f",
        "http://u@127.0.0.2:9/",
        "http://127.0.0.1:90/",
    ]
    async with limited_session(1) as s:
        for url in urls * 2:
            await fail(s, url)
        assert sorted(limiter_state(s)["hosts"]) == [
            "http://127.0.0.1:9",
            "http://127.0.0.1:90",
            "http://127.0.0.2:9",
            "http://[::1]:9",
        ]
