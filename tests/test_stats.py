import asyncio
import contextlib

import httpperf

REFUSED = "http://127.0.0.1:9/"


async def count(name, url, requests=1):
    async with httpperf.Session() as s:
        for _ in range(requests):
            with contextlib.suppress(httpperf.RequestError):
                await s.get(url)
        return s.stats()[name]


async def get_together(session, url, requests):
    await asyncio.gather(*(session.get(url) for _ in range(requests)))


async def until(condition):
    for _ in range(100):
        if condition():
            return True
        await asyncio.sleep(0.01)
    return False


def test_a_new_session_has_counted_nothing():
    stats = httpperf.Session().stats()
    assert [name for name, value in stats.items() if value] == ["http_versions"]


async def test_counts_requests(server):
    assert await count("requests", server.url, 3) == 3


async def test_counts_new_connections_not_reused_ones(server):
    assert await count("connections", server.url, 3) == 1


async def test_counts_http_versions(server):
    assert await count("http_versions", server.url) == {"HTTP/1.0": 0, "HTTP/1.1": 1, "HTTP/2": 0, "HTTP/3": 0}


async def test_counts_errors():
    assert await count("errors", REFUSED, 3) == 3


async def test_counts_bytes_received(server):
    async with httpperf.Session() as s:
        r = await s.get(server.url)
        assert s.stats()["bytes_received"] == len(r.header) + len(r.body)


async def test_counts_a_request_body_as_bytes_sent(server):
    async with httpperf.Session() as s:
        await s.get(server.url, data=b"x" * 1000)
        assert s.stats()["bytes_sent"] > 1000


async def test_counts_cancelled_requests(server):
    async with httpperf.Session() as s:
        task = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        task.cancel()
        assert await until(lambda: s.stats()["cancelled"] == 1)


async def test_requests_over_the_host_limit_queue(server):
    async with httpperf.Session(connections="session", max_host_connections=2) as s:
        tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(5)]
        assert await until(lambda: s.stats()["queued"] == 3)
        assert s.stats()["running"] == 2
        server.release.set()
        await asyncio.gather(*tasks)


async def test_the_engine_counts_all_its_sessions(server):
    async with httpperf.Engine() as engine:
        await get_together(httpperf.Session(engine=engine), server.url, 1)
        await get_together(httpperf.Session(engine=engine), server.url, 2)
        assert engine.stats()["requests"] == 3


async def test_a_shared_pool_session_has_no_connection_count(server):
    assert await count("connections_open", server.url) is None


async def test_an_own_pool_session_counts_its_open_connections(server):
    async with httpperf.Session(connections="session") as s:
        await get_together(s, server.url, 3)
        assert s.stats()["connections_open"] == 3


async def test_the_engine_counts_open_connections_in_every_pool(server):
    async with httpperf.Engine() as engine:
        await get_together(httpperf.Session(engine=engine, connections="shared"), server.url, 2)
        await get_together(httpperf.Session(engine=engine, connections="session"), server.url, 2)
        assert engine.stats()["connections_open"] == 4


async def test_closing_a_session_closes_its_connections(server):
    async with httpperf.Engine() as engine:
        s = httpperf.Session(engine=engine, connections="session")
        await get_together(s, server.url, 3)
        s.close()
        assert await until(lambda: engine.stats()["connections_open"] == 0)
