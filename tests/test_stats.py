import asyncio

import httpperf


async def stats_after(url, requests=1, **options):
    async with httpperf.Session(http_version="HTTP/1.1", **options) as s:
        for _ in range(requests):
            try:
                await s.get(url)
            except httpperf.RequestError:
                pass
        return s.stats()


def open_connections(session_or_engine):
    return session_or_engine.stats()["connections_open"]


async def eventually(get, expected):
    for _ in range(100):
        if get() == expected:
            break
        await asyncio.sleep(0.01)
    return get()


async def get_together(session, url, requests):
    await asyncio.gather(*(session.get(url) for _ in range(requests)))


def test_a_new_session_has_counted_nothing():
    stats = httpperf.Session().stats()
    assert [name for name, value in stats.items() if value] == ["http_versions"]


async def test_counts_requests(server):
    assert (await stats_after(server.url + "/fast", 3))["requests"] == 3


async def test_counts_new_connections_not_reused_ones(server):
    assert (await stats_after(server.url + "/fast", 3))["connections"] == 1


async def test_counts_http_versions(server):
    assert (await stats_after(server.url + "/fast", 3))["http_versions"]["HTTP/1.1"] == 3


async def test_counts_errors():
    assert (await stats_after("http://127.0.0.1:9/", 3))["errors"] == 3


async def test_counts_bytes_received(server):
    async with httpperf.Session(http_version="HTTP/1.1") as s:
        r = await s.get(server.url + "/fast")
        assert s.stats()["bytes_received"] == len(r.header) + len(r.body)


async def test_counts_a_request_body_as_bytes_sent(server):
    async with httpperf.Session(http_version="HTTP/1.1") as s:
        await s.get(server.url + "/fast", data=b"x" * 1000)
        assert s.stats()["bytes_sent"] > 1000


async def test_counts_cancelled_requests(server):
    async with httpperf.Session(http_version="HTTP/1.1") as s:
        task = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert await eventually(lambda: s.stats()["cancelled"], 1) == 1


async def test_running_and_queued_requests(server):
    async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=2) as s:
        tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(5)]
        await eventually(lambda: s.stats()["queued"], 3)
        stats = s.stats()
        server.release.set()
        await asyncio.gather(*tasks)
        assert (stats["running"], stats["queued"]) == (2, 3)


async def test_the_engine_counts_all_its_sessions(server):
    async with httpperf.Engine() as engine:
        for requests in (1, 2):
            await get_together(httpperf.Session(engine=engine), server.url + "/fast", requests)
        assert engine.stats()["requests"] == 3


async def test_a_shared_pool_session_has_no_connection_count(server):
    assert (await stats_after(server.url + "/fast"))["connections_open"] is None


async def test_an_own_pool_session_counts_its_open_connections(server):
    async with httpperf.Session(http_version="HTTP/1.1", connections="session") as s:
        await get_together(s, server.url + "/fast", 3)
        assert open_connections(s) == 3


async def test_the_engine_counts_open_connections_in_every_pool(server):
    async with httpperf.Engine() as engine:
        for connections in ("shared", "session"):
            s = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections=connections)
            await get_together(s, server.url + "/fast", 2)
        assert open_connections(engine) == 4


async def test_closing_a_session_closes_its_connections(server):
    async with httpperf.Engine() as engine:
        s = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections="session")
        await get_together(s, server.url + "/fast", 3)
        s.close()
        assert await eventually(lambda: open_connections(engine), 0) == 0
