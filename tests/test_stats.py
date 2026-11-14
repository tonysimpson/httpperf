import asyncio

import httpperf


def stats_after(url, requests=1, **options):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", **options) as s:
            for _ in range(requests):
                try:
                    await s.get(url)
                except httpperf.RequestError:
                    pass
            return s.stats()

    return asyncio.run(main())


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


def test_counts_requests(server):
    assert stats_after(server.url + "/fast", 3)["requests"] == 3


def test_counts_new_connections_not_reused_ones(server):
    assert stats_after(server.url + "/fast", 3)["connections"] == 1


def test_counts_http_versions(server):
    assert stats_after(server.url + "/fast", 3)["http_versions"]["HTTP/1.1"] == 3


def test_counts_errors():
    assert stats_after("http://127.0.0.1:9/", 3)["errors"] == 3


def test_counts_bytes_received(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as s:
            r = await s.get(server.url + "/fast")
            return s.stats()["bytes_received"], len(r.header) + len(r.body)

    received, response_size = asyncio.run(main())
    assert received == response_size


def test_counts_a_request_body_as_bytes_sent(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as s:
            await s.get(server.url + "/fast", data=b"x" * 1000)
            return s.stats()["bytes_sent"]

    assert asyncio.run(main()) > 1000


def test_counts_cancelled_requests(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as s:
            task = asyncio.create_task(s.get(server.url + "/slow"))
            await asyncio.sleep(0.2)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return await eventually(lambda: s.stats()["cancelled"], 1)

    assert asyncio.run(main()) == 1


def test_running_and_queued_requests(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=2) as s:
            tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(5)]
            while s.stats()["queued"] < 3:
                await asyncio.sleep(0.01)
            stats = s.stats()
            server.release.set()
            await asyncio.gather(*tasks)
            return stats["running"], stats["queued"]

    assert asyncio.run(main()) == (2, 3)


def test_the_engine_counts_all_its_sessions(server):
    async def main():
        async with httpperf.Engine() as engine:
            for requests in (1, 2):
                s = httpperf.Session(engine=engine)
                await get_together(s, server.url + "/fast", requests)
            return engine.stats()["requests"]

    assert asyncio.run(main()) == 3


def test_a_shared_pool_session_has_no_connection_count(server):
    assert stats_after(server.url + "/fast")["connections_open"] is None


def test_an_own_pool_session_counts_its_open_connections(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session") as s:
            await get_together(s, server.url + "/fast", 3)
            return open_connections(s)

    assert asyncio.run(main()) == 3


def test_the_engine_counts_open_connections_in_every_pool(server):
    async def main():
        async with httpperf.Engine() as engine:
            for connections in ("shared", "session"):
                s = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections=connections)
                await get_together(s, server.url + "/fast", 2)
            return open_connections(engine)

    assert asyncio.run(main()) == 4


def test_closing_a_session_closes_its_connections(server):
    async def main():
        async with httpperf.Engine() as engine:
            s = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections="session")
            await get_together(s, server.url + "/fast", 3)
            s.close()
            return await eventually(lambda: open_connections(engine), 0)

    assert asyncio.run(main()) == 0
