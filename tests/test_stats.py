import asyncio

import httpperf

VERSIONS = {"HTTP/1.0": 0, "HTTP/1.1": 0, "HTTP/2": 0, "HTTP/3": 0}


def counts(stats):
    return {k: v for k, v in stats.items() if not k.startswith("bytes")}


def test_new_stats_are_zero():
    zero = {
        "requests": 0,
        "errors": 0,
        "cancelled": 0,
        "connections": 0,
        "connections_open": None,
        "tls_handshakes": 0,
        "http_versions": VERSIONS,
        "bytes_sent": 0,
        "bytes_received": 0,
        "running": 0,
        "queued": 0,
    }
    engine = httpperf.Engine()
    assert engine.stats() == dict(zero, connections_open=0)
    assert httpperf.Session(engine=engine).stats() == zero
    assert httpperf.Session(engine=engine, connections="session").stats() == dict(zero, connections_open=0)
    engine.close()


def test_requests_and_bytes(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session") as s:
            responses = [await s.get(server.url + "/fast") for _ in range(3)]
            return responses, s.stats(), s.engine.stats()

    responses, session, engine = asyncio.run(main())
    assert counts(session) == {
        "requests": 3,
        "errors": 0,
        "cancelled": 0,
        "connections": 1,
        "connections_open": 1,
        "tls_handshakes": 0,
        "http_versions": dict(VERSIONS, **{"HTTP/1.1": 3}),
        "running": 0,
        "queued": 0,
    }
    assert session["bytes_received"] == sum(len(r.header) + len(r.body) for r in responses)
    assert session["bytes_sent"] > 0
    assert engine == session


def test_body_counts_as_bytes_sent(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as s:
            await s.get(server.url + "/fast")
            before = s.stats()["bytes_sent"]
            await s.get(server.url + "/fast", data=b"x" * 1000)
            return before, s.stats()["bytes_sent"]

    before, after = asyncio.run(main())
    assert after - 2 * before > 1000


def test_sessions_count_separately(server):
    async def main():
        async with httpperf.Engine() as engine:
            a = httpperf.Session(engine=engine)
            b = httpperf.Session(engine=engine)
            await a.get(server.url + "/fast")
            await asyncio.gather(b.get(server.url + "/fast"), b.get(server.url + "/fast"))
            return a.stats()["requests"], b.stats()["requests"], engine.stats()["requests"]

    assert asyncio.run(main()) == (1, 2, 3)


def test_errors():
    async def main():
        async with httpperf.Session() as s:
            await asyncio.gather(*(s.get("http://127.0.0.1:9/") for _ in range(3)), return_exceptions=True)
            return s.stats()

    stats = asyncio.run(main())
    assert (stats["requests"], stats["errors"], stats["running"]) == (0, 3, 0)


def test_running_queued_and_cancelled(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=2) as s:
            tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(5)]
            while s.stats()["queued"] < 3:
                await asyncio.sleep(0.01)
            busy = s.stats()
            tasks[0].cancel()
            tasks[4].cancel()
            server.release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            return busy, s.stats()

    busy, idle = asyncio.run(main())
    assert (busy["running"], busy["queued"]) == (2, 3)
    assert (idle["running"], idle["queued"], idle["requests"], idle["cancelled"]) == (0, 0, 3, 2)


def open_connections(session):
    return session.stats()["connections_open"]


def test_a_shared_pool_session_has_no_connection_count(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as s:
            await s.get(server.url + "/fast")
            return open_connections(s)

    assert asyncio.run(main()) is None


def test_own_pool_sessions_count_their_connections_and_the_engine_counts_all(server):
    async def main():
        async with httpperf.Engine() as engine:
            shared = httpperf.Session(engine=engine, http_version="HTTP/1.1")
            three = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections="session")
            one = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections="session")
            await asyncio.gather(*(shared.get(server.url + "/fast") for _ in range(2)))
            await asyncio.gather(*(three.get(server.url + "/fast") for _ in range(3)))
            await one.get(server.url + "/fast")
            return open_connections(three), open_connections(one), open_connections(engine)

    three, one, engine = asyncio.run(main())
    assert three == 3
    assert one == 1
    assert engine == 2 + 3 + 1


def test_closing_a_session_closes_its_connections(server):
    async def main():
        async with httpperf.Engine() as engine:
            s = httpperf.Session(engine=engine, http_version="HTTP/1.1", connections="session")
            await asyncio.gather(*(s.get(server.url + "/fast") for _ in range(3)))
            before = open_connections(engine)
            s.close()
            for _ in range(100):
                if open_connections(engine) == 0:
                    break
                await asyncio.sleep(0.01)
            return before, open_connections(engine), open_connections(s)

    engine_before, engine_after, closed_session = asyncio.run(main())
    assert engine_before == 3
    assert engine_after == 0
    assert closed_session == 0
