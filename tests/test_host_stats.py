import pytest

import httpperf


async def test_host_stats_are_off_by_default():
    with pytest.raises(RuntimeError, match="host_stats=True"):
        httpperf.Engine().host_stats()


async def test_requests_are_counted_per_host(make_server):
    a, b = make_server(), make_server()
    async with httpperf.Engine(host_stats=True) as engine:
        session = httpperf.Session(engine=engine)
        for url in [a.url, a.url + "/big", b.url]:
            await session.get(url)
        assert {k: v["requests"] for k, v in engine.host_stats().items()} == {a.url: 2, b.url: 1}


async def test_spellings_of_a_host_share_its_counts(server):
    async with httpperf.Engine(host_stats=True) as engine:
        session = httpperf.Session(engine=engine)
        await session.get(server.url)
        await session.get(server.url.replace("http://", "HTTP://"))
        assert engine.host_stats()[server.url]["requests"] == 2


async def test_errors_are_counted_per_host():
    async with httpperf.Engine(host_stats=True) as engine:
        with pytest.raises(httpperf.RequestError):
            await httpperf.Session(engine=engine).get("http://127.0.0.1:1/")
        assert engine.host_stats()["http://127.0.0.1:1"]["errors"] == 1


async def test_every_host_is_kept():
    async with httpperf.Engine(host_stats=True) as engine:
        session = httpperf.Session(engine=engine)
        for i in range(1, 201):
            with pytest.raises(httpperf.RequestError):
                await session.get("http://127.0.0.%d:1/" % i)
        assert len(engine.host_stats()) == 200
