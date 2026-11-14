import asyncio
import random

import pytest
from helpers import until

import httpperf


async def test_cancel_stops_the_transfer(server):
    async with httpperf.Session() as s:
        task = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        task.cancel()
        assert await until(lambda: server.hung_up == 1)


async def test_wait_for_stops_the_transfer(server):
    async with httpperf.Session() as s:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(s.get(server.url + "/slow"), 0.2)
        assert await until(lambda: server.hung_up == 1)


async def test_cancel_frees_the_connection_slot(server):
    async with httpperf.Session(connections="session", max_host_connections=1) as s:
        slow = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        slow.cancel()
        assert (await asyncio.wait_for(s.get(server.url), 2)).status_code == 200


async def test_a_cancelled_queued_request_is_never_sent(server):
    async with httpperf.Session(connections="session", max_host_connections=1) as s:
        slow = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        queued = asyncio.create_task(s.get(server.url + "/queued"))
        after = asyncio.create_task(s.get(server.url + "/after"))
        await asyncio.sleep(0.2)
        queued.cancel()
        await asyncio.sleep(0.1)
        server.release.set()
        await asyncio.gather(slow, after)
    assert server.started == ["/slow", "/after"]


async def test_a_request_cancelled_before_the_engine_starts_it_is_never_sent(server):
    async with httpperf.Session() as s:
        task = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0.2)
        await s.get(server.url + "/later")
    assert "/slow" not in server.started


async def test_the_session_works_after_cancelling_many(server):
    async with httpperf.Session() as s:
        tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(100)]
        await asyncio.sleep(0.5)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert (await s.get(server.url)).status_code == 200


async def test_requests_not_cancelled_still_complete_when_cancels_race_completion(server):
    rng = random.Random(1)
    async with httpperf.Session() as s:
        for _ in range(30):
            tasks = [asyncio.create_task(s.get(server.url)) for _ in range(50)]
            await asyncio.sleep(rng.random() * 0.005)
            for task in rng.sample(tasks, 25):
                task.cancel()
            await asyncio.wait_for(asyncio.wait(tasks), 5)
            statuses = [t.result().status_code for t in tasks if not t.cancelled()]
            assert statuses == [200] * len(statuses)


async def test_cancel_after_the_engine_closed(server):
    engine = httpperf.Engine()
    async with httpperf.Session(engine=engine) as s:
        task = asyncio.create_task(s.get(server.url + "/slow"))
        await asyncio.sleep(0.2)
        with pytest.warns(ResourceWarning):
            engine.close()
        task.cancel()
        with pytest.raises((asyncio.CancelledError, httpperf.RequestError)):
            await task
