import asyncio

import httpperf


def usage(host):
    if host["multiplexed"]:
        return 1 if host["active"] else 0
    return host["active"]


def check(state):
    assert state["connections"] == sum(usage(h) for h in state["hosts"].values())
    return state


def key(url):
    return url if url.count(":") == 2 else url + ":80"


async def wait_until(get_state, condition, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        state = check(get_state())
        if condition(state):
            return state
        assert asyncio.get_running_loop().time() < deadline, state
        await asyncio.sleep(0.01)


def test_host_limit_queues_and_drains(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=2) as s:
            state = s._session._limiter_state
            tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(5)]
            busy = await wait_until(state, lambda st: st["hosts"] and st["hosts"][key(server.url)]["waiting"] == 3)
            server.release.set()
            responses = await asyncio.gather(*tasks)
            return busy, check(state()), responses

    busy, idle, responses = asyncio.run(main())
    assert busy["connections"] == 2 and busy["hosts"][key(server.url)]["active"] == 2
    assert idle["connections"] == 0
    assert idle["hosts"][key(server.url)] == {"active": 0, "waiting": 0, "multiplexed": False}
    assert sorted(r.queue_time > 0 for r in responses) == [False, False, True, True, True]


def test_total_limit_across_hosts(make_server):
    a, b = make_server(), make_server()

    async def main():
        async with httpperf.Engine(max_total_connections=3) as engine:
            state = engine._ae_loop._limiter_state
            async with httpperf.Session(engine=engine, http_version="HTTP/1.1") as s:
                first = [asyncio.create_task(s.get(a.url + "/slow")) for _ in range(3)]
                await wait_until(state, lambda st: st["connections"] == 3)
                second = [asyncio.create_task(s.get(b.url + "/fast")) for _ in range(2)]
                busy = await wait_until(
                    state, lambda st: key(b.url) in st["hosts"] and st["hosts"][key(b.url)]["waiting"] == 2
                )
                a.release.set()
                await asyncio.gather(*first, *second)
                return busy, check(state())

    busy, idle = asyncio.run(main())
    assert busy["connections"] == 3
    assert busy["hosts"][key(a.url)]["active"] == 3
    assert idle["connections"] == 0
    assert all(h["active"] == h["waiting"] == 0 for h in idle["hosts"].values())


def test_failed_requests_release_their_slots():
    async def main():
        async with httpperf.Session(connections="session", max_host_connections=1) as s:
            results = await asyncio.gather(*(s.get("http://127.0.0.1:9/") for _ in range(5)), return_exceptions=True)
            return results, check(s._session._limiter_state())

    results, state = asyncio.run(main())
    assert all(isinstance(r, httpperf.RequestError) for r in results)
    assert state["connections"] == 0
    assert state["hosts"]["http://127.0.0.1:9"] == {"active": 0, "waiting": 0, "multiplexed": False}


def test_cancelled_requests_release_their_slots(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=2) as s:
            state = s._session._limiter_state
            tasks = [asyncio.create_task(s.get(server.url + "/slow")) for _ in range(6)]
            await wait_until(state, lambda st: st["hosts"] and st["hosts"][key(server.url)]["waiting"] == 4)
            for task in tasks[::2]:
                task.cancel()
            await wait_until(state, lambda st: st["hosts"][key(server.url)]["waiting"] == 1)
            server.release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            return check(state()), [t.cancelled() for t in tasks]

    state, cancelled = asyncio.run(main())
    assert state["connections"] == 0
    assert state["hosts"][key(server.url)] == {"active": 0, "waiting": 0, "multiplexed": False}
    assert cancelled == [True, False] * 3


def test_no_limiter_without_limits():
    assert httpperf.Session()._session._limiter_state() is None
    assert httpperf.Engine()._ae_loop._limiter_state() is None
