import asyncio
import subprocess
import sys
import sysconfig
import threading

import pytest

import httpperf

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def in_threads(n, target):
    results = [None] * n
    barrier = threading.Barrier(n)

    def run(i):
        barrier.wait()
        results[i] = target()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def statuses_from_own_loop(url):
    async def main():
        async with httpperf.Engine() as engine:
            s = httpperf.Session(engine=engine)
            return [r.status_code for r in await asyncio.gather(*(s.get(url) for _ in range(50)))]

    return asyncio.run(main())


@pytest.mark.skipif(not FREE_THREADED, reason="needs a free-threaded Python")
def test_importing_keeps_the_gil_disabled():
    code = "import sys, httpperf; print(sys._is_gil_enabled())"
    assert subprocess.run([sys.executable, "-c", code], capture_output=True, text=True).stdout.split() == ["False"]


def test_threads_with_their_own_loops_and_engines(server):
    assert in_threads(4, lambda: statuses_from_own_loop(server.url)) == [[200] * 50] * 4


def bodies_read_at_once(response):
    return {id(body) for body in in_threads(8, response._resp.get_body)}


async def test_a_body_read_from_many_threads_is_one_object(s, httpbin):
    responses = [await s.get(httpbin.url + "/range/100000") for _ in range(100)]
    assert [len(bodies_read_at_once(r)) for r in responses] == [1] * 100


def outcomes_while_closing(url, close):
    async def main():
        engine = httpperf.Engine()
        s = httpperf.Session(engine=engine)
        tasks = [asyncio.ensure_future(s.get(url)) for _ in range(200)]
        await asyncio.sleep(0)
        closer = threading.Thread(target=close, args=(engine, s))
        closer.start()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        closer.join()
        engine.close()
        return {type(r).__name__ for r in results}

    return asyncio.run(main())


@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_closing_a_session_from_another_thread(server):
    outcomes = outcomes_while_closing(server.url, lambda engine, s: s.close())
    assert outcomes <= {"Response", "RuntimeError", "RequestError"}


@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_closing_an_engine_from_another_thread(server):
    outcomes = outcomes_while_closing(server.url, lambda engine, s: engine.close())
    assert outcomes <= {"Response", "RequestError"}
