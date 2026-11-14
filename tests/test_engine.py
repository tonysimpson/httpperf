import asyncio
import errno
import gc
import os
import socket
import subprocess
import sys
import threading
import warnings

import pytest

import httpperf

REFUSED = "http://127.0.0.1:1/"


def open_fds():
    fds = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            fds += not os.readlink("/proc/self/fd/" + fd).startswith("socket:")
        except OSError:
            pass
    return fds


def run_python(code, *args):
    p = subprocess.run([sys.executable, "-c", code, *args], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    return p.stdout.split()


def fail_on(loop, session):
    with pytest.raises(httpperf.RequestError):
        loop.run_until_complete(session.get(REFUSED))


@pytest.fixture
def silent_url():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(100)
    yield "http://127.0.0.1:%d/" % sock.getsockname()[1]
    sock.close()


@pytest.fixture
def two_loops():
    loops = asyncio.new_event_loop(), asyncio.new_event_loop()
    yield loops
    for loop in loops:
        loop.close()


def test_close_stops_the_thread():
    engine = httpperf.Engine()
    engine.close()
    engine._thread.join(timeout=2)
    assert not engine._thread.is_alive()


def test_close_releases_its_files():
    gc.collect()
    before = open_fds()
    engine = httpperf.Engine()
    engine.close()
    engine._thread.join(timeout=2)
    del engine
    gc.collect()
    assert open_fds() == before


def test_an_unclosed_engine_stops_when_dropped():
    loop = asyncio.new_event_loop()
    engine = httpperf.Engine()
    thread = engine._thread
    fail_on(loop, httpperf.Session(engine=engine))
    loop.close()
    del engine
    gc.collect()
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_running_out_of_files_raises_oserror():
    code = """
import asyncio, os, resource
import httpperf

resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
loop = asyncio.new_event_loop()
held = []
try:
    while True:
        held.append(os.open(os.devnull, os.O_RDONLY))
except OSError:
    pass
os.close(held.pop())
try:
    httpperf.Engine()
except OSError as e:
    print(e.errno)
"""
    assert run_python(code) == [str(errno.EMFILE)]


def test_many_requests_and_sessions_do_not_block_pipes():
    code = """
import asyncio, sys
import httpperf


async def main():
    el = httpperf.Engine()
    session = httpperf.Session(engine=el)
    results = await asyncio.gather(*(session.get(sys.argv[1]) for _ in range(20000)), return_exceptions=True)
    sessions = [httpperf.Session(engine=el, connections="session") for _ in range(20000)]
    el.close()
    el._thread.join()
    del sessions
    print(sum(isinstance(r, httpperf.RequestError) for r in results))


asyncio.run(main())
"""
    assert run_python(code, REFUSED) == ["20000"]


def test_a_session_keeps_its_connections_across_asyncio_run(server):
    code = """
import asyncio, sys
import httpperf

session = httpperf.Session()

async def new_connection():
    return (await session.get(sys.argv[1])).connect_time > 0

print(asyncio.run(new_connection()), asyncio.run(new_connection()))
"""
    assert run_python(code, server.url) == ["True", "False"]


def test_an_engine_refuses_a_second_live_loop(two_loops):
    first, second = two_loops
    session = httpperf.Session(engine=httpperf.Engine())
    fail_on(first, session)
    with pytest.raises(RuntimeError, match="another event loop"):
        second.run_until_complete(session.get(REFUSED))


def test_an_engine_moves_to_a_new_loop_once_the_old_one_closes(two_loops):
    first, second = two_loops
    session = httpperf.Session(engine=httpperf.Engine())
    fail_on(first, session)
    first.close()
    fail_on(second, session)


def test_a_closed_engine_refuses_requests():
    engine = httpperf.Engine()
    session = httpperf.Session(engine=engine)
    engine.close()
    with pytest.raises(RuntimeError, match="engine is closed"):
        asyncio.run(session.get(REFUSED))


def test_session_arguments_are_keyword_only():
    with pytest.raises(TypeError):
        httpperf.Session("shared")


def test_engine_arguments_are_keyword_only():
    with pytest.raises(TypeError):
        httpperf.Engine(10)


def default_engine_of_a_new_thread():
    engines = []

    async def main():
        session = httpperf.Session()
        with pytest.raises(httpperf.RequestError):
            await session.get(REFUSED)
        engines.append(session._engine)

    thread = threading.Thread(target=asyncio.run, args=(main(),))
    thread.start()
    thread.join()
    return engines[0]


def test_each_loop_gets_its_own_default_engine():
    assert default_engine_of_a_new_thread() is not default_engine_of_a_new_thread()


def test_a_default_engine_stops_when_its_loop_finishes():
    thread = default_engine_of_a_new_thread()._thread
    gc.collect()
    thread.join(timeout=2)
    assert not thread.is_alive()


async def test_the_default_engine_keeps_connections_while_its_loop_runs(server):
    async def new_connection():
        async with httpperf.Session() as session:
            return (await session.get(server.url)).connect_time > 0

    await new_connection()
    gc.collect()
    assert not await new_connection()


def test_a_session_needs_no_loop_until_its_first_request():
    session = httpperf.Session()
    session.add_cookie_list([httpperf.Cookie(False, "example.com", False, "/", False, 0, "a", "1")])
    assert session.cookies() == {"a": "1"}


async def test_closing_an_engine_with_requests_in_flight_warns(silent_url):
    engine = httpperf.Engine()
    asyncio.create_task(httpperf.Session(engine=engine).get(silent_url))
    await asyncio.sleep(0.1)
    with pytest.warns(ResourceWarning, match="1 requests in flight"):
        engine.close()


@pytest.mark.filterwarnings("ignore::ResourceWarning")
async def test_closing_an_engine_fails_its_requests_in_flight(silent_url):
    engine = httpperf.Engine()
    task = asyncio.create_task(httpperf.Session(engine=engine).get(silent_url))
    await asyncio.sleep(0.1)
    engine.close()
    with pytest.raises(httpperf.RequestError, match="engine closed"):
        await task


def test_dropping_a_loop_after_cancelling_does_not_warn(silent_url):
    loop = asyncio.new_event_loop()
    task = loop.create_task(httpperf.Session().get(silent_url))
    loop.run_until_complete(asyncio.sleep(0.1))
    task.cancel()
    loop.run_until_complete(asyncio.sleep(0.1))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        loop.close()
        del task, loop
        gc.collect()


async def test_cancelled_requests_do_not_strand_others(server):
    async with httpperf.Session() as session:
        tasks = [asyncio.create_task(session.get(server.url + "/slow")) for _ in range(10)]
        await asyncio.sleep(0.2)
        for task in tasks[::2]:
            task.cancel()
        server.release.set()
        responses = await asyncio.wait_for(asyncio.gather(*tasks[1::2]), 5)
    assert [r.status_code for r in responses] == [200] * 5
