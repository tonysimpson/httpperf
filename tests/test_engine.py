import asyncio
import errno
import gc
import http.server
import os
import socket
import subprocess
import sys
import threading
import time
import warnings

import pytest

import httpperf


def open_fds():
    fds = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            fds += not os.readlink("/proc/self/fd/" + fd).startswith("socket:")
        except OSError:
            pass
    return fds


def test_stop_releases_thread_and_fds():
    loop = asyncio.new_event_loop()
    try:
        gc.collect()
        before = open_fds()
        el = httpperf.Engine()
        thread = el._thread
        el.close()
        thread.join(timeout=2)
        assert not thread.is_alive()
        del el
        gc.collect()
        assert open_fds() == before
    finally:
        loop.close()


def test_out_of_fds_raises():
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
    print("OSError", e.errno)
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["OSError", str(errno.EMFILE)]


def test_many_requests_and_sessions_do_not_block_pipes():
    code = """
import asyncio, socket
import httpperf

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
url = "http://127.0.0.1:%d/" % sock.getsockname()[1]
sock.close()


async def main():
    el = httpperf.Engine()
    session = httpperf.Session(engine=el)
    results = await asyncio.gather(*(session.get(url) for _ in range(20000)), return_exceptions=True)
    sessions = [httpperf.Session(engine=el, connections="session") for _ in range(20000)]
    el.close()
    el._thread.join()
    del sessions
    print(sum(isinstance(r, httpperf.RequestError) for r in results))


asyncio.run(main())
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["20000"]


def test_session_keeps_its_engine_across_asyncio_run():
    code = """
import asyncio, http.server, threading
import httpperf

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
url = "http://127.0.0.1:%d/" % server.server_address[1]
session = httpperf.Session(http_version="HTTP/1.1")

async def get():
    return (await session.get(url)).connect_time > 0

print(asyncio.run(get()), asyncio.run(get()))
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["True", "False"]


def test_engine_binds_to_one_live_loop():
    engine = httpperf.Engine()
    session = httpperf.Session(engine=engine)
    first = asyncio.new_event_loop()
    second = asyncio.new_event_loop()
    try:
        with pytest.raises(httpperf.RequestError):
            first.run_until_complete(session.get("http://127.0.0.1:1/"))
        with pytest.raises(RuntimeError, match="another event loop"):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
        first.close()
        with pytest.raises(httpperf.RequestError):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
        engine.close()
        with pytest.raises(RuntimeError, match="engine is closed"):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
    finally:
        first.close()
        second.close()


def test_arguments_are_keyword_only():
    with pytest.raises(TypeError):
        httpperf.Session("shared")
    with pytest.raises(TypeError):
        httpperf.Engine(10)


def test_unclosed_engine_stops_when_dropped():
    loop = asyncio.new_event_loop()
    try:
        engine = httpperf.Engine()
        thread = engine._thread
        with pytest.raises(httpperf.RequestError):
            loop.run_until_complete(httpperf.Session(engine=engine).get("http://127.0.0.1:1/"))
    finally:
        loop.close()
    del engine
    gc.collect()
    thread.join(timeout=2)
    assert not thread.is_alive()


def engine_threads():
    return sum(t.name == "httpperf-engine" for t in threading.enumerate())


def wait_for_engine_threads(n):
    deadline = time.monotonic() + 2
    while engine_threads() != n and time.monotonic() < deadline:
        gc.collect()
        time.sleep(0.01)
    return engine_threads()


@pytest.fixture
def silent_url():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(100)
    yield "http://127.0.0.1:%d/" % sock.getsockname()[1]
    sock.close()


def test_default_engine_is_per_loop():
    gc.collect()
    before = wait_for_engine_threads(engine_threads())
    engines = []
    errors = []

    async def main():
        session = httpperf.Session()
        with pytest.raises(httpperf.RequestError):
            await session.get("http://127.0.0.1:1/")
        engines.append(session._engine)

    def run():
        try:
            asyncio.run(main())
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert engines[0] is not engines[1]
    del engines[:]
    assert wait_for_engine_threads(before) == before


def test_session_needs_no_loop_until_first_request():
    session = httpperf.Session()
    session.add_cookie_list([httpperf.Cookie(False, "example.com", False, "/", False, 0, "a", "1")])
    assert session.cookies() == {"a": "1"}
    session.close()


def test_close_with_requests_in_flight_warns(silent_url):
    async def main():
        engine = httpperf.Engine()
        task = asyncio.ensure_future(httpperf.Session(engine=engine).get(silent_url))
        await asyncio.sleep(0.1)
        with pytest.warns(ResourceWarning, match="1 requests in flight"):
            engine.close()
        with pytest.raises(httpperf.RequestError, match="engine closed"):
            await task

    asyncio.run(main())


def test_dropped_loop_after_cancelling_does_not_warn(silent_url):
    gc.collect()
    before = wait_for_engine_threads(engine_threads())
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
    assert wait_for_engine_threads(before) == before


def test_default_engine_lasts_as_long_as_its_loop():
    import http.server

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:%d/" % server.server_address[1]

    async def journey():
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            return (await session.get(url)).connect_time > 0

    async def main():
        first = await journey()
        gc.collect()
        return first, await journey()

    try:
        assert asyncio.run(main()) == (True, False)
    finally:
        server.shutdown()
        server.server_close()


def test_cancelled_requests_do_not_strand_others():
    release = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            release.wait(10)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:%d/" % server.server_address[1]
    errors = []

    async def main():
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: errors.append(context))
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            tasks = [asyncio.create_task(session.get(url)) for _ in range(10)]
            await asyncio.sleep(0.2)
            for task in tasks[::2]:
                task.cancel()
            release.set()
            done, pending = await asyncio.wait(tasks[1::2], timeout=5)
            await asyncio.sleep(0.1)
            return [t.result().status_code for t in done], len(pending)

    try:
        assert asyncio.run(main()) == ([200] * 5, 0)
        assert errors == []
    finally:
        server.shutdown()
