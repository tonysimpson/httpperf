import asyncio
import errno
import gc
import os
import socket
import subprocess
import sys
import threading
import time

import pytest

import acurl


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
        el = acurl.Engine()
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
import acurl

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
    acurl.Engine()
except OSError as e:
    print("OSError", e.errno)
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["OSError", str(errno.EMFILE)]


def test_many_requests_and_sessions_do_not_block_pipes():
    code = """
import asyncio, socket
import acurl

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
url = "http://127.0.0.1:%d/" % sock.getsockname()[1]
sock.close()


async def main():
    el = acurl.Engine()
    session = acurl.Session(engine=el)
    results = await asyncio.gather(*(session.get(url) for _ in range(20000)), return_exceptions=True)
    sessions = [acurl.Session(engine=el, connections="session") for _ in range(20000)]
    el.close()
    el._thread.join()
    del sessions
    print(sum(isinstance(r, acurl.RequestError) for r in results))


asyncio.run(main())
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["20000"]


def test_session_keeps_its_engine_across_asyncio_run():
    code = """
import asyncio, http.server, threading
import acurl

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
session = acurl.Session(http_version="HTTP/1.1")

async def get():
    return (await session.get(url)).connect_time > 0

print(asyncio.run(get()), asyncio.run(get()))
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["True", "False"]


def test_engine_binds_to_one_live_loop():
    engine = acurl.Engine()
    session = acurl.Session(engine=engine)
    first = asyncio.new_event_loop()
    second = asyncio.new_event_loop()
    try:
        with pytest.raises(acurl.RequestError):
            first.run_until_complete(session.get("http://127.0.0.1:1/"))
        with pytest.raises(RuntimeError, match="another event loop"):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
        first.close()
        with pytest.raises(acurl.RequestError):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
        engine.close()
        with pytest.raises(RuntimeError, match="engine is closed"):
            second.run_until_complete(session.get("http://127.0.0.1:1/"))
    finally:
        first.close()
        second.close()


def test_arguments_are_keyword_only():
    with pytest.raises(TypeError):
        acurl.Session("shared")
    with pytest.raises(TypeError):
        acurl.Engine(10)


def test_unclosed_engine_stops_when_dropped():
    loop = asyncio.new_event_loop()
    try:
        engine = acurl.Engine()
        thread = engine._thread
        with pytest.raises(acurl.RequestError):
            loop.run_until_complete(acurl.Session(engine=engine).get("http://127.0.0.1:1/"))
    finally:
        loop.close()
    del engine
    gc.collect()
    thread.join(timeout=2)
    assert not thread.is_alive()


def engine_threads():
    return sum(t.name == "acurl-engine" for t in threading.enumerate())


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
        session = acurl.Session()
        with pytest.raises(acurl.RequestError):
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
    session = acurl.Session()
    session.add_cookie_list([acurl.Cookie(False, "example.com", False, "/", False, 0, "a", "1")])
    assert session.cookies() == {"a": "1"}
    session.close()


def test_close_with_requests_in_flight_warns(silent_url):
    async def main():
        engine = acurl.Engine()
        task = asyncio.ensure_future(acurl.Session(engine=engine).get(silent_url))
        await asyncio.sleep(0.1)
        with pytest.warns(ResourceWarning, match="1 requests in flight"):
            engine.close()
        with pytest.raises(acurl.RequestError, match="engine closed"):
            await task

    asyncio.run(main())


def test_dropped_loop_with_requests_in_flight_frees_them(silent_url):
    gc.collect()
    before = wait_for_engine_threads(engine_threads())
    loop = asyncio.new_event_loop()
    task = loop.create_task(acurl.Session().get(silent_url))
    loop.run_until_complete(asyncio.sleep(0.1))
    task.cancel()
    loop.run_until_complete(asyncio.sleep(0))
    with pytest.warns(ResourceWarning, match="1 requests in flight"):
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
        async with acurl.Session(http_version="HTTP/1.1") as session:
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
