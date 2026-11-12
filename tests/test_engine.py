import asyncio
import errno
import gc
import os
import subprocess
import sys

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


def test_default_engine_survives_asyncio_run():
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
