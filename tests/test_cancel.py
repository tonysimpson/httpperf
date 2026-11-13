import asyncio
import http.server
import random
import select
import socket
import threading
import time

import pytest

import httpperf


class Server:
    def __init__(self):
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.started = []
        self.hung_up = 0
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                with server.lock:
                    server.started.append(self.path)
                if self.path.startswith("/slow"):
                    while not server.release.wait(0.02):
                        readable, _, _ = select.select([self.connection], [], [], 0)
                        if readable and self.connection.recv(1, socket.MSG_PEEK) == b"":
                            with server.lock:
                                server.hung_up += 1
                            return
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, *args):
                pass

        http_server = type("HTTPServer", (http.server.ThreadingHTTPServer,), {"request_queue_size": 256})
        self.httpd = http_server(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def wait_for(self, condition, timeout=5):
        deadline = time.monotonic() + timeout
        while not condition():
            if time.monotonic() > deadline:
                return False
            time.sleep(0.01)
        return True


@pytest.fixture
def server():
    s = Server()
    yield s
    s.release.set()
    s.httpd.shutdown()


def run(main):
    errors = []

    async def wrapper():
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: errors.append(context))
        return await main()

    result = asyncio.run(wrapper())
    assert errors == []
    return result


def test_cancel_stops_the_transfer(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            task = asyncio.create_task(session.get(server.url + "/slow"))
            await asyncio.sleep(0.2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return await asyncio.get_running_loop().run_in_executor(None, server.wait_for, lambda: server.hung_up == 1)

    assert run(main)


def test_cancel_frees_the_connection_slot(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=1) as session:
            slow = asyncio.create_task(session.get(server.url + "/slow"))
            await asyncio.sleep(0.2)
            slow.cancel()
            r = await asyncio.wait_for(session.get(server.url + "/fast"), 2)
            return r.status_code

    assert run(main) == 200


def test_cancel_a_queued_request(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1", connections="session", max_host_connections=1) as session:
            slow = asyncio.create_task(session.get(server.url + "/slow"))
            await asyncio.sleep(0.2)
            queued = asyncio.create_task(session.get(server.url + "/queued"))
            after = asyncio.create_task(session.get(server.url + "/after"))
            await asyncio.sleep(0.2)
            queued.cancel()
            await asyncio.sleep(0.1)
            server.release.set()
            results = [(await slow).status_code, (await after).status_code]
            return results, queued.cancelled()

    assert run(main) == ([200, 200], True)
    assert server.started == ["/slow", "/after"]


def test_cancel_before_the_engine_starts_it(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            task = asyncio.create_task(session.get(server.url + "/slow"))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0.2)
            r = await session.get(server.url + "/fast")
            return r.status_code

    assert run(main) == 200
    assert "/slow" not in server.started


def test_wait_for_stops_the_transfer(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(session.get(server.url + "/slow"), 0.2)
            return await asyncio.get_running_loop().run_in_executor(None, server.wait_for, lambda: server.hung_up == 1)

    assert run(main)


def test_cancel_many(server):
    async def main():
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            tasks = [asyncio.create_task(session.get(server.url + "/slow")) for _ in range(100)]
            await asyncio.sleep(0.5)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            r = await session.get(server.url + "/fast")
            return r.status_code, all(t.cancelled() for t in tasks)

    assert run(main) == (200, True)


def test_cancel_races_completion(server):
    rng = random.Random(1)

    async def main():
        ok = 0
        async with httpperf.Session(http_version="HTTP/1.1") as session:
            for _ in range(30):
                tasks = [asyncio.create_task(session.get(server.url + "/fast")) for _ in range(50)]
                cancelled = set(rng.sample(range(50), 25))
                await asyncio.sleep(rng.random() * 0.005)
                for i in cancelled:
                    tasks[i].cancel()
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
                for i, result in enumerate(results):
                    if i not in cancelled:
                        assert result.status_code == 200
                        ok += 1
        return ok

    assert run(main) == 30 * 25


def test_cancel_after_the_engine_closed(server):
    async def main():
        engine = httpperf.Engine()
        async with httpperf.Session(engine=engine, http_version="HTTP/1.1") as session:
            task = asyncio.create_task(session.get(server.url + "/slow"))
            await asyncio.sleep(0.2)
            with pytest.warns(ResourceWarning):
                engine.close()
            task.cancel()
            with pytest.raises((asyncio.CancelledError, httpperf.RequestError)):
                await task

    run(main)
