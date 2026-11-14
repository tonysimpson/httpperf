import asyncio
import http.server
import inspect
import select
import socket
import sys
import threading

import pytest

import httpperf


class TestHTTPServer(http.server.ThreadingHTTPServer):
    request_queue_size = 1024
    daemon_threads = True

    def handle_error(self, request, client_address):
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)


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

        self.httpd = TestHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, args=(0.01,), daemon=True).start()


@pytest.fixture
def make_server():
    servers = []

    def make():
        servers.append(Server())
        return servers[-1]

    yield make
    for s in servers:
        s.release.set()
        s.httpd.shutdown()
        s.httpd.server_close()


@pytest.fixture
def server(make_server):
    return make_server()


@pytest.fixture
def s():
    session = httpperf.Session()
    yield session
    session.close()


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    if inspect.iscoroutinefunction(pyfuncitem.obj):
        args = {name: pyfuncitem.funcargs[name] for name in pyfuncitem._fixtureinfo.argnames}
        assert asyncio.run(without_loop_errors(pyfuncitem.obj(**args))) == []
        return True


async def without_loop_errors(test):
    errors = []
    asyncio.get_running_loop().set_exception_handler(lambda loop, context: errors.append(context))
    await test
    return errors
