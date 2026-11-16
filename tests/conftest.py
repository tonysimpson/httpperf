import asyncio
import http.server
import inspect
import select
import socket
import sys
import threading

import pytest
from helpers import BIG

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
                if self.path in ("/big", "/truncated"):
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(BIG)))
                    self.end_headers()
                    self.wfile.write(BIG if self.path == "/big" else BIG[:1000])
                    self.close_connection = self.path == "/truncated"
                    return
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/")
                    self.send_header("Content-Length", "5")
                    self.end_headers()
                    self.wfile.write(b"moved")
                    return
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

            def do_POST(self):
                if self.path.startswith("/redirect/"):
                    self.read_body()
                    self.send_response(int(self.path.split("/")[2]))
                    self.send_header("Location", "/")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = self.read_body()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Chunked", str("Content-Length" not in self.headers))
                self.send_header("X-Expect", self.headers.get("Expect", ""))
                self.send_header("X-Content-Type", self.headers.get("Content-Type", ""))
                self.end_headers()
                self.wfile.write(body)

            do_PUT = do_POST

            def read_body(self):
                if "Content-Length" in self.headers:
                    return self.rfile.read(int(self.headers["Content-Length"]))
                body = b""
                while True:
                    size = int(self.rfile.readline(), 16)
                    body += self.rfile.read(size)
                    self.rfile.readline()
                    if size == 0:
                        return body

            def log_message(self, *args):
                pass

        self.httpd = TestHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, args=(0.01,), daemon=True).start()


class Wire:
    def __init__(self):
        self.requests = []
        self.reply = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
        self.close_after_reply = False
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.url = "http://127.0.0.1:%d" % self.sock.getsockname()[1]
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self.handle, args=(conn,), daemon=True).start()

    def handle(self, conn):
        data = b""
        with conn:
            while True:
                while b"\r\n\r\n" not in data:
                    chunk = conn.recv(65536)
                    if not chunk:
                        return
                    data += chunk
                head, _, data = data.partition(b"\r\n\r\n")
                lines = head.lower().split(b"\r\n")
                length = next((int(line[15:]) for line in lines if line.startswith(b"content-length:")), 0)
                end = b"0\r\n\r\n" if b"transfer-encoding: chunked" in lines else None
                while (end is not None and end not in data) or (end is None and len(data) < length):
                    data += conn.recv(65536)
                size = data.index(end) + len(end) if end is not None else length
                self.requests.append((head, data[:size]))
                data = data[size:]
                conn.sendall(self.reply)
                if self.close_after_reply:
                    return

    def head(self):
        return self.requests[-1][0].decode("latin-1")

    def body(self):
        body = self.requests[-1][1]
        if "transfer-encoding: chunked" not in self.head().lower():
            return body
        decoded = b""
        while True:
            size, _, body = body.partition(b"\r\n")
            if int(size, 16) == 0:
                return decoded
            decoded, body = decoded + body[: int(size, 16)], body[int(size, 16) + 2 :]

    def target(self):
        return self.head().split(" ")[1]


@pytest.fixture
def wire():
    w = Wire()
    yield w
    w.sock.close()


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
