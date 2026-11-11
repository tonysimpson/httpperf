import asyncio
import http.server
import os
import ssl
import threading

import pytest
import pytest_httpbin.certs

import acurl


class ReusedHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = str(self.connection.session_reused).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def tls_url():
    certs = os.path.dirname(pytest_httpbin.certs.where())
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(os.path.join(certs, "server.pem"), os.path.join(certs, "server.key"))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ReusedHandler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield "https://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()


@pytest.mark.parametrize("tls, resumed", [("shared", "True"), ("session", "False")])
def test_tls_resumption_across_sessions(tls_url, tls, resumed):
    loop = asyncio.new_event_loop()
    el = acurl.EventLoop(loop=loop)
    first = el.session(connections="session", tls=tls)
    assert loop.run_until_complete(first.get(tls_url)).text == "False"
    second = el.session(connections="session", tls=tls)
    assert loop.run_until_complete(second.get(tls_url)).text == resumed
    el.stop()
    loop.close()
