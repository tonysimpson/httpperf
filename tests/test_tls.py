import asyncio
import http.server
import os
import ssl
import subprocess
import threading

import pytest
import pytest_httpbin.certs

import httpperf


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
    server.server_close()


@pytest.mark.parametrize("tls, resumed", [("shared", "True"), ("session", "False")])
def test_tls_resumption_across_sessions(tls_url, tls, resumed):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    first = httpperf.Session(engine=el, connections="session", tls=tls)
    assert loop.run_until_complete(first.get(tls_url)).text == "False"
    second = httpperf.Session(engine=el, connections="session", tls=tls)
    assert loop.run_until_complete(second.get(tls_url)).text == resumed
    el.close()
    loop.close()


class ClientCertHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = dict(item[0] for item in self.connection.getpeercert()["subject"])["commonName"].encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def client_certs(tmp_path_factory):
    path = tmp_path_factory.mktemp("certs")
    certs = {}
    for name in ("user1", "user2"):
        cert, key = str(path / (name + ".pem")), str(path / (name + ".key"))
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"]
            + ["-subj", "/CN=" + name, "-keyout", key, "-out", cert],
            check=True,
            capture_output=True,
        )
        certs[name] = (cert, key)
    return certs


@pytest.fixture(scope="module")
def mtls_url(client_certs):
    certs = os.path.dirname(pytest_httpbin.certs.where())
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(os.path.join(certs, "server.pem"), os.path.join(certs, "server.key"))
    context.verify_mode = ssl.CERT_REQUIRED
    for cert, _ in client_certs.values():
        context.load_verify_locations(cert)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ClientCertHandler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield "https://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


def test_client_certs(mtls_url, client_certs):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    user1 = httpperf.Session(engine=el, cert=client_certs["user1"])
    user2 = httpperf.Session(engine=el, cert=client_certs["user2"])

    def who(session, **kwargs):
        return loop.run_until_complete(session.get(mtls_url, **kwargs)).text

    assert [who(user1), who(user2), who(user1), who(user2)] == ["user1", "user2", "user1", "user2"]
    assert who(user1, cert=client_certs["user2"]) == "user2"
    with pytest.raises(httpperf.RequestError):
        who(httpperf.Session(engine=el))
    with pytest.raises(ValueError):
        who(user1, cert="user1.pem")
    el.close()
    loop.close()


def test_verify(tls_url):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    ca = pytest_httpbin.certs.where()

    def get(session, **kwargs):
        return loop.run_until_complete(session.get(tls_url, **kwargs)).status_code

    unverified = httpperf.Session(engine=el)
    assert get(unverified) == 200
    with pytest.raises(httpperf.RequestError):
        get(unverified, verify=True)
    assert get(unverified, verify=ca) == 200
    with pytest.raises(httpperf.RequestError):
        get(httpperf.Session(engine=el, verify=True))
    verified = httpperf.Session(engine=el, verify=ca)
    assert get(verified) == 200
    assert get(verified, verify=False) == 200
    with pytest.raises(ValueError):
        get(verified, verify=1)
    el.close()
    loop.close()


@pytest.fixture
def ca_lookup(monkeypatch, tmp_path):
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        monkeypatch.delenv(name, raising=False)
    bundle = tmp_path / "ca.pem"
    bundle.write_text("")
    python_bundle = tmp_path / "python.pem"
    python_bundle.write_text("")
    paths = ssl.DefaultVerifyPaths(
        str(python_bundle), str(tmp_path), "SSL_CERT_FILE", str(python_bundle), "SSL_CERT_DIR", str(tmp_path)
    )
    monkeypatch.setattr(ssl, "get_default_verify_paths", lambda: paths)
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: None)
    monkeypatch.setattr(httpperf, "_CA_FILES", ("/missing/ca.pem", str(bundle)))
    return tmp_path


def test_ca_lookup_uses_the_environment_first(ca_lookup, monkeypatch):
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: str(ca_lookup / "ca.pem"))
    monkeypatch.setenv("SSL_CERT_FILE", str(ca_lookup / "ca.pem"))
    assert httpperf._find_ca() == (str(ca_lookup / "ca.pem"), None)
    monkeypatch.setenv("SSL_CERT_DIR", str(ca_lookup))
    assert httpperf._find_ca() == (str(ca_lookup / "ca.pem"), str(ca_lookup))
    monkeypatch.delenv("SSL_CERT_FILE")
    assert httpperf._find_ca() == (None, str(ca_lookup))


def test_ca_lookup_ignores_missing_environment_paths(ca_lookup, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", "/missing/ca.pem")
    monkeypatch.setenv("SSL_CERT_DIR", "/missing/certs")
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_leaves_libcurls_own_bundle_alone(ca_lookup, monkeypatch):
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: str(ca_lookup / "ca.pem"))
    assert httpperf._find_ca() == (None, None)
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: "/missing/ca.pem")
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_uses_pythons_bundle_without_its_directory(ca_lookup):
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_falls_back_to_known_bundles(ca_lookup, monkeypatch):
    (ca_lookup / "python.pem").unlink()
    assert httpperf._find_ca() == (str(ca_lookup / "ca.pem"), None)
    monkeypatch.setattr(httpperf, "_CA_FILES", ("/missing/ca.pem",))
    assert httpperf._find_ca() == (None, None)


def test_verify_with_ssl_cert_dir(tls_url, monkeypatch, tmp_path):
    with open(pytest_httpbin.certs.where()) as f:
        (tmp_path / "ca.pem").write_text(f.read())
    subprocess.run(["openssl", "rehash", str(tmp_path)], check=True, capture_output=True)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.setenv("SSL_CERT_DIR", str(tmp_path))
    monkeypatch.setattr(httpperf, "_ca", httpperf._find_ca())
    assert httpperf._ca == (None, str(tmp_path))
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    response = loop.run_until_complete(httpperf.Session(engine=el, verify=True).get(tls_url))
    assert response.status_code == 200
    el.close()
    loop.close()
