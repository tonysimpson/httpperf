import http.server
import os
import ssl
import subprocess
import threading

import pytest
import pytest_httpbin.certs

import httpperf

CA = pytest_httpbin.certs.where()


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
    threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True).start()
    yield "https://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


async def resumed(url, tls):
    async with httpperf.Engine() as engine:
        await httpperf.Session(engine=engine, connections="session", tls=tls).get(url)
        r = await httpperf.Session(engine=engine, connections="session", tls=tls).get(url)
    return r.text == "True"


@pytest.mark.parametrize("tls, expected", [("shared", True), ("session", False)])
async def test_tls_resumption_across_sessions(tls_url, tls, expected):
    assert await resumed(tls_url, tls) == expected


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
    threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True).start()
    yield "https://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


async def who(session, url, **options):
    return (await session.get(url, **options)).text


async def test_each_session_presents_its_own_certificate(mtls_url, client_certs):
    async with httpperf.Engine() as engine:
        user1 = httpperf.Session(engine=engine, cert=client_certs["user1"])
        user2 = httpperf.Session(engine=engine, cert=client_certs["user2"])
        names = [await who(s, mtls_url) for s in (user1, user2, user1, user2)]
    assert names == ["user1", "user2", "user1", "user2"]


async def test_a_request_certificate_overrides_the_sessions(mtls_url, client_certs):
    async with httpperf.Session(cert=client_certs["user1"]) as s:
        assert await who(s, mtls_url, cert=client_certs["user2"]) == "user2"


async def test_no_certificate_is_refused(mtls_url):
    async with httpperf.Session() as s:
        with pytest.raises(httpperf.RequestError):
            await s.get(mtls_url)


async def test_a_certificate_must_be_a_pair(mtls_url):
    async with httpperf.Session() as s:
        with pytest.raises(ValueError):
            await s.get(mtls_url, cert="user1.pem")


async def verified(url, session_verify, **options):
    async with httpperf.Session(verify=session_verify) as s:
        try:
            await s.get(url, **options)
        except httpperf.RequestError:
            return False
    return True


@pytest.mark.parametrize(
    "session_verify, options, succeeds",
    [
        (False, {}, True),
        (False, {"verify": True}, False),
        (False, {"verify": CA}, True),
        (True, {}, False),
        (CA, {}, True),
        (CA, {"verify": False}, True),
    ],
)
async def test_verify(tls_url, session_verify, options, succeeds):
    assert await verified(tls_url, session_verify, **options) == succeeds


async def test_verify_must_be_a_bool_or_a_path(tls_url):
    async with httpperf.Session() as s:
        with pytest.raises(ValueError):
            await s.get(tls_url, verify=1)


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


@pytest.mark.parametrize("file, directory", [(True, False), (True, True), (False, True)])
def test_ca_lookup_uses_the_environment_first(ca_lookup, monkeypatch, file, directory):
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: str(ca_lookup / "ca.pem"))
    if file:
        monkeypatch.setenv("SSL_CERT_FILE", str(ca_lookup / "ca.pem"))
    if directory:
        monkeypatch.setenv("SSL_CERT_DIR", str(ca_lookup))
    expected = (str(ca_lookup / "ca.pem") if file else None, str(ca_lookup) if directory else None)
    assert httpperf._find_ca() == expected


def test_ca_lookup_ignores_missing_environment_paths(ca_lookup, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", "/missing/ca.pem")
    monkeypatch.setenv("SSL_CERT_DIR", "/missing/certs")
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_leaves_libcurls_own_bundle_alone(ca_lookup, monkeypatch):
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: str(ca_lookup / "ca.pem"))
    assert httpperf._find_ca() == (None, None)


def test_ca_lookup_skips_a_missing_libcurl_bundle(ca_lookup, monkeypatch):
    monkeypatch.setattr(httpperf._httpperf, "default_ca_file", lambda: "/missing/ca.pem")
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_uses_pythons_bundle_without_its_directory(ca_lookup):
    assert httpperf._find_ca() == (str(ca_lookup / "python.pem"), None)


def test_ca_lookup_falls_back_to_known_bundles(ca_lookup):
    (ca_lookup / "python.pem").unlink()
    assert httpperf._find_ca() == (str(ca_lookup / "ca.pem"), None)


def test_ca_lookup_finds_nothing_without_bundles(ca_lookup, monkeypatch):
    (ca_lookup / "python.pem").unlink()
    monkeypatch.setattr(httpperf, "_CA_FILES", ("/missing/ca.pem",))
    assert httpperf._find_ca() == (None, None)


async def test_verify_with_ssl_cert_dir(tls_url, monkeypatch, tmp_path):
    with open(CA) as f:
        (tmp_path / "ca.pem").write_text(f.read())
    subprocess.run(["openssl", "rehash", str(tmp_path)], check=True, capture_output=True)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.setenv("SSL_CERT_DIR", str(tmp_path))
    monkeypatch.setattr(httpperf, "_ca", httpperf._find_ca())
    assert await verified(tls_url, True)
