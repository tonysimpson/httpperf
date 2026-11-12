import asyncio
import http.server
import socket
import threading
import time

import pytest

import httpperf


class CountingHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    open_connections = 0

    def setup(self):
        super().setup()
        CountingHandler.open_connections += 1

    def finish(self):
        super().finish()
        CountingHandler.open_connections -= 1

    def do_GET(self):
        if self.path == "/slow":
            time.sleep(0.05)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.send_header("Set-Cookie", "name=value")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CountingHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield "http://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


def wait_for_connections(n):
    deadline = time.monotonic() + 2
    while CountingHandler.open_connections != n and time.monotonic() < deadline:
        time.sleep(0.01)
    return CountingHandler.open_connections


def test_close_closes_connections(url):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    s = httpperf.Session(engine=el, connections="session")
    r = loop.run_until_complete(s.get(url))
    assert r.cookies == {"name": "value"}
    assert wait_for_connections(1) == 1
    s.close()
    assert wait_for_connections(0) == 0
    assert r.status_code == 200
    with pytest.raises(RuntimeError):
        loop.run_until_complete(s.get(url))
    el.close()
    loop.close()


@pytest.mark.parametrize("connections, new", [("session", [True, False]), ("none", [True, True])])
def test_connection_reuse(url, connections, new):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    s = httpperf.Session(engine=el, connections=connections)
    assert [loop.run_until_complete(s.get(url)).connect_time > 0 for _ in new] == new
    s.close()
    el.close()
    loop.close()


@pytest.mark.parametrize("connections", ["shared", "none"])
@pytest.mark.parametrize("limits, connects", [({"max_host_connections": 2}, 2), ({"max_total_connections": 3}, 3)])
def test_event_loop_connection_limits(url, limits, connects, connections):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine(**limits)
    s = httpperf.Session(engine=el, connections=connections, http_version="HTTP/1.1")

    async def main():
        return await asyncio.gather(*(s.get(url + "slow") for _ in range(6)))

    responses = loop.run_until_complete(main())
    if connections == "shared":
        assert sum(r.connect_time > 0 for r in responses) == connects
    assert sum(r.queue_time > 0 for r in responses) == 6 - connects
    el.close()
    loop.close()


def test_max_idle_connections(url):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine(max_idle_connections=1)
    s = httpperf.Session(engine=el, http_version="HTTP/1.1")

    async def batch():
        return await asyncio.gather(*(s.get(url + "slow") for _ in range(3)))

    assert sum(r.connect_time > 0 for r in loop.run_until_complete(batch())) == 3
    assert sum(r.connect_time > 0 for r in loop.run_until_complete(batch())) == 2
    el.close()
    loop.close()


@pytest.mark.parametrize("options, wait", [({"idle_timeout": 0}, 1.1), ({"max_connection_age": 1}, 2.1)])
def test_connection_expiry(url, options, wait):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    s = httpperf.Session(engine=el, connections="session", **options)
    first = loop.run_until_complete(s.get(url))
    time.sleep(wait)
    second = loop.run_until_complete(s.get(url))
    assert first.connect_time > 0
    assert second.connect_time > 0
    s.close()
    el.close()
    loop.close()


@pytest.mark.parametrize(
    "limits, hosts, queued",
    [({"max_host_connections": 2}, ["127.0.0.1"], 4), ({"max_connections": 3}, ["127.0.0.1", "localhost"], 3)],
)
def test_session_connection_limits(url, limits, hosts, queued):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    s = httpperf.Session(engine=el, connections="session", http_version="HTTP/1.1", **limits)
    urls = [url.replace("127.0.0.1", host) + "slow" for host in hosts for _ in range(6 // len(hosts))]

    async def main():
        return await asyncio.gather(*(s.get(u) for u in urls))

    responses = loop.run_until_complete(main())
    assert sum(r.queue_time > 0 for r in responses) == queued
    assert all(r.client_lag < r.queue_time + 0.05 for r in responses)
    s.close()
    el.close()
    loop.close()


def test_context_managers(url):
    loop = asyncio.new_event_loop()

    async def main():
        async with httpperf.Engine() as el:
            async with httpperf.Session(engine=el, connections="session") as s:
                assert (await s.get(url)).status_code == 200
            with pytest.raises(RuntimeError):
                await s.get(url)
            assert wait_for_connections(0) == 0
            with httpperf.Session(engine=el) as s2:
                assert (await s2.get(url)).status_code == 200
        return el

    el = loop.run_until_complete(main())
    el._thread.join(2)
    assert not el._thread.is_alive()
    assert wait_for_connections(0) == 0
    with httpperf.Engine() as el:
        pass
    el._thread.join(2)
    assert not el._thread.is_alive()
    loop.close()


def test_timeouts(url):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)
    full = socket.socket()
    full.bind(("127.0.0.1", 0))
    full.listen(0)
    filler = socket.create_connection(full.getsockname())

    def get(session, target, **kwargs):
        start = time.monotonic()
        try:
            return loop.run_until_complete(session.get(target, **kwargs)).status_code
        except httpperf.RequestError as e:
            return type(e), e.code, round(time.monotonic() - start, 1)

    unlimited = httpperf.Session(engine=el)
    assert get(unlimited, "http://127.0.0.1:%d/" % silent.getsockname()[1], timeout=0.2) == (httpperf.Timeout, 28, 0.2)
    assert get(unlimited, "http://127.0.0.1:%d/" % full.getsockname()[1], connect_timeout=0.2) == (
        httpperf.Timeout,
        28,
        0.2,
    )
    impatient = httpperf.Session(engine=el, timeout=0.01)
    assert get(impatient, url + "slow")[0] is httpperf.Timeout
    assert get(impatient, url + "slow", timeout=0) == 200
    assert get(impatient, url + "slow", timeout=1) == 200
    for bad in (-1, float("nan")):
        with pytest.raises(ValueError):
            get(unlimited, url, timeout=bad)
    with pytest.raises(TypeError):
        get(unlimited, url, connect_timeout="1")
    el.close()
    loop.close()
    for s in (silent, full, filler):
        s.close()


def test_error_messages_are_specific():
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with pytest.raises(httpperf.RequestError) as e:
        loop.run_until_complete(httpperf.Session(engine=el).get("http://127.0.0.1:%d/" % port))
    assert type(e.value) is httpperf.RequestError
    assert e.value.code == 7
    assert "port %d" % port in str(e.value)
    el.close()
    loop.close()


def test_server_time(url):
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()
    r = loop.run_until_complete(httpperf.Session(engine=el).get(url + "slow"))
    if r.posttransfer_time is None:
        pytest.skip("libcurl older than 8.10")
    assert r.pretransfer_time <= r.posttransfer_time < r.starttransfer_time <= r.total_time
    assert 0.05 <= r.starttransfer_time - r.posttransfer_time < 0.06
    assert 0 <= r.client_lag < 0.01
    el.close()
    loop.close()


def test_proxy_environment_is_read_per_session(url, monkeypatch):
    for name in ("http_proxy", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
        monkeypatch.delenv(name, raising=False)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    proxy_port = sock.getsockname()[1]
    sock.close()
    loop = asyncio.new_event_loop()
    el = httpperf.Engine()

    def get(session):
        try:
            return loop.run_until_complete(session.get(url)).status_code
        except httpperf.RequestError as e:
            return str(e)

    direct = httpperf.Session(engine=el)
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:%d" % proxy_port)
    proxied = httpperf.Session(engine=el)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    bypassed = httpperf.Session(engine=el)
    monkeypatch.delenv("http_proxy")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:%d" % proxy_port)
    monkeypatch.delenv("NO_PROXY")
    fallback = httpperf.Session(engine=el)
    monkeypatch.delenv("ALL_PROXY")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:%d" % proxy_port)
    uppercase = httpperf.Session(engine=el)

    assert get(direct) == 200
    assert "port %d" % proxy_port in get(proxied)
    assert get(bypassed) == 200
    assert "port %d" % proxy_port in get(fallback)
    assert get(uppercase) == 200
    el.close()
    loop.close()
