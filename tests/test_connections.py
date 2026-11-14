import asyncio
import http.server
import socket
import threading
import time

import pytest
from helpers import until

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
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CountingHandler)
    threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True).start()
    yield "http://127.0.0.1:%d/" % server.server_address[1]
    server.shutdown()
    server.server_close()


def unused_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def silent_url():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(8)
    yield "http://127.0.0.1:%d/" % sock.getsockname()[1]
    sock.close()


@pytest.fixture
def full_url():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(0)
    filler = socket.create_connection(sock.getsockname())
    yield "http://127.0.0.1:%d/" % sock.getsockname()[1]
    filler.close()
    sock.close()


async def get_together(session, url, n):
    return await asyncio.gather(*(session.get(url) for _ in range(n)))


async def seconds_to_time_out(request):
    start = time.monotonic()
    with pytest.raises(httpperf.Timeout):
        await request
    return time.monotonic() - start


def connections_to_server():
    return CountingHandler.open_connections


async def test_closing_a_session_closes_its_connections(url):
    s = httpperf.Session(connections="session")
    await s.get(url)
    assert await until(lambda: connections_to_server() == 1)
    s.close()
    assert await until(lambda: connections_to_server() == 0)


async def test_a_closed_session_refuses_requests(url):
    s = httpperf.Session()
    s.close()
    with pytest.raises(RuntimeError):
        await s.get(url)


@pytest.mark.parametrize("connections, new_connections", [("session", [True, False]), ("none", [True, True])])
async def test_connection_reuse(url, connections, new_connections):
    async with httpperf.Session(connections=connections) as s:
        assert [(await s.get(url)).connect_time > 0 for _ in new_connections] == new_connections


@pytest.mark.parametrize("connections", ["shared", "none"])
@pytest.mark.parametrize("limits, allowed", [({"max_host_connections": 2}, 2), ({"max_total_connections": 3}, 3)])
async def test_requests_over_an_engine_limit_queue(url, limits, allowed, connections):
    async with httpperf.Engine(**limits) as engine:
        responses = await get_together(httpperf.Session(engine=engine, connections=connections), url + "slow", 6)
    assert sum(r.queue_time > 0 for r in responses) == 6 - allowed


@pytest.mark.parametrize("limits, allowed", [({"max_host_connections": 2}, 2), ({"max_total_connections": 3}, 3)])
async def test_engine_limits_cap_new_connections(url, limits, allowed):
    async with httpperf.Engine(**limits) as engine:
        responses = await get_together(httpperf.Session(engine=engine), url + "slow", 6)
    assert sum(r.connect_time > 0 for r in responses) == allowed


async def test_max_idle_connections(url):
    async with httpperf.Engine(max_idle_connections=1) as engine:
        s = httpperf.Session(engine=engine)
        await get_together(s, url + "slow", 3)
        responses = await get_together(s, url + "slow", 3)
    assert sum(r.connect_time > 0 for r in responses) == 3 - 1


@pytest.mark.parametrize("options, wait", [({"idle_timeout": 0}, 1.1), ({"max_connection_age": 1}, 2.1)])
async def test_expired_connections_are_not_reused(url, options, wait):
    async with httpperf.Session(connections="session", **options) as s:
        await s.get(url)
        await asyncio.sleep(wait)
        assert (await s.get(url)).connect_time > 0


@pytest.mark.parametrize(
    "limits, hosts, queued",
    [({"max_host_connections": 2}, ["127.0.0.1"], 6 - 2), ({"max_connections": 3}, ["127.0.0.1", "localhost"], 6 - 3)],
)
async def test_requests_over_a_session_limit_queue(url, limits, hosts, queued):
    urls = [url.replace("127.0.0.1", host) + "slow" for host in hosts for _ in range(6 // len(hosts))]
    async with httpperf.Session(connections="session", **limits) as s:
        responses = await asyncio.gather(*(s.get(u) for u in urls))
    assert sum(r.queue_time > 0 for r in responses) == queued


async def test_queue_time_is_not_client_lag(url):
    async with httpperf.Session(connections="session", max_host_connections=2) as s:
        responses = await get_together(s, url + "slow", 6)
    assert max(r.client_lag - r.queue_time for r in responses) < 0.05


async def test_async_with_closes_the_session(url):
    async with httpperf.Session() as s:
        await s.get(url)
    with pytest.raises(RuntimeError):
        await s.get(url)


async def test_with_closes_the_session(url):
    with httpperf.Session() as s:
        await s.get(url)
    with pytest.raises(RuntimeError):
        await s.get(url)


async def test_async_with_stops_the_engine(url):
    async with httpperf.Engine() as engine:
        await httpperf.Session(engine=engine).get(url)
    engine._thread.join(2)
    assert not engine._thread.is_alive()


def test_with_stops_the_engine():
    with httpperf.Engine() as engine:
        pass
    engine._thread.join(2)
    assert not engine._thread.is_alive()


async def test_timeout(silent_url):
    async with httpperf.Session() as s:
        assert await seconds_to_time_out(s.get(silent_url, timeout=0.2)) == pytest.approx(0.2, abs=0.05)


async def test_connect_timeout(full_url):
    async with httpperf.Session() as s:
        assert await seconds_to_time_out(s.get(full_url, connect_timeout=0.2)) == pytest.approx(0.2, abs=0.05)


async def test_a_session_timeout_applies_to_its_requests(url):
    async with httpperf.Session(timeout=0.01) as s:
        await seconds_to_time_out(s.get(url + "slow"))


@pytest.mark.parametrize("timeout", [0, 1])
async def test_a_request_timeout_overrides_the_sessions(url, timeout):
    async with httpperf.Session(timeout=0.01) as s:
        assert (await s.get(url + "slow", timeout=timeout)).status_code == 200


@pytest.mark.parametrize("timeout", [-1, float("nan")])
async def test_invalid_timeouts_are_refused(url, timeout):
    async with httpperf.Session() as s:
        with pytest.raises(ValueError):
            await s.get(url, timeout=timeout)


async def test_a_timeout_must_be_a_number(url):
    async with httpperf.Session() as s:
        with pytest.raises(TypeError):
            await s.get(url, connect_timeout="1")


async def test_a_refused_connection_names_the_port():
    port = unused_port()
    async with httpperf.Session() as s:
        with pytest.raises(httpperf.RequestError, match="port %d" % port):
            await s.get("http://127.0.0.1:%d/" % port)


async def test_a_refused_connection_has_curls_error_code():
    async with httpperf.Session() as s:
        with pytest.raises(httpperf.RequestError) as e:
            await s.get("http://127.0.0.1:%d/" % unused_port())
    assert e.value.code == 7


async def test_timings_are_in_order(url):
    async with httpperf.Session() as s:
        r = await s.get(url + "slow")
    if r.posttransfer_time is None:
        pytest.skip("libcurl older than 8.10")
    assert r.pretransfer_time <= r.posttransfer_time < r.starttransfer_time <= r.total_time


async def test_server_time_is_the_wait_after_sending(url):
    async with httpperf.Session() as s:
        r = await s.get(url + "slow")
    if r.posttransfer_time is None:
        pytest.skip("libcurl older than 8.10")
    assert r.starttransfer_time - r.posttransfer_time == pytest.approx(0.055, abs=0.005)


async def test_client_lag_is_small_when_idle(url):
    async with httpperf.Session() as s:
        r = await s.get(url)
    assert 0 <= r.client_lag < 0.01


@pytest.fixture
def proxy_environment(monkeypatch):
    for name in (
        "http_proxy",
        "HTTP_PROXY",
        "https_proxy",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
        "no_proxy",
        "NO_PROXY",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


DEAD_PROXY = "http://127.0.0.1:%d" % unused_port()


async def went_through_dead_proxy(url):
    async with httpperf.Session() as s:
        try:
            await s.get(url)
        except httpperf.RequestError as e:
            return DEAD_PROXY.rsplit(":", 1)[1] in str(e)
    return False


@pytest.mark.parametrize(
    "environment, proxied",
    [
        ({}, False),
        ({"http_proxy": DEAD_PROXY}, True),
        ({"http_proxy": DEAD_PROXY, "NO_PROXY": "127.0.0.1"}, False),
        ({"ALL_PROXY": DEAD_PROXY}, True),
        ({"HTTP_PROXY": DEAD_PROXY}, False),
    ],
)
async def test_proxy_environment(url, proxy_environment, environment, proxied):
    for name, value in environment.items():
        proxy_environment.setenv(name, value)
    assert await went_through_dead_proxy(url) == proxied


async def test_a_session_keeps_the_proxy_environment_it_was_made_with(url, proxy_environment):
    async with httpperf.Session() as s:
        proxy_environment.setenv("http_proxy", DEAD_PROXY)
        assert (await s.get(url)).status_code == 200


async def new_connections_while_responses_arrive(engine, url):
    busy = httpperf.Session(engine=engine)
    done = False

    async def traffic():
        while not done:
            await busy.get(url)

    workers = [asyncio.create_task(traffic()) for _ in range(20)]
    sessions = [httpperf.Session(engine=engine, connections="session") for _ in range(350)]
    responses = await asyncio.gather(*(s.get(url) for s in sessions))
    for s in sessions:
        s.close()
    done = True
    await asyncio.gather(*workers)
    return [r.status_code for r in responses]


async def test_many_new_connections_while_responses_arrive(server):
    for _ in range(4):
        async with httpperf.Engine() as engine:
            assert await new_connections_while_responses_arrive(engine, server.url) == [200] * 350
