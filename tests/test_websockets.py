import asyncio
import base64
import gc
import hashlib
import json
import socket
import ssl
import subprocess
import sys
import threading
import weakref

import pytest
import pytest_httpbin.certs

import httpperf
from httpperf import websockets

pytestmark = pytest.mark.skipif(not httpperf._httpperf.websockets, reason="libcurl has no WebSocket support")

server_lib = pytest.importorskip("websockets.asyncio.server")


async def handler(ws):
    path = ws.request.path
    if path == "/headers":
        await ws.send(json.dumps(dict(ws.request.headers.raw_items())))
    elif path.startswith("/close/"):
        await ws.close(int(path.split("/")[2]), "bye")
    elif path == "/drop":
        await ws.recv()
        ws.transport.abort()
    elif path.startswith("/send/"):
        for _ in range(int(path.split("/")[2])):
            await ws.send("m")
        await ws.close()
    elif path.startswith("/flood/"):
        for i in range(int(path.split("/")[2])):
            await ws.send(b"%d " % i + b"x" * 65536)
        await ws.close()
    elif path == "/ping":
        await (await ws.ping())
        await ws.send("ponged")
        await ws.wait_closed()
    else:
        async for message in ws:
            await ws.send(message)


def first_offered(connection, subprotocols):
    return subprotocols[0] if subprotocols else None


def run_server(ssl_context=None):
    ready = threading.Event()
    state = {}

    def run():
        async def main():
            async with server_lib.serve(
                handler, "127.0.0.1", 0, ssl=ssl_context, select_subprotocol=first_offered, max_size=None
            ) as server:
                state["port"] = server.sockets[0].getsockname()[1]
                state["stop"] = asyncio.get_running_loop().create_future()
                state["loop"] = asyncio.get_running_loop()
                ready.set()
                await state["stop"]

        asyncio.run(main())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    ready.wait()
    return state, thread


@pytest.fixture(scope="module")
def ws_url():
    state, thread = run_server()
    yield "ws://127.0.0.1:%d" % state["port"]
    state["loop"].call_soon_threadsafe(state["stop"].set_result, None)
    thread.join()


@pytest.fixture(scope="module")
def wss_url():
    certs = pytest_httpbin.certs.where().rsplit("/", 1)[0]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certs + "/server.pem", certs + "/server.key")
    state, thread = run_server(context)
    yield "wss://127.0.0.1:%d" % state["port"]
    state["loop"].call_soon_threadsafe(state["stop"].set_result, None)
    thread.join()


def frame(opcode, payload=b""):
    return bytes([0x80 | opcode, len(payload)]) + payload


def silent_server(after_handshake=b""):
    sock = socket.create_server(("127.0.0.1", 0))

    def run():
        conn, _ = sock.accept()
        request = b""
        while b"\r\n\r\n" not in request:
            request += conn.recv(65536)
        key = next(
            line.split(b":", 1)[1].strip()
            for line in request.split(b"\r\n")
            if line.lower().startswith(b"sec-websocket-key")
        )
        accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
        conn.sendall(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n" + after_handshake
        )
        try:
            while conn.recv(65536):
                pass
        except OSError:
            pass

    threading.Thread(target=run, daemon=True).start()
    return sock, "ws://127.0.0.1:%d/" % sock.getsockname()[1]


@pytest.mark.parametrize("message", ["hello", "", "é" * 1000, b"\x00\xff", b""])
async def test_a_message_is_echoed(ws_url, message):
    async with websockets.connect(ws_url) as ws:
        await ws.send(message)
        assert await ws.recv() == message


async def test_a_large_message_is_echoed(ws_url):
    async with websockets.connect(ws_url, max_size=None) as ws:
        await ws.send(b"x" * 3_000_000)
        assert await ws.recv() == b"x" * 3_000_000


async def test_fragments_arrive_as_one_message(ws_url):
    async with websockets.connect(ws_url) as ws:
        await ws.send(["frag", "ment"])
        assert await ws.recv() == "fragment"


async def test_connect_can_be_awaited(ws_url):
    ws = await websockets.connect(ws_url)
    await ws.send("x")
    assert await ws.recv() == "x"
    await ws.close()


async def test_messages_keep_their_order(ws_url):
    async with websockets.connect(ws_url) as ws:
        for i in range(1000):
            await ws.send(str(i))
        assert [await ws.recv() for _ in range(1000)] == [str(i) for i in range(1000)]


async def test_iteration_ends_when_the_server_closes(ws_url):
    async with websockets.connect(ws_url + "/send/3") as ws:
        assert [m async for m in ws] == ["m", "m", "m"]


@pytest.mark.parametrize("code, error", [(1000, False), (1001, False), (1011, True)])
async def test_a_server_close_raises_ok_or_error(ws_url, code, error):
    ws = await websockets.connect(ws_url + "/close/%d" % code)
    with pytest.raises(websockets.ConnectionClosedError if error else websockets.ConnectionClosedOK) as e:
        await ws.recv()
    assert (e.value.rcvd.code, e.value.rcvd.reason, ws.close_code) == (code, "bye", code)


async def test_a_dropped_connection_is_an_error(ws_url):
    ws = await websockets.connect(ws_url + "/drop")
    await ws.send("x")
    with pytest.raises(websockets.ConnectionClosedError):
        await ws.recv()
    assert ws.close_code == 1006


async def test_closing_sets_the_code(ws_url):
    ws = await websockets.connect(ws_url)
    await ws.close(4000, "done")
    assert (ws.close_code, ws.close_reason) == (4000, "done")


async def test_sending_after_closing_raises(ws_url):
    ws = await websockets.connect(ws_url)
    await ws.close()
    with pytest.raises(websockets.ConnectionClosedOK):
        await ws.send("x")


async def test_a_ping_gives_the_latency(ws_url):
    async with websockets.connect(ws_url) as ws:
        latency = await (await ws.ping())
    assert latency == ws.latency > 0


async def test_a_missed_keepalive_closes_the_connection():
    sock, url = silent_server()
    ws = await websockets.connect(url, ping_interval=0.1, ping_timeout=0.1)
    with pytest.raises(websockets.ConnectionClosedError):
        await asyncio.wait_for(ws.recv(), 5)
    assert ws._sent.code == 1011
    sock.close()


async def test_a_message_over_max_size_closes_with_1009(ws_url):
    ws = await websockets.connect(ws_url, max_size=10)
    await ws.send("x" * 11)
    with pytest.raises(websockets.ConnectionClosedError):
        await ws.recv()
    assert ws._sent.code == 1009


async def test_headers_and_subprotocols_are_sent(ws_url):
    async with websockets.connect(
        ws_url + "/headers", additional_headers={"Authorization": "Bearer x"}, subprotocols=["chat"]
    ) as ws:
        headers = json.loads(await ws.recv())
        assert (headers["Authorization"], ws.subprotocol) == ("Bearer x", "chat")


async def test_a_refused_handshake_raises_invalid_status(httpbin):
    with pytest.raises(websockets.InvalidStatus) as e:
        await websockets.connect(httpbin.url.replace("http", "ws") + "/get")
    assert e.value.response.status_code == 400


async def test_a_non_websocket_uri_is_refused():
    with pytest.raises(websockets.InvalidURI):
        await websockets.connect("http://127.0.0.1/")


async def test_an_open_timeout(server):
    with pytest.raises(asyncio.TimeoutError):
        await websockets.connect(server.url.replace("http", "ws") + "/slow", open_timeout=0.2)


async def test_wss_checks_the_certificate(wss_url):
    with pytest.raises(httpperf.RequestError):
        await websockets.connect(wss_url)


async def test_wss_with_an_ssl_context_from_pem_data(wss_url):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    with open(pytest_httpbin.certs.where()) as f:
        context.load_verify_locations(cadata=f.read())
    async with websockets.connect(wss_url, ssl=context) as ws:
        await ws.send("x")
        assert await ws.recv() == "x"


async def test_wss_with_verify_off(wss_url):
    async with websockets.connect(wss_url, verify=False) as ws:
        await ws.send("x")
        assert await ws.recv() == "x"


async def test_many_connections_at_once(ws_url):
    async def one(i):
        async with websockets.connect(ws_url) as ws:
            await ws.send(str(i))
            return await ws.recv()

    assert await asyncio.gather(*[one(i) for i in range(200)]) == [str(i) for i in range(200)]


async def test_sending_and_receiving_at_once(ws_url):
    async with websockets.connect(ws_url) as ws:

        async def sender():
            for i in range(2000):
                await ws.send(b"%d" % i)

        task = asyncio.ensure_future(sender())
        received = [await ws.recv() for _ in range(2000)]
        await task
    assert received == [b"%d" % i for i in range(2000)]


async def test_a_slow_reader_gets_every_message_in_order(ws_url):
    async with websockets.connect(ws_url + "/flood/200", max_queue=2) as ws:
        await asyncio.sleep(0.5)
        assert ws._messages.qsize() <= 3
        received = [m async for m in ws]
    assert [m.split(b" ")[0] for m in received] == [b"%d" % i for i in range(200)]


async def test_a_server_ping_is_answered(ws_url):
    async with websockets.connect(ws_url + "/ping") as ws:
        assert await asyncio.wait_for(ws.recv(), 5) == "ponged"


async def test_a_close_without_a_code_is_1005_and_ok():
    sock, url = silent_server(frame(8))
    ws = await websockets.connect(url, close_timeout=0.2)
    with pytest.raises(websockets.ConnectionClosedOK):
        await ws.recv()
    assert ws._rcvd.code == 1005
    sock.close()


async def test_invalid_utf8_closes_with_1007():
    sock, url = silent_server(frame(1, b"\xff"))
    ws = await websockets.connect(url, close_timeout=0.2)
    with pytest.raises(websockets.ConnectionClosedError):
        await ws.recv()
    assert ws._sent.code == 1007
    sock.close()


async def test_close_gives_up_after_close_timeout():
    sock, url = silent_server()
    ws = await websockets.connect(url, close_timeout=0.2)
    await asyncio.wait_for(ws.close(), 5)
    assert ws.close_code == 1006
    sock.close()


async def test_recv_keeps_raising_after_a_close(ws_url):
    ws = await websockets.connect(ws_url + "/close/1000")
    for _ in range(2):
        with pytest.raises(websockets.ConnectionClosedOK):
            await ws.recv()


async def test_a_pending_ping_fails_on_close():
    sock, url = silent_server()
    ws = await websockets.connect(url, close_timeout=0.2, ping_interval=None)
    waiter = await ws.ping()
    await ws.close()
    with pytest.raises(websockets.ConnectionClosed):
        await waiter
    sock.close()


async def test_wait_closed_returns_when_the_server_closes(ws_url):
    ws = await websockets.connect(ws_url + "/close/1000")
    await asyncio.wait_for(ws.wait_closed(), 5)
    assert ws.close_code == 1000


async def test_large_messages_from_several_tasks_do_not_interleave(ws_url):
    async with websockets.connect(ws_url, max_size=None) as ws:
        messages = [bytes([i]) * 500_000 for i in range(3)]
        await asyncio.gather(*[ws.send(m) for m in messages])
        received = [await ws.recv() for _ in messages]
    assert sorted(received) == messages


async def test_origin_and_user_agent_are_sent(ws_url):
    async with websockets.connect(ws_url + "/headers", origin="https://o.example", user_agent_header="ua/1") as ws:
        headers = json.loads(await ws.recv())
    assert (headers["Origin"], headers["User-Agent"]) == ("https://o.example", "ua/1")


async def test_a_session_sends_its_cookies_on_the_handshake(ws_url):
    async with httpperf.Session() as session:
        session.add_cookie_list([httpperf.session_cookie_for_url("http://127.0.0.1/", "sid", "abc")])
        async with websockets.connect(ws_url + "/headers", session=session) as ws:
            assert json.loads(await ws.recv())["Cookie"] == "sid=abc"


async def test_an_open_websocket_holds_its_connection_slot(ws_url):
    async with httpperf.Engine(max_host_connections=1) as engine:
        session = httpperf.Session(engine=engine)
        async with websockets.connect(ws_url, session=session):
            with pytest.raises(asyncio.TimeoutError):
                await websockets.connect(ws_url, session=session, open_timeout=0.3)


async def test_closing_the_engine_closes_its_websockets(ws_url):
    engine = httpperf.Engine()
    ws = await websockets.connect(ws_url, session=httpperf.Session(engine=engine))
    engine.close()
    with pytest.raises(websockets.ConnectionClosedError):
        await asyncio.wait_for(ws.recv(), 5)


@pytest.mark.parametrize("path", ["", "/close/1000"])
async def test_a_finished_connection_is_freed_without_the_garbage_collector(ws_url, path):
    gc.disable()
    try:
        ws = await websockets.connect(ws_url + path)
        ref = weakref.ref(ws)
        await ws.close()
        await asyncio.sleep(0.05)
        del ws
        assert ref() is None
    finally:
        gc.enable()


LEAK_SERVER = """
import asyncio
from websockets.asyncio.server import serve

async def once(ws):
    await ws.send(await ws.recv())
    await ws.close()

async def main():
    async with serve(once, "127.0.0.1", 0) as server:
        print(server.sockets[0].getsockname()[1], flush=True)
        await asyncio.Future()

asyncio.run(main())
"""

LEAK_PROBE = """
import asyncio, gc, json, sys
import httpperf
from httpperf import websockets

def rss():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) * 1024

async def one(url, session, mode):
    ws = await websockets.connect(url, session=session)
    await ws.send("x")
    await ws.recv()
    if mode == "close":
        await ws.close()
    else:
        await ws.wait_closed()

async def main(url, mode):
    session = httpperf.Session()
    for _ in range(1000):
        await one(url, session, mode)
    gc.collect()
    before = rss()
    for _ in range(2000):
        await one(url, session, mode)
    gc.collect()
    print(json.dumps((rss() - before) / 2000))

asyncio.run(main(sys.argv[1], sys.argv[2]))
"""


@pytest.mark.parametrize("mode", ["close", "server-closes"])
def test_websockets_do_not_leak(mode):
    server = subprocess.Popen([sys.executable, "-c", LEAK_SERVER], stdout=subprocess.PIPE, text=True)
    try:
        url = "ws://127.0.0.1:%s/" % server.stdout.readline().strip()
        p = subprocess.run([sys.executable, "-c", LEAK_PROBE, url, mode], capture_output=True, text=True, timeout=120)
    finally:
        server.kill()
        server.wait()
    assert p.returncode == 0, p.stderr
    assert json.loads(p.stdout) < 1024
