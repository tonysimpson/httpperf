import asyncio
import socket
import sys
import threading
import time

import pytest
from helpers import BIG, until

import httpperf


def serve(handle):
    sock = socket.create_server(("127.0.0.1", 0))

    def run():
        while True:
            try:
                conn, _ = sock.accept()
            except OSError:
                return
            threading.Thread(target=handle, args=(conn,), daemon=True).start()

    threading.Thread(target=run, daemon=True).start()
    return sock, "http://127.0.0.1:%d/" % sock.getsockname()[1]


def trickle(conn):
    with conn:
        conn.recv(65536)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
        try:
            for _ in range(100):
                conn.sendall(b"x")
                time.sleep(0.1)
        except OSError:
            pass


async def test_iter_content_gives_every_byte_with_no_empty_chunks(wire, s):
    wire.reply = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789"
    async with await s.get(wire.url, stream=True) as r:
        chunks = [c async for c in r.iter_content(4)]
    assert (b"".join(chunks), all(chunks)) == (b"0123456789", True)


async def test_an_unread_stream_does_not_block_the_next_request(server, s):
    r = await s.get(server.url + "/big", stream=True)
    assert (await asyncio.wait_for(s.get(server.url), 5)).body == b"ok"
    r.close()


async def test_a_streamed_download_and_an_upload_at_once(server, s):
    async def download():
        async with await s.get(server.url + "/big", stream=True) as r:
            return b"".join([c async for c in r.iter_content()])

    up = s.post(server.url, data=iter([BIG]))
    got, sent = await asyncio.wait_for(asyncio.gather(download(), up), 10)
    assert (got, sent.body) == (BIG, BIG)


async def test_a_trickling_server_is_stopped_by_the_timeout(s):
    sock, url = serve(trickle)
    start = time.monotonic()
    with pytest.raises(httpperf.Timeout):
        await s.get(url, timeout=0.5)
    sock.close()
    assert time.monotonic() - start < 2


async def test_a_server_that_never_answers_is_stopped_by_the_timeout(server, s):
    with pytest.raises(httpperf.Timeout):
        await s.get(server.url + "/slow", timeout=0.3)


async def test_a_timed_out_requests_response_does_not_reach_the_next(server, s):
    with pytest.raises(httpperf.Timeout):
        await s.get(server.url + "/slow", timeout=0.2)
    server.release.set()
    assert (await s.get(server.url + "/next")).body == b"ok"


async def test_a_tiny_timeout_is_a_timeout(server, s):
    with pytest.raises(httpperf.Timeout):
        await s.get(server.url + "/slow", timeout=0.000001)


async def test_cancelling_a_request_raises_cancelled_and_hangs_up(server, s):
    task = asyncio.ensure_future(s.get(server.url + "/slow"))
    await until(lambda: server.started)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await until(lambda: server.hung_up == 1)


@pytest.mark.skipif(sys.version_info < (3, 11), reason="asyncio.timeout is new in 3.11")
async def test_asyncio_timeout_cancels_cleanly(server, s):
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await s.get(server.url + "/slow")
    server.release.set()
    assert (await s.get(server.url)).body == b"ok"


async def test_many_requests_timed_out_by_wait_for(server, s):
    async def one():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(s.get(server.url + "/slow"), 0.05)

    await asyncio.gather(*[one() for _ in range(50)])
    server.release.set()
    assert (await s.get(server.url)).body == b"ok"


def reject_upload(conn):
    with conn:
        while b"\r\n\r\n" not in conn.recv(65536):
            pass
        conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        conn.shutdown(socket.SHUT_WR)
        time.sleep(0.2)


async def test_a_post_rejected_before_its_body_is_read(s):
    sock, url = serve(reject_upload)
    r = await s.post(url, data=BIG)
    assert r.status_code == 400
    assert (await s.post(url, data=b"x")).status_code == 400
    sock.close()


async def test_one_call_sends_one_request(server, s):
    with pytest.raises(httpperf.Timeout):
        await s.get(server.url + "/slow", timeout=0.5)
    assert server.started == ["/slow"]
