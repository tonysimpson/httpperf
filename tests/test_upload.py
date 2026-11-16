import asyncio
import socket
import struct
import threading

import pytest
from helpers import BIG

import httpperf


@pytest.fixture
def big_file(tmp_path):
    path = tmp_path / "big"
    path.write_bytes(BIG)
    with open(path, "rb") as f:
        yield f


def pieces():
    for i in range(0, len(BIG), 100000):
        yield BIG[i : i + 100000]


async def test_a_file_is_uploaded_whole(s, server, big_file):
    assert (await s.post(server.url, data=big_file)).body == BIG


async def test_a_file_is_sent_with_its_length(s, server, big_file):
    assert (await s.post(server.url, data=big_file)).headers["x-chunked"] == "False"


async def test_a_file_is_sent_with_no_content_type(s, server, big_file):
    assert (await s.post(server.url, data=big_file)).headers["x-content-type"] == ""


async def test_an_iterable_is_uploaded_whole(s, server):
    assert (await s.put(server.url, data=pieces())).body == BIG


async def test_an_iterable_is_sent_chunked(s, server):
    assert (await s.put(server.url, data=pieces())).headers["x-chunked"] == "True"


async def test_an_async_iterable_is_uploaded_whole(s, server):
    async def chunks():
        for piece in pieces():
            yield piece

    assert (await s.post(server.url, data=chunks())).body == BIG


async def test_a_slow_source_pauses_the_transfer(s, server):
    async def slow():
        yield b"a"
        await asyncio.sleep(0.1)
        yield b"b"

    assert (await s.post(server.url, data=slow())).paused_time > 0.05


async def test_other_requests_carry_on_while_an_upload_waits(s, server):
    others = asyncio.Event()

    async def waiting():
        yield b"a"
        await others.wait()

    upload = asyncio.ensure_future(s.post(server.url, data=waiting()))
    assert (await s.get(server.url)).body == b"ok"
    others.set()
    assert (await upload).body == b"a"


async def test_an_error_in_the_source_is_raised(s, server):
    def failing():
        yield b"a"
        raise OSError("disk gone")

    with pytest.raises(OSError, match="disk gone"):
        await s.post(server.url, data=failing())


async def test_a_large_body_is_sent_without_expect(s, server):
    assert (await s.post(server.url, data=BIG)).headers["x-expect"] == ""


async def test_an_expect_header_that_is_set_is_kept(s, server):
    r = await s.post(server.url, data=BIG, headers={"Expect": "100-continue"})
    assert r.headers["x-expect"] == "100-continue"


async def test_a_307_redirect_sends_the_file_again(s, server, big_file):
    assert (await s.post(server.url + "/redirect/307", data=big_file)).body == BIG


async def test_a_307_redirect_cannot_send_an_iterable_again(s, server):
    with pytest.raises(httpperf.RequestError, match="sent again"):
        await s.post(server.url + "/redirect/307", data=pieces())


async def test_a_303_redirect_after_an_upload_is_a_get(s, server, big_file):
    assert (await s.post(server.url + "/redirect/303", data=big_file)).body == b"ok"


class DroppingServer:
    def __init__(self):
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.url = "http://127.0.0.1:%d/" % self.listener.getsockname()[1]
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        first, _ = self.listener.accept()
        reader = first.makefile("rb")
        self.read_head(reader)
        first.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        self.read_head(reader)
        first.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        reader.close()
        first.close()
        second, _ = self.listener.accept()
        reader = second.makefile("rb")
        reader.read(self.read_head(reader))
        second.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")

    def read_head(self, reader):
        length = 0
        while (line := reader.readline()) not in (b"\r\n", b""):
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":")[1])
        return length


async def test_an_upload_on_a_closed_reused_connection_is_an_error(s, big_file):
    server = DroppingServer()
    await s.get(server.url)
    with pytest.raises(httpperf.RequestError):
        await s.post(server.url, data=big_file)


async def test_an_upload_can_be_retried_after_a_closed_connection(s, big_file):
    server = DroppingServer()
    await s.get(server.url)
    with pytest.raises(httpperf.RequestError):
        await s.post(server.url, data=big_file)
    big_file.seek(0)
    assert (await s.post(server.url, data=big_file)).body == b"ok"
