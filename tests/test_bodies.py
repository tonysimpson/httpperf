import tempfile

import pytest

import httpperf


async def test_a_form_field_of_none_is_left_out(wire, s):
    await s.post(wire.url, data={"a": None, "b": "x"})
    assert wire.body() == b"b=x"


async def test_a_non_ascii_str_body_has_its_utf8_length(httpbin, s):
    r = await s.post(httpbin.url + "/anything", data="héllo")
    assert r.json()["headers"]["Content-Length"] == "6"


async def test_a_non_ascii_json_body_arrives_whole(httpbin, s):
    assert (await s.post(httpbin.url + "/anything", json={"a": "é"})).json()["json"] == {"a": "é"}


@pytest.mark.parametrize(
    "value, received",
    [
        (("é.txt", b"x"), b"x"),
        (("a.txt", "héllo"), "héllo".encode()),
        (("a.bin", bytes(range(256))), bytes(range(256))),
    ],
)
async def test_a_file_part_arrives_intact(wire, s, value, received):
    await s.post(wire.url, files={"f": value})
    assert b"\r\n\r\n" + received + b"\r\n--" in wire.body()


async def test_a_non_ascii_filename_is_sent(wire, s):
    await s.post(wire.url, files={"f": ("é.txt", b"x")})
    assert 'filename="é.txt"'.encode() in wire.body()


async def test_a_temporary_file_is_uploaded(wire, s):
    with tempfile.TemporaryFile() as f:
        f.write(b"hello")
        f.seek(0)
        await s.post(wire.url, files={"f": f})
    assert b"\r\n\r\nhello\r\n--" in wire.body()


async def test_a_header_of_none_is_left_out(wire, s):
    await s.get(wire.url, headers={"X-A": None})
    assert "x-a" not in wire.head().lower()


@pytest.mark.parametrize("method", ["POST", "PUT"])
async def test_a_bodyless_post_or_put_sends_a_zero_length(wire, s, method):
    await s.request(method, wire.url)
    assert "\r\ncontent-length: 0" in wire.head().lower()


async def test_an_iterable_with_a_content_length_is_not_chunked(wire, s):
    await s.post(wire.url, data=iter([b"ab", b"c"]), headers={"Content-Length": "3"})
    assert ("transfer-encoding" not in wire.head().lower(), wire.body()) == (True, b"abc")


async def test_an_empty_file_is_sent_with_a_zero_length(wire, s, tmp_path):
    (tmp_path / "f").write_bytes(b"")
    with open(tmp_path / "f", "rb") as f:
        await s.post(wire.url, data=f)
    assert "\r\ncontent-length: 0" in wire.head().lower()


async def test_an_empty_bytesio_is_sent_with_a_zero_length(wire, s):
    import io

    await s.post(wire.url, data=io.BytesIO())
    assert "\r\ncontent-length: 0" in wire.head().lower()


async def test_a_set_host_header_is_sent_once(wire, s):
    await s.post(wire.url, data=iter([b"x"]), headers={"Host": "example.com"})
    assert wire.head().lower().count("\r\nhost:") == 1


async def test_header_names_keep_their_case(wire, s):
    await s.get(wire.url, headers={"X-MiXeD": "1"})
    assert "\r\nX-MiXeD: 1" in wire.head()


async def test_several_streamed_uploads_on_one_connection(wire):
    async with httpperf.Session(connections="session") as s:
        for i in range(5):
            await s.post(wire.url, data=iter([b"%d" % i] * 1000))
            assert wire.body() == b"%d" % i * 1000
        assert s.stats()["connections"] == 1
