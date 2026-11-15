import asyncio
import tempfile

import pytest

import httpperf

SIZE = 50000


@pytest.fixture
def url(httpbin):
    return httpbin.url + "/range/%d" % SIZE


@pytest.fixture
def expected(url):
    async def get():
        async with httpperf.Session() as s:
            return (await s.get(url)).body

    return asyncio.run(get())


@pytest.fixture
def path(tmp_path):
    return tmp_path / "body"


@pytest.mark.parametrize("as_path", [str, lambda p: str(p).encode(), lambda p: p], ids=["str", "bytes", "Path"])
async def test_the_body_is_written_to_the_path(s, url, expected, path, as_path):
    await s.get(url, body_to=as_path(path))
    assert path.read_bytes() == expected


async def test_a_body_written_to_a_path_is_none(s, url, path):
    assert (await s.get(url, body_to=path)).body is None


async def test_the_download_size_counts_a_body_written_to_a_path(s, url, path):
    assert (await s.get(url, body_to=path)).download_size == SIZE


async def test_an_existing_file_is_replaced(s, url, expected, path):
    path.write_bytes(b"x" * (SIZE * 2))
    await s.get(url, body_to=path)
    assert path.read_bytes() == expected


async def test_an_empty_body_leaves_an_empty_file(s, httpbin, path):
    await s.get(httpbin.url + "/status/204", body_to=path)
    assert path.read_bytes() == b""


async def test_a_named_temporary_file_sees_the_body(s, url, expected):
    with tempfile.NamedTemporaryFile() as f:
        await s.get(url, body_to=f.name)
        assert f.read() == expected


async def test_a_missing_directory_raises(s, url, tmp_path):
    with pytest.raises(FileNotFoundError):
        await s.get(url, body_to=tmp_path / "missing" / "body")


async def test_an_open_file_is_refused(s, url, path):
    with open(path, "wb") as f:
        with pytest.raises(TypeError):
            await s.get(url, body_to=f)


async def test_a_discarded_body_is_none(s, url):
    assert (await s.get(url, body_to=httpperf.DISCARD)).body is None


async def test_a_discarded_body_is_counted(s, url):
    assert (await s.get(url, body_to=httpperf.DISCARD)).download_size == SIZE


async def test_a_followed_redirect_keeps_its_body(s, server, path):
    r = await s.get(server.url + "/redirect", body_to=path)
    assert r.history[0].body == b"moved"


async def test_only_the_final_body_is_written(s, server, path):
    await s.get(server.url + "/redirect", body_to=path)
    assert path.read_bytes() == b"ok"


async def test_an_unfollowed_redirect_writes_its_body(s, server, path):
    await s.get(server.url + "/redirect", body_to=path, allow_redirects=False)
    assert path.read_bytes() == b"moved"


@pytest.mark.parametrize("read", [lambda r: r.text, lambda r: r.json()], ids=["text", "json"])
async def test_a_body_that_was_not_kept_cannot_be_read(s, url, read):
    r = await s.get(url, body_to=httpperf.DISCARD)
    with pytest.raises(ValueError, match="body_to"):
        read(r)


async def test_a_failed_write_raises(s, url):
    with pytest.raises(httpperf.RequestError, match="No space left on device"):
        await s.get(url, body_to="/dev/full")
