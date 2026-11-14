import asyncio
import io
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
def f(tmp_path):
    with open(tmp_path / "body", "w+b") as f:
        yield f


def contents(f):
    f.seek(0)
    return f.read()


async def test_the_body_is_written_to_the_file(s, url, expected, f):
    await s.get(url, body_to=f)
    assert contents(f) == expected


async def test_a_body_written_to_a_file_is_none(s, url, f):
    assert (await s.get(url, body_to=f)).body is None


async def test_the_download_size_counts_a_body_written_to_a_file(s, url, f):
    assert (await s.get(url, body_to=f)).download_size == SIZE


async def test_the_file_position_is_after_the_body(s, url, f):
    await s.get(url, body_to=f)
    assert f.tell() == SIZE


async def test_the_body_is_written_after_existing_content(s, url, expected, f):
    f.write(b"before")
    await s.get(url, body_to=f)
    assert contents(f) == b"before" + expected


async def test_an_appended_body_follows_existing_content(s, url, expected, tmp_path):
    (tmp_path / "body").write_bytes(b"before")
    with open(tmp_path / "body", "ab") as f:
        await s.get(url, body_to=f)
    assert (tmp_path / "body").read_bytes() == b"before" + expected


async def test_the_file_can_be_closed_before_the_response_arrives(server, tmp_path):
    async with httpperf.Session() as s:
        with open(tmp_path / "body", "wb") as f:
            request = asyncio.create_task(s.get(server.url + "/slow", body_to=f))
            await asyncio.sleep(0.1)
        server.release.set()
        await request
    assert (tmp_path / "body").read_bytes() == b"ok"


@pytest.mark.parametrize(
    "make_file",
    [tempfile.TemporaryFile, tempfile.NamedTemporaryFile, tempfile.SpooledTemporaryFile],
    ids=["TemporaryFile", "NamedTemporaryFile", "SpooledTemporaryFile"],
)
async def test_temporary_files(s, url, expected, make_file):
    with make_file() as f:
        await s.get(url, body_to=f)
        assert contents(f) == expected


async def test_a_discarded_body_is_none(s, url):
    assert (await s.get(url, body_to=httpperf.DISCARD)).body is None


async def test_a_discarded_body_is_counted(s, url):
    assert (await s.get(url, body_to=httpperf.DISCARD)).download_size == SIZE


async def test_a_followed_redirect_keeps_its_body(s, server, f):
    r = await s.get(server.url + "/redirect", body_to=f)
    assert r.history[0].body == b"moved"


async def test_only_the_final_body_is_written(s, server, f):
    await s.get(server.url + "/redirect", body_to=f)
    assert contents(f) == b"ok"


async def test_an_unfollowed_redirect_writes_its_body(s, server, f):
    await s.get(server.url + "/redirect", body_to=f, allow_redirects=False)
    assert contents(f) == b"moved"


@pytest.mark.parametrize("read", [lambda r: r.text, lambda r: r.json()], ids=["text", "json"])
async def test_a_body_that_was_not_kept_cannot_be_read(s, url, read):
    r = await s.get(url, body_to=httpperf.DISCARD)
    with pytest.raises(ValueError, match="body_to"):
        read(r)


async def test_a_failed_write_raises(s, url):
    with open("/dev/full", "wb") as full:
        with pytest.raises(httpperf.RequestError, match="No space left on device"):
            await s.get(url, body_to=full)


def text_file(tmp_path):
    return open(tmp_path / "text", "w")


def read_only_file(tmp_path):
    (tmp_path / "read").write_bytes(b"")
    return open(tmp_path / "read", "rb")


def closed_file(tmp_path):
    f = open(tmp_path / "closed", "wb")
    f.close()
    return f


@pytest.mark.parametrize(
    "make_file, error",
    [
        (text_file, TypeError),
        (lambda tmp_path: tempfile.NamedTemporaryFile("w+"), TypeError),
        (read_only_file, ValueError),
        (closed_file, ValueError),
        (lambda tmp_path: io.BytesIO(), TypeError),
    ],
    ids=["text", "text temporary", "read only", "closed", "BytesIO"],
)
async def test_unsuitable_files_are_refused(s, url, tmp_path, make_file, error):
    f = make_file(tmp_path)
    with pytest.raises(error):
        await s.get(url, body_to=f)
    f.close()
