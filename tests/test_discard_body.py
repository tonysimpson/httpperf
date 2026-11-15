import pytest

SIZE = 50000


@pytest.fixture
def url(httpbin):
    return httpbin.url + "/range/%d" % SIZE


async def test_a_discarded_body_is_none(s, url):
    assert (await s.get(url, discard_body=True)).body is None


async def test_a_discarded_body_is_counted(s, url):
    assert (await s.get(url, discard_body=True)).download_size == SIZE


@pytest.mark.parametrize("read", [lambda r: r.text, lambda r: r.json()], ids=["text", "json"])
async def test_a_discarded_body_cannot_be_read(s, url, read):
    r = await s.get(url, discard_body=True)
    with pytest.raises(ValueError, match="discarded"):
        read(r)


async def test_redirects_are_followed_when_discarding(s, server):
    r = await s.get(server.url + "/redirect", discard_body=True)
    assert [h.status_code for h in r.history] + [r.status_code] == [302, 200]


async def test_a_redirect_body_is_discarded_too(s, server):
    r = await s.get(server.url + "/redirect", discard_body=True)
    assert r.history[0].body is None
