import asyncio
import gc

import pytest
from helpers import BIG, until

import httpperf


async def test_a_streamed_body_arrives_whole(s, server):
    r = await s.get(server.url + "/big", stream=True)
    assert b"".join([chunk async for chunk in r.iter_content()]) == BIG


async def test_chunks_are_no_bigger_than_asked_for(s, server):
    r = await s.get(server.url + "/big", stream=True)
    assert max([len(chunk) async for chunk in r.iter_content(1000)]) == 1000


async def test_read_gives_the_body(s, server):
    r = await s.get(server.url + "/big", stream=True)
    assert await r.read() == BIG


async def test_the_body_must_be_read_first(s, server):
    r = await s.get(server.url + "/big", stream=True)
    with pytest.raises(RuntimeError, match="read"):
        _ = r.body


async def test_timings_wait_for_the_body(s, server):
    r = await s.get(server.url + "/big", stream=True)
    with pytest.raises(RuntimeError, match="read"):
        _ = r.total_time


async def test_a_slow_reader_pauses_the_transfer(s, server):
    r = await s.get(server.url + "/big", stream=True)
    await asyncio.sleep(0.1)
    await r.read()
    assert r.paused_time > 0.05


async def test_other_requests_carry_on_while_one_is_paused(s, server):
    r = await s.get(server.url + "/big", stream=True)
    await asyncio.sleep(0.1)
    assert (await s.get(server.url)).body == b"ok"
    await r.read()


async def test_a_stream_read_to_the_end_leaves_its_connection_open(s, server):
    for _ in range(2):
        await (await s.get(server.url + "/big", stream=True)).read()
    assert s.stats()["connections"] == 1


async def test_closing_stops_the_transfer(s, server):
    r = await s.get(server.url + "/big", stream=True)
    r.close()
    assert await until(lambda: s.stats()["cancelled"] == 1)


async def test_a_dropped_response_stops_the_transfer(s, server):
    await s.get(server.url + "/big", stream=True)
    gc.collect()
    assert await until(lambda: s.stats()["cancelled"] == 1)


async def test_a_body_cut_short_raises(s, server):
    r = await s.get(server.url + "/truncated", stream=True)
    with pytest.raises(httpperf.RequestError):
        await r.read()


async def test_redirects_are_followed_when_streaming(s, server):
    r = await s.get(server.url + "/redirect", stream=True)
    assert (r.history[0].body, await r.read()) == (b"moved", b"ok")


async def test_stream_cannot_be_combined_with_discard_body(s, server):
    with pytest.raises(ValueError):
        await s.get(server.url, stream=True, discard_body=True)


async def test_an_unstreamed_body_can_be_iterated(s, server):
    r = await s.get(server.url + "/big")
    assert b"".join([chunk async for chunk in r.iter_content(1000)]) == BIG


@pytest.mark.filterwarnings("ignore::ResourceWarning")
async def test_closing_the_engine_fails_a_paused_stream(server):
    engine = httpperf.Engine()
    r = await httpperf.Session(engine=engine).get(server.url + "/big", stream=True)
    await asyncio.sleep(0.1)
    engine.close()
    with pytest.raises(httpperf.RequestError, match="engine closed"):
        await r.read()
