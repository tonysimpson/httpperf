import pytest

from httpperf import _httpperf, websockets


async def test_connect_needs_websocket_support_in_libcurl(monkeypatch):
    monkeypatch.setattr(_httpperf, "websockets", 0)
    with pytest.raises(RuntimeError, match="libcurl"):
        await websockets.connect("ws://127.0.0.1:9/")
