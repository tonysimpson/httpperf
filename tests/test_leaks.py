import json
import subprocess
import sys

import pytest

PROBE = """
import asyncio, gc, json, os, subprocess, sys
import httpperf

def rss():
    if not os.path.exists("/proc/self/status"):
        return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())])) * 1024
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) * 1024

async def get(el, url, connections, mode):
    session = httpperf.Session(engine=el, connections=connections)
    if mode == "upload":
        r = await session.post(url, data=iter([b"x" * 1000]))
    else:
        r = await session.get(url, stream=mode == "stream")
    await r.read()

async def main(url, n, connections, mode):
    el = httpperf.Engine()
    for _ in range(n // 4):
        await get(el, url, connections, mode)
    gc.collect()
    before = rss()
    for _ in range(n):
        await get(el, url, connections, mode)
    gc.collect()
    print(json.dumps((rss() - before) / n))

asyncio.run(main(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]))
"""


@pytest.mark.parametrize("mode", ["", "stream", "upload"])
@pytest.mark.parametrize("connections", ["shared", "session", "none"])
def test_session_per_request_does_not_leak(httpbin, connections, mode):
    p = subprocess.run(
        [sys.executable, "-c", PROBE, httpbin.url + "/anything", "1000", connections, mode],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert p.returncode == 0, p.stderr
    assert json.loads(p.stdout) < 1024
