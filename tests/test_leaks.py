import json
import subprocess
import sys

import pytest

PROBE = """
import asyncio, gc, json, sys
import acurl

def rss():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) * 1024

async def main(url, n, connections):
    el = acurl.Engine()
    for _ in range(n // 4):
        await acurl.Session(engine=el, connections=connections).get(url)
    gc.collect()
    before = rss()
    for _ in range(n):
        await acurl.Session(engine=el, connections=connections).get(url)
    gc.collect()
    print(json.dumps((rss() - before) / n))

asyncio.run(main(sys.argv[1], int(sys.argv[2]), sys.argv[3]))
"""


@pytest.mark.parametrize("connections", ["shared", "session", "none"])
def test_session_per_request_does_not_leak(httpbin, connections):
    p = subprocess.run(
        [sys.executable, "-c", PROBE, httpbin.url + "/get", "1000", connections],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert p.returncode == 0, p.stderr
    assert json.loads(p.stdout) < 1024
