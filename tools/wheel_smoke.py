import asyncio
import os
import subprocess
import sys

import httpperf
from httpperf import _backend


async def check(http_version):
    async with httpperf.Session(verify=True, http_version=http_version) as session:
        r = await session.get("https://pypi.org/simple/httpperf/")
    assert r.http_version == http_version, r.http_version
    assert r.status_code in (200, 404), r.status_code
    print("ok", _backend.name, r.http_version, r.status_code, flush=True)


async def main():
    await check("HTTP/2")
    await check("HTTP/3")


asyncio.run(main())
if _backend.name == "curl":
    subprocess.run([sys.executable, __file__], env={**os.environ, "HTTPPERF_BACKEND": "hx"}, check=True)
