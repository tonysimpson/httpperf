import asyncio

import httpperf


async def check(http_version):
    async with httpperf.Session(verify=True, http_version=http_version) as session:
        r = await session.get("https://pypi.org/simple/httpperf/")
    assert r.http_version == http_version, r.http_version
    assert r.status_code in (200, 404), r.status_code
    print("ok", r.http_version, r.status_code)


async def main():
    await check("HTTP/2")
    await check("HTTP/3")


asyncio.run(main())
