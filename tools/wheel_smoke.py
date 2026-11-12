import asyncio

import httpperf


async def main():
    async with httpperf.Session(verify=True) as session:
        r = await session.get("https://pypi.org/simple/httpperf/")
    assert r.http_version == "HTTP/2", r.http_version
    assert r.status_code in (200, 404), r.status_code
    print("ok", r.http_version, r.status_code)


asyncio.run(main())
