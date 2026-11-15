import asyncio

BIG = bytes(range(256)) * 16384


async def until(condition, timeout=5):
    for _ in range(int(timeout * 100)):
        if condition():
            return True
        await asyncio.sleep(0.01)
    return False
