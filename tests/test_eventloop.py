import asyncio
import gc
import os

import acurl


def open_fds():
    return len(os.listdir("/proc/self/fd"))


def test_stop_releases_thread_and_fds():
    loop = asyncio.new_event_loop()
    try:
        before = open_fds()
        el = acurl.EventLoop(loop=loop)
        thread = el._thread
        el.stop()
        thread.join(timeout=2)
        assert not thread.is_alive()
        del el
        gc.collect()
        assert open_fds() == before
    finally:
        loop.close()
