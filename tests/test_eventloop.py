import asyncio
import errno
import gc
import os
import subprocess
import sys

import acurl


def open_fds():
    return len(os.listdir("/proc/self/fd"))


def test_stop_releases_thread_and_fds():
    loop = asyncio.new_event_loop()
    try:
        gc.collect()
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


def test_out_of_fds_raises():
    code = """
import asyncio, os, resource
import acurl

resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
loop = asyncio.new_event_loop()
held = []
try:
    while True:
        held.append(os.open(os.devnull, os.O_RDONLY))
except OSError:
    pass
os.close(held.pop())
try:
    acurl.EventLoop(loop=loop)
except OSError as e:
    print("OSError", e.errno)
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert p.stdout.split() == ["OSError", str(errno.EMFILE)]
