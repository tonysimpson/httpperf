# Profiling httpperf

httpperf does its network I/O on a C thread (`EventLoop_main`) and hands requests and responses to Python through pipes. The tools below work on both threads.

All examples use a small driver script, `demo.py`:

```python
import asyncio
import sys

import httpperf

engine = httpperf.Engine()
session = httpperf.Session(engine=engine)


async def main():
    for _ in range(int(sys.argv[2])):
        await session.get(sys.argv[1])


asyncio.run(main())
engine.close()
engine._thread.join()
```

## Call tracing and timing: uftrace

[uftrace](https://github.com/namhyung/uftrace) records every function entry and exit in the extension, per thread, with timestamps.

1. Build the extension with compiler instrumentation (`-finstrument-functions`). `HTTPPERF_PROFILE=1` turns it on; the vendored `ae` loop and inline functions from system and Python headers are excluded to keep traces readable. Force a rebuild so a cached build isn't reused:

   ```sh
   HTTPPERF_PROFILE=1 uv pip install --reinstall-package httpperf -e .
   # or: HTTPPERF_PROFILE=1 pip install --no-cache-dir --force-reinstall --no-deps -e .
   ```

2. Record. `--force` is needed because the Python interpreter itself isn't instrumented; `--no-libcall` leaves out calls into libc and libcurl:

   ```sh
   uftrace record --force --no-libcall python demo.py http://127.0.0.1:8080/hello 1000
   ```

3. Inspect the `uftrace.data` directory:

   ```sh
   uftrace report                 # total and self time per function
   uftrace report --threads       # the same, split by thread
   uftrace replay                 # the full call trace, with thread IDs and durations
   uftrace replay --tid <TID>     # one thread only (thread IDs are listed by `uftrace info`)
   uftrace graph                  # call graph with aggregated times
   uftrace tui                    # interactive browser
   uftrace dump --chrome > trace.json   # open in https://ui.perfetto.dev
   ```

   Useful filters: `-F <function>` to trace only below a function (for example `-F start_request`), `-N <function>` to hide one, `-t 10us` to drop calls shorter than a threshold. Leave out `--no-libcall` to see time spent in libc and libcurl as well.

4. Instrumentation adds overhead to every call, so rebuild without it before benchmarking:

   ```sh
   uv pip install --reinstall-package httpperf -e .
   ```

## Memory: heaptrack

[heaptrack](https://github.com/KDE/heaptrack) records every allocation with its call stack and is fast enough to run on a real workload. It needs no special build, though debug symbols give better stacks (`CFLAGS=-g` when building).

```sh
heaptrack -o httpperf python demo.py http://127.0.0.1:8080/hello 10000
heaptrack_print httpperf.zst | less      # peak usage, top allocators, leaks
heaptrack_gui httpperf.zst               # flame graphs and allocation timelines, if installed
```

The "leaked" section lists memory still allocated at exit, with the stack that allocated it, which is the quickest way to find a leak in the C code.

## Memory: valgrind massif

massif takes heap snapshots over time and shows which call stacks hold the most memory at the peak. It is much slower than heaptrack (expect 20–50×), so use fewer requests. `PYTHONMALLOC=malloc` makes Python's own allocations go through `malloc`, so they appear too:

```sh
PYTHONMALLOC=malloc valgrind --tool=massif --massif-out-file=massif.out python demo.py http://127.0.0.1:8080/hello 300
ms_print massif.out | less
```

`massif-visualizer massif.out` gives a graphical view, if installed. Steadily rising snapshots across a run with a constant workload point to a leak.
