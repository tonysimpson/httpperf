# Command line

`python -m httpperf` (or `httpperf`) is a load generator in the spirit of [wrk](https://github.com/wg/wrk) and [wrk2](https://github.com/giltene/wrk2). Use it to find out how much a server can handle, and how fast it responds when it's busy.

It has two modes:

- **Flat out** (the default): a fixed number of requests are in flight at all times. Each one sends its next request as soon as the last response arrives. Use this to find peak throughput.
- **Fixed rate** (`--rate`): requests go out on a schedule, whatever the server is doing. Use this to measure latency at a given load.

Both take a list of steps, so one command can find where a server levels off.

## Quick start

```
python -m httpperf http://localhost:8080/
```

Ten requests in flight for ten seconds, after a one-second warm-up.

Find the peak:

```
python -m httpperf -c 10,50,200,1000 http://localhost:8080/
```

Then measure latency below it:

```
python -m httpperf --rate 20000,40000,60000 http://localhost:8080/
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `-c`, `--connections N[,N...]` | 10 (flat out), 100 (fixed rate) | Flat out: requests in flight, and a list runs each in turn. Fixed rate: the most connections to open, one number. |
| `-r`, `--rate N[,N...]` | | Requests per second, in total. Switches to fixed rate. A list runs each in turn. |
| `-t`, `--workers N` | 2 | Workers, each with its own event loop and `Engine`. Connections and rate are split between them. On Python with the GIL each worker is a process; on free-threaded Python it is a thread. |
| `-d`, `--duration T` | 10s | How long each step is measured for: `500ms`, `10s`, `2m`. |
| `-w`, `--warmup T` | 1s | How long each step runs before measuring starts. |
| `-H`, `--header "Name: value"` | | Add a header. Repeatable. |
| `-X`, `--method M` | GET | HTTP method. |
| `--body DATA` | | Request body. `@file` reads it from a file. |
| `--http 1.1\|2\|3` | 1.1 | HTTP version to ask for. HTTP/2 needs `https://`; plain `http://` stays on HTTP/1.1. The library defaults to HTTP/2, but the CLI defaults to HTTP/1.1 so that `-c` means connections, as in wrk. |
| `--timeout T` | 2s | A request that takes longer is an error. |
| `-s`, `--script FILE` | | Python file defining the request to make. See [Scripts](#scripts). |
| `--json` | | Print the results as JSON instead of the report. |

`-H`, `-X` and `--body` apply to the default request, not to scripts.

Arguments after `--` are left in `sys.argv` for the script.

## What a step does

1. Run for `--warmup` without recording anything, so connections are open and the server is warm.
2. Record for `--duration`.
3. Stop sending and wait for requests in flight, up to `--timeout`. Responses that arrive after the measured period aren't recorded.

The workers, each with its own event loop, `Engine` and `Session`, start before the first step and stay up until the last. Connections carry over from one step to the next, so the server isn't hit with a burst of closes and new connections between steps. A step with more connections than the last opens the extra ones during its warm-up. One with fewer leaves the rest idle.

### Flat out

Each of the `-c` requests in flight sends the next request as soon as its response arrives. Throughput is what the server can sustain at that concurrency.

With HTTP/1.1, the default, each request in flight has its own connection, so there are up to `-c` connections. curl opens one when a request needs it and none is free, so fewer are opened if the client can't keep that many on the wire.

With `--http 2` or `--http 3`, requests in flight share connections, so `-c` is the number of streams, usually on one connection per worker.

Latency is measured from when the request was sent. A server that stalls holds up the requests in flight, so fewer requests are sent during the stall and few slow ones are recorded. Flat-out latency is fine for comparing steps, but it understates the tail. Use `--rate` for latency you can quote.

### Fixed rate

Request *i* is due at *start* + *i* / *rate*, spread evenly between the workers. Each time a worker's event loop wakes, it sends every request that is due, so a late wake-up doesn't push the schedule back.

Latency is measured from when the request was sent. Each request is sent on schedule, whatever is happening to earlier ones, so a server that stalls can't hold back sends: every request sent during the stall records the time it really waited. This avoids coordinated omission without wrk2's approach of measuring from when each request was due, which would also count the client's own lateness.

`-c` caps connections (`max_total_connections`), split between the workers. Requests over the cap wait in httpperf's queue, and that wait counts in their latency.

If the server can't keep up, the queue grows without limit. A step stops early, marked **over capacity**, when more than one second's worth of requests are outstanding. Later steps in a `--rate` list are skipped.

How late requests went out is reported as send lag. The schedule is only as precise as the event loop's timers: asyncio and uvloop wait in whole milliseconds, so when the client is idle, requests go out up to 1 ms late. That spreads the load slightly unevenly, but it doesn't count in latency. Send lag p99 over 2 ms means the client couldn't keep to the schedule.

Latency, send lag and errors cover requests that were due during the measured period, including ones that finish after it. Achieved req/s counts what finished during it.

## Errors

A request is an error if:

- it fails (connection refused, reset, timeout), or
- the response status is 400 or above.

Errors are counted and reported by kind (`HTTP 503`, `timeout`, curl's message for other failures, or the script's exception). They count in req/s, as in wrk, but not in latency. A step with more than 1% errors can't be the best step.

## Scripts

A script is a Python file defining one coroutine:

```python
async def request(session, url):
    return await session.get(url + "/search", params={"q": "shoes"})
```

`session` is the worker's `httpperf.Session`. `url` is the URL from the command line.

- Each call of `request` counts as one request for throughput and latency, even if it makes several HTTP requests.
- An exception raised by `request` is an error, named by its message.
- If it returns a `Response`, the status rule above applies.
- Module-level code runs before the first step, so set up data there: once in the main process, to check the script, and once in each worker process. With free-threaded Python the workers are threads and share the main process's copy. Arguments after `--` are in `sys.argv`.
- On free-threaded Python the script runs in every worker thread at once. Keep shared state read-only, or lock it.

```python
import random
import sys

import httpperf

ids = open(sys.argv[1]).read().split()


async def request(session, url):
    r = await session.get(url + "/items/" + random.choice(ids), body_to=httpperf.DISCARD)
    if r.headers.get("x-cache") != "hit":
        raise ValueError("cache miss")
    return r
```

Without a script, the request is the method, headers and body from the command line, with the response body discarded (`body_to=httpperf.DISCARD`).

Python work in the script adds to httpperf's work per request. A slow script limits the result, and the report will say so (see [Warnings](#warnings)).

## The report

The layout follows wrk's, with p90 and p99 added. The first lines are printed before the test starts.

A single step, against lwan serving a 13-byte response, with the client on 4 cores:

```
Running 10s test @ http://127.0.0.1:8080/hello
  2 workers and 10 connections
  Worker Stats       Avg     Stdev       p90       p99       Max   +/- Stdev
    Latency      48.41us   35.90us   48.50us   52.50us    6.40ms      99.88%
    Req/Sec      102.71k     2.01k                       105.48k      80.50%
  2054228 requests in 10.00s, 358.51MB read
Requests/sec:  205422.80
Transfer/sec:    35.85MB

Warning: httpperf was the limit, not the server, so the server can do at least 205423 requests/sec.
```

| Line | Meaning |
|---|---|
| Latency | Per request, from all workers: mean, standard deviation, p90, p99, max, and the share of requests within one standard deviation of the mean. |
| Req/Sec | Per worker, sampled every 100 ms: mean, standard deviation, max, and the share of samples within one standard deviation of the mean. |
| requests, read | Requests completed in the measured period, errors included, and response bytes read (headers and bodies). |
| Errors | Only when there were some: the count, and the most common kinds. |
| Requests/sec, Transfer/sec | Over the measured period. |

Several steps print one row each as they finish:

```
Running 4 steps of 10s @ http://127.0.0.1:8080/hello
  2 workers
   Connections  Requests/sec       Avg       p90       p99       Max  Errors
            10     204669.10   48.58us   48.50us   53.50us    4.12ms       0
            50     204802.40  243.86us  246.50us  279.00us    4.25ms       0
           200     205313.60  973.82us  986.00us  998.00us    5.01ms       0
          1000     205457.90    4.87ms    4.91ms    5.01ms   10.46ms       0

Best: 10 connections, 204669.10 requests/sec

Warning: httpperf was the limit at 10, 50, 200 and 1000 connections, not the server, so the server can do at least 205458 requests/sec.
Warning: Not all the connections were in use at 200 and 1000 connections, because httpperf was the limit.
```

The best flat-out step is the one with the fewest connections that came within 3% of the highest requests/sec: the point where more concurrency stops buying throughput and only adds latency. For fixed rate, the first column is the target rate, and the best step is the highest rate that kept up. Neither counts a step with more than 1% errors.

`--json` prints `{"steps": [...], "best": index}` at the end instead, with more detail for each step, unrounded, in seconds and bytes: latency percentiles from p50 to p99.99, time to first byte, download time, connect and TLS times, `client_lag`, `queue_time`, send lag, bytes sent, connections opened and closed, HTTP versions, and TLS handshakes.

## Warnings

Printed at the end when they apply. A warning that applies to several steps is printed once, naming the steps.

| Warning | When |
|---|---|
| httpperf was the limit, not the server, so the server can do at least N requests/sec. | Requests spent more than half their time (p50) waiting on httpperf rather than with the server: `client_lag`, which covers a busy event loop, a busy curl thread, and runs with few connections where httpperf's own work per request sets the pace. |
| Only N of M connections were in use, because httpperf was the limit. | Flat out over HTTP/1.1: httpperf was the limit and had fewer than 90% of `-c` connections open. |
| N% of requests failed. | More than 1% of requests failed. |
| Requests/sec varied by N% from second to second. | More than 10%. |
| The server closed connections during the test, so some requests waited for new ones. | Connections were closed while measuring. |
| httpperf couldn't keep to the schedule, so it sent fewer requests than asked for. | Fixed rate: achieved rate under 99% of the target, or send lag p99 over 2 ms. |
| Requests piled up, so the run stopped there. | Fixed rate: more than a second's worth of requests outstanding. |
| Requests/sec was still rising at the last step, so try more connections. | Flat out with a list: the best step was the last, and httpperf wasn't the limit. |

"httpperf was the limit" is common against fast servers, and the result is then a lower bound for the server. Each worker manages about 100k requests/sec for small responses and uses about two cores (its event loop and its curl thread). Against lwan, with 8 hardware threads for the client, 1, 2, 4 and 6 workers reached 104k, 207k, 281k and 336k requests/sec, and 8 workers were slower than 6. Give each worker two hardware threads.

## Exit status

| Code | Meaning |
|---|---|
| 0 | All steps ran without errors. |
| 1 | Some requests failed (including when the server can't be reached), or a step was over capacity. |
| 2 | Bad arguments or a broken script. |

Ctrl-C stops the current step, prints the results of the steps that finished, and exits with 130.

## Tips

- Use the server's own machine, or one close to it, and pin the client and server to separate cores (`taskset`). Otherwise they compete and both numbers are wrong.
- If the warnings say httpperf was the limit, more workers (`-t`) help while there are spare cores, up to about one worker for every two hardware threads.
- uvloop is used if installed.
- For a test that needs more than one kind of request, or users with state, write the load test with the library instead.

## Not in the first version

- Several URLs or scripted user journeys with setup and teardown.
- Random (Poisson) arrivals for `--rate`.
- A latency target, such as "the highest rate with p99 under 50 ms".
- Pre-built requests to save Python work per request.

## Implementation notes

- Latency is recorded into a log-linear histogram (HdrHistogram-style, percentiles accurate to under 1%) per worker, written in Python and merged after each step. The CLI costs about 0.5 µs of CPU per request more than `bench/client.py`, so no C helper is needed.
- First byte, download, `client_lag`, `queue_time`, connect and TLS times come from `session.set_response_callback`, so they cover each HTTP request a script makes.
- Per-second req/s comes from counting completions into one-second buckets. Engine `stats()` is read at the start of the step, the start and end of measuring, and once requests in flight have finished.
- Workers run in a `concurrent.futures` pool: `ProcessPoolExecutor` with the `spawn` start method on Python with the GIL (forking after curl threads have started isn't safe), `ThreadPoolExecutor` on free-threaded Python. Each step is one task per worker. A step task lasts the whole step, so no worker takes two, and a shared barrier at start-up makes sure every worker is ready and gives each its index.
- The code lives in `httpperf/_cli.py`, because a process pool can't find functions in a package's `__main__`.
