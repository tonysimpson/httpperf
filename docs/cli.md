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
| `-t`, `--threads N` | 2 | Client threads. Each has its own event loop and `Engine`. Connections and rate are split between them. |
| `-d`, `--duration T` | 10s | How long each step is measured for: `500ms`, `10s`, `2m`. |
| `-w`, `--warmup T` | 1s | How long each step runs before measuring starts. |
| `-H`, `--header "Name: value"` | | Add a header. Repeatable. |
| `-X`, `--method M` | GET | HTTP method. |
| `--body DATA` | | Request body. `@file` reads it from a file. |
| `--http 1.1\|2\|3` | 2 | HTTP version to ask for. As in the library, HTTP/2 needs `https://`; plain `http://` uses HTTP/1.1. |
| `--timeout T` | 2s | A request that takes longer is an error. |
| `-s`, `--script FILE` | | Python file defining the request to make. See [Scripts](#scripts). |
| `--json` | | Print the results as JSON instead of the report. |

`-H`, `-X` and `--body` apply to the default request, not to scripts.

Arguments after `--` are left in `sys.argv` for the script.

## What a step does

Before the first step, one request is made. If it fails to connect or times out, httpperf stops there.

1. Run for `--warmup` without recording anything, so connections are open and the server is warm.
2. Record for `--duration`.
3. Stop sending and wait for requests in flight, up to `--timeout`. Responses that arrive after the measured period aren't recorded.

The `--threads` threads, each with its own event loop, `Engine` and `Session`, start before the first step and stay up until the last. Connections carry over from one step to the next, so the server isn't hit with a burst of closes and new connections between steps. A step with more connections than the last opens the extra ones during its warm-up. One with fewer leaves the rest idle.

### Flat out

Each of the `-c` requests in flight sends the next request as soon as its response arrives. Throughput is what the server can sustain at that concurrency.

`-c` is requests in flight, not a count of connections. curl opens a connection when a request needs one and none is free, so with HTTP/1.1 there are up to `-c` connections. Fewer are opened if the client can't keep that many on the wire, and HTTP/2 shares a connection between requests.

Latency is measured from when the request was sent. A server that stalls holds up the requests in flight, so fewer requests are sent during the stall and few slow ones are recorded. Flat-out latency is fine for comparing steps, but it understates the tail. Use `--rate` for latency you can quote.

### Fixed rate

Request *i* is due at *start* + *i* / *rate*, spread evenly between the threads. Each time a thread's event loop wakes, it sends every request that is due, so a late wake-up doesn't push the schedule back.

Latency is measured from when the request was due, not when it was sent, as in wrk2. If the server stalls, every request due during the stall records the time it really waited.

`-c` caps connections (`max_total_connections`), split between the threads. Requests over the cap wait in httpperf's queue, and that wait counts in their latency.

If the server can't keep up, the queue grows without limit. A step stops early, marked **over capacity**, when more than one second's worth of requests are outstanding. Later steps in a `--rate` list are skipped.

The schedule is only as precise as the event loop's timers. asyncio and uvloop wait in whole milliseconds, so requests go out up to 1 ms late, about 0.5 ms on average, and that counts in their latency. Sub-millisecond latencies at a fixed rate are not meaningful.

Latency and errors cover requests that were due during the measured period, including ones that finish after it. Achieved req/s counts what finished during it.

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

`session` is the thread's `httpperf.Session`. `url` is the URL from the command line.

- Each call of `request` counts as one request for throughput and latency, even if it makes several HTTP requests.
- An exception raised by `request` is an error, named by its message.
- If it returns a `Response`, the status rule above applies.
- Module-level code runs once, before the first step, so set up data there. Arguments after `--` are in `sys.argv`.
- The script runs in every thread at once. Keep shared state read-only, or lock it.

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

Python work in the script is client CPU per request. A slow script limits the result, and the report will say so (see [Warnings](#warnings)).

## The report

lwan serving a 13-byte response, with the client on 4 cores:

```
httpperf 1.0rc4 (libcurl 8.14.1), 2 threads, http://localhost:8080/hello
flat out, 10s per step after 1s warm-up

  connections    req/s  ±/s     p50     p99   p99.9     max  errors
           10  169,406   1%  0.06ms  0.07ms  0.13ms  6.75ms       0
           50  207,192   1%  0.30ms  0.35ms  0.93ms  5.39ms       0
          200  210,121   0%  0.97ms  1.30ms  2.90ms  5.98ms       0
        1,000  211,613   1%  4.72ms  6.29ms  10.1ms  14.8ms       0  ← best

best: 1,000 connections, 211,613 req/s
  latency      p50 4.72ms  p90 4.91ms  p99 6.29ms  p99.9 10.1ms  max 14.8ms
    first byte p50 0.02ms  p99 0.02ms
    download   p50 0.00ms  p99 0.00ms
    connect    p50 0.14ms  p99 0.17ms
  requests     2,116,127 in 10s, 0 errors
  transfer     received 38.7 MB/s, sent 21.2 MB/s
  connections  2 opened, 55 open at the end, HTTP/1.1, 0 TLS handshakes
  client       3.0 of 4 cores, 14µs CPU per request
               client lag p99 6.26ms, connection wait p99 0.00ms

warning: 10 connections: client saturated: client lag p99 0.05ms is over a quarter of latency p99 0.07ms
warning: 50 connections: client saturated: a client thread was 99% busy, so the result describes the client, not the server
warning: 200 connections: client saturated: a client thread was 100% busy, so the result describes the client, not the server
warning: 1,000 connections: client saturated: a client thread was 100% busy, so the result describes the client, not the server
```

Here the client was the limit at every step, and the warnings say so: lwan can serve far more than 211k req/s.

A step with one value for `-c` or `--rate` prints only the second part.

| Line | Meaning |
|---|---|
| req/s, ±/s | Requests completed per second over the measured period, errors included, and how much the per-second counts varied (coefficient of variation). |
| latency | Flat out: from send to response. Fixed rate: from when the request was due to response. |
| first byte, download | curl's `starttransfer_time`, and `total_time` minus it. A slow server shows up in first byte. A large body or slow network shows up in download. |
| requests | Requests completed in the measured period, and errors by kind. |
| transfer | `bytes_received` and `bytes_sent` per second: headers and bodies, not TLS or TCP overhead. |
| connect, TLS | Time to connect and to complete the TLS handshake, for connections opened while measuring. Shown only when there were some. |
| connections | Connections opened during the step, warm-up included, open at the end, HTTP versions used, TLS handshakes. |
| client | CPU used by the httpperf process, as cores and per request. `client_lag` p99: how long requests waited on Python, before they were sent and after they finished. `queue_time` p99: how long requests waited for a free connection. |

For fixed rate, the step table has columns for the target rate and the achieved rate, and `best` is the highest rate that kept up.

`--json` prints `{"steps": [...], "best": index}`, with every number above for each step, unrounded, in seconds and bytes, and percentiles from p50 to p99.99.

## Warnings

Printed under the report when they apply:

- **Client saturated**: the process used more than 90% of the cores it was allowed, one of its threads (an event loop or a curl thread) was more than 90% busy, or `client_lag` p99 was more than a quarter of latency p99. The result describes the client, not the server. Add `--threads` if there are spare cores, simplify the script, or run the client on another machine.
- **Errors**: more than 1% of requests failed. The most common kinds are listed.
- **Unsteady**: req/s varied more than 10% from second to second. Run for longer, or check the server.
- **Connection churn**: connections were closed during measurement, so the server or a proxy is closing them. Latency includes opening new ones.
- **Behind schedule** (fixed rate): achieved rate was under 99% of the target.
- **Over capacity** (fixed rate): the step stopped early because requests piled up.
- **Peak not reached** (flat out list): the best step was the last, and the client wasn't saturated. Try more connections.

## Exit status

| Code | Meaning |
|---|---|
| 0 | All steps ran without errors. |
| 1 | Some requests failed, or a step was over capacity. |
| 2 | Bad arguments, a broken script, or the server couldn't be reached at all. |

Ctrl-C stops the current step, prints the results of the steps that finished, and exits with 130.

## Tips

- Use the server's own machine, or one close to it, and pin the client and server to separate cores (`taskset`). Otherwise they compete and both numbers are wrong.
- Watch the client line. If it says the client is saturated, more threads help while there are spare cores. On Python with the GIL, two threads is usually the most that helps, since the event loops share the GIL. Free-threaded Python (3.14t) scales further.
- uvloop is used if installed.
- For a test that needs more than one kind of request, or users with state, write the load test with the library instead.

## Not in the first version

- Several URLs or scripted user journeys with setup and teardown.
- Random (Poisson) arrivals for `--rate`.
- A latency target, such as "the highest rate with p99 under 50 ms".
- Pre-built requests to save Python work per request.

## Implementation notes

- Latency is recorded into a log-linear histogram (HdrHistogram-style, percentiles accurate to under 1%) per thread, written in Python and merged after each step. The CLI costs about 0.5 µs of CPU per request more than `bench/client.py`, so no C helper is needed.
- First byte, download, `client_lag`, `queue_time`, connect and TLS times come from `session.set_response_callback`, so they cover each HTTP request a script makes.
- Per-second req/s comes from counting completions into one-second buckets. Engine `stats()` is read at the start of the step, the start and end of measuring, and once requests in flight have finished.
- Process CPU comes from `resource.getrusage`, per-thread CPU from `/proc/self/task/*/stat` (Linux only; elsewhere that check is skipped), and allowed cores from `os.sched_getaffinity`.
