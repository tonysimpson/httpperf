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
| `-c`, `--connections N[,N...]` | 10 (flat out), 100 (fixed rate) | Flat out: requests in flight. Fixed rate: the most connections to open. A list runs each in turn. |
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

Only one of `-c` and `--rate` may be a list.

`-H`, `-X` and `--body` apply to the default request, not to scripts.

Arguments after `--` are left in `sys.argv` for the script.

## What a step does

1. Start `--threads` threads, each with its own event loop, `Engine` and `Session`.
2. Run for `--warmup` without recording anything, so connections are open and the server is warm.
3. Record for `--duration`.
4. Stop sending and wait for requests in flight, up to `--timeout`. Responses that arrive after the measured period aren't recorded.

Each step opens new connections, so steps don't affect each other.

### Flat out

Each of the `-c` requests in flight sends the next request as soon as its response arrives. Throughput is what the server can sustain at that concurrency.

Latency is measured from when the request was sent. A server that stalls holds up the requests in flight, so fewer requests are sent during the stall and few slow ones are recorded. Flat-out latency is fine for comparing steps, but it understates the tail. Use `--rate` for latency you can quote.

### Fixed rate

Request *i* is due at *start* + *i* / *rate*, spread evenly between the threads. Each time a thread's event loop wakes, it sends every request that is due, so a late wake-up doesn't push the schedule back.

Latency is measured from when the request was due, not when it was sent, as in wrk2. If the server stalls, every request due during the stall records the time it really waited.

`-c` caps connections per thread (`max_connections`). Requests over the cap wait in httpperf's queue, and that wait counts in their latency.

If the server can't keep up, the queue grows without limit. A step stops early, marked **over capacity**, when more than one second's worth of requests are queued. Later steps in a `--rate` list are skipped.

The schedule is only as precise as the event loop's timers, about ±1 ms. Sub-millisecond latencies at a fixed rate are not meaningful.

## Errors

A request is an error if:

- it fails (connection refused, reset, timeout), or
- the response status is 400 or above.

Errors are counted and reported by kind (`503`, `timeout`, `connection refused`, ...). They are not included in latency.

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

```
httpperf 1.0 (libcurl 8.14.1), 2 threads, http://localhost:8080/
flat out, 10s per step after 1s warm-up

  connections      req/s   ±/s     p50      p99   p99.9     max   errors
           10     48,210    1%   0.19ms   0.41ms  0.88ms  3.1ms        0
           50    101,544    2%   0.45ms   1.20ms  2.30ms  9.8ms        0
          200    104,980    3%   1.80ms   4.10ms  7.70ms   21ms        0  ← best
         1000    103,112    9%   9.20ms     19ms    44ms  130ms        0

best: 200 connections, 104,980 req/s
  latency      p50 1.80ms  p90 2.60ms  p99 4.10ms  p99.9 7.70ms  max 21ms
    first byte p50 1.70ms  p99 3.90ms
    download   p50 0.05ms  p99 0.20ms
  requests     1,049,800 in 10.0s, 0 errors
  transfer     received 152 MB/s, sent 9.1 MB/s
  connections  200 opened, 200 open at the end, HTTP/1.1, 0 TLS handshakes
  client       1.7 of 2 cores, 16µs CPU per request
               client lag p99 0.08ms, connection wait p99 0ms
```

A step with one value for `-c` or `--rate` prints only the second part.

| Line | Meaning |
|---|---|
| req/s, ±/s | Requests completed per second over the measured period, and how much the per-second counts varied (coefficient of variation). |
| latency | Flat out: from send to response. Fixed rate: from when the request was due to response. |
| first byte, download | curl's `starttransfer_time`, and `total_time` minus it. A slow server shows up in first byte. A large body or slow network shows up in download. |
| requests | Requests completed in the measured period, and errors by kind. |
| transfer | `bytes_received` and `bytes_sent` per second: headers and bodies, not TLS or TCP overhead. |
| connections | Connections opened during the step, open at the end, HTTP versions used, TLS handshakes. Connect and TLS times are added when connections were opened during measurement. |
| client | CPU used by the httpperf process, as cores and per request. `client_lag` p99: how late Python was handling responses. `queue_time` p99: how long requests waited for a free connection. |

For fixed rate, the step table has columns for the target rate and the achieved rate, and `best` is the highest rate that kept up.

`--json` prints one object per step with every number above, unrounded, and the latency percentiles from p50 to max.

## Warnings

Printed under the report when they apply:

- **Client saturated**: the process used more than 90% of the cores it was allowed, or `client_lag` p99 was more than 10% of latency p99. The result describes the client, not the server. Add `--threads`, simplify the script, or run the client on another machine.
- **Errors**: more than 1% of requests failed. The most common kinds are listed.
- **Unsteady**: req/s varied more than 10% from second to second. Run for longer, or check the server.
- **Connection churn**: connections were opened during measurement, so the server or a proxy is closing them. Latency includes new connections.
- **Behind schedule** (fixed rate): achieved rate was under 99% of the target.
- **Over capacity** (fixed rate): the step stopped early because requests piled up.
- **Peak not reached** (flat out list): the best step was the last. Try more connections.

## Exit status

| Code | Meaning |
|---|---|
| 0 | All steps ran without errors. |
| 1 | Some requests failed, or a step was over capacity. |
| 2 | Bad arguments, a broken script, or the server couldn't be reached at all. |

Ctrl-C stops the current step and prints the results of the steps that finished.

## Tips

- Use the server's own machine, or one close to it, and pin the client and server to separate cores (`taskset`). Otherwise they compete and both numbers are wrong.
- Watch the client line. If it says the client is saturated, more threads usually help until you run out of cores.
- uvloop is used if installed.
- For a test that needs more than one kind of request, or users with state, write the load test with the library instead.

## Not in the first version

- Several URLs or scripted user journeys with setup and teardown.
- Random (Poisson) arrivals for `--rate`.
- A latency target, such as "the highest rate with p99 under 50 ms".
- Pre-built requests to save Python work per request.

## Implementation notes

- Latency is recorded by `session.set_response_callback` into a log-linear histogram (HdrHistogram-style: percentiles accurate to about 1%) per thread, merged at the end. Written in Python first. If its overhead per request is significant, add a small C helper to `_httpperf` for the CLI only.
- `client_lag`, `queue_time`, first byte and download times go into histograms the same way.
- Engine `stats()` is sampled every second for the per-second req/s, transfer and connection counts.
- CPU comes from `resource.getrusage` and `os.sched_getaffinity`.
- Measure the CLI's own CPU per request against `bench/client.py` before settling the design.
