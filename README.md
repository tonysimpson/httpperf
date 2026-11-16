# httpperf

An asyncio HTTP client for load testing, built on libcurl.

## Design

A load-testing client has three jobs: drive as much load as possible from each machine, report the server's response times rather than time spent waiting on the client, and read responses promptly so the server never sees a slow reader. httpperf is built around these.

All network I/O runs in a small C event loop (the vendored `ae` reactor from Redis) on its own thread, which never takes the GIL. Python hands requests to that thread and collects finished responses through pipes:

```
asyncio loop ──req_in pipe──▶ curl thread (ae + curl multi) ──▶ sockets
     ▲                               │
     └──────── req_out pipe ◀────────┘
```

Bytes are read off the wire as they arrive, whatever Python is doing. If Python falls behind, finished responses wait in the pipe rather than in socket buffers, so the client simply sends new requests more slowly, and curl's timings stay accurate. The cost is the hand-off itself, and the curl thread uses roughly one extra core under load. That is deliberate: a spare core is cheap, and response times you can't trust are worthless.

`Response.client_lag` shows how far Python is behind (see [Timings](#timings)).

## Install

```sh
pip install httpperf
```

Wheels are published for Linux on x86_64 and aarch64, for Python 3.9 to 3.15 and free-threaded 3.14 and 3.15 (`python3.14t`, `python3.15t`), with glibc 2.34 or later (Ubuntu 22.04, Debian 12, RHEL 9 and later) or musl (Alpine). They include their own libcurl, with HTTP/2 and HTTP/3.

Anywhere else, including Python 3.8, pip builds from source against the system's libcurl, which needs its development headers and a C compiler. That libcurl decides whether HTTP/3 is available: `curl -V` lists `HTTP3` under Features when it does.

```sh
sudo apt install libcurl4-openssl-dev   # Debian/Ubuntu
pip install httpperf
```

To build against your own libcurl even where a wheel exists, skip the wheel, point the build at it and make sure it is found at run time:

```sh
CFLAGS=-I$PREFIX/include LDFLAGS="-L$PREFIX/lib -Wl,-rpath,$PREFIX/lib" pip install --no-binary httpperf httpperf
```

A session with `http_version="HTTP/3"` raises `ValueError` if the libcurl in use lacks HTTP/3.

## Quick start

```python
import asyncio

import httpperf


async def main():
    async with httpperf.Session() as session:
        r = await session.get("https://example.com/")
        print(r.status_code, r.http_version, r.total_time, r.client_lag)


asyncio.run(main())
```

Sessions are cheap: create one per virtual user or journey. They share a default `Engine` (the curl thread, the shared connection pool and the TLS cache) unless you pass your own.

## Command line

`python -m httpperf` is a load generator in the spirit of wrk and wrk2. Find the most a server can handle by trying more requests in flight:

```
python -m httpperf -c 10,50,200,1000 http://localhost:8080/
```

Then measure latency at fixed rates below that peak:

```
python -m httpperf --rate 20000,40000,60000 http://localhost:8080/
```

Each step reports requests per second, latency percentiles, errors and transfer rates, and warns when the client rather than the server was the limit. A short Python script can define the request to make. See [docs/cli.md](docs/cli.md) for all the options.

## API

### `Session(*, engine=None, ...)`

All arguments are keyword-only. A session can be created anywhere, even outside a running asyncio loop.

| Argument | Default | Meaning |
|---|---|---|
| `engine` | `None` | The `Engine` to use. `None` means the default engine of the asyncio loop the session is first used in, created when first needed and shared by every session in that loop without one. |
| `connections` | `"shared"` | `"shared"`: the engine's pool. `"session"`: the session's own pool. `"none"`: a new connection for every request. |
| `tls` | `"shared"` | Where TLS session tickets are kept. `"shared"`: in the engine, so new connections resume their handshake. `"session"`: per session, so each session starts with a full handshake. |
| `http_version` | `"HTTP/2"` | `"HTTP/2"`: HTTP/2 over TLS when the server offers it, with requests waiting to share one multiplexed connection. Plain http uses HTTP/1.1. `"HTTP/1.1"`: HTTP/1.1 only. `"HTTP/3"`: HTTP/3 over QUIC only, with no fallback, so requests fail if the server does not offer it. |
| `idle_timeout` | `118` | Seconds a connection may sit idle and still be reused. 0 means connections are never reused. |
| `max_connection_age` | `0` | Seconds after which a connection is no longer reused, however busy it is. 0 means no limit. |
| `max_host_connections` | `0` | Per-session cap on connections per host. Needs `connections="session"` or `"none"`. |
| `max_connections` | `0` | Per-session cap on connections in use. Needs `connections="session"` or `"none"`. |
| `cert` | `None` | `(certfile, keyfile)` paths for a client certificate, used by every request unless the request overrides it. |
| `timeout` | `None` | Seconds allowed for each request, from the start of the transfer to the last byte (each redirect hop separately). `None` or 0 means no limit. Cancelling the task (or `asyncio.wait_for`) also stops the transfer and frees its connection slot; on HTTP/1.1 that closes the connection, on HTTP/2 and HTTP/3 it resets only that stream. |
| `connect_timeout` | `None` | Seconds allowed to connect, TLS included. `None` means curl's default of 300 s. |
| `verify` | `True` | Server certificate checking. `False`: none, for test servers with self-signed certificates. `True`: check against the system CAs, from the first of: `SSL_CERT_FILE` and `SSL_CERT_DIR`, if set and present; libcurl's own default bundle, if present; Python's default bundle (`ssl.get_default_verify_paths()`); the usual system locations. A path: check against that CA bundle. Bytes: check against the PEM CA certificates they hold, for certificates kept in memory or a secret store. Requests can override it. |
| `json_dumps` | `None` | Function that turns a `json=` argument into the request body (`str` or `bytes`). `None` means the engine's. |
| `json_loads` | `None` | Function that turns the response body (`bytes`) into the result of `json()`. `None` means the engine's. |

Proxies come from the environment, as curl reads them: `http_proxy` (not `HTTP_PROXY`, which curl ignores for security), `https_proxy` or `HTTPS_PROXY`, then `all_proxy` or `ALL_PROXY`, with `no_proxy` or `NO_PROXY` for exceptions. A session reads them once, when it is created, so later changes apply to new sessions only.

Cookies always belong to the session, whatever the connection settings.

### `Engine(*, max_host_connections=0, max_total_connections=0, max_idle_connections=1000, buffer_size=16384, host_stats=False, json_dumps=..., json_loads=...)`

What sessions share: the curl thread, the shared connection pool, the TLS session cache and the limits. Most programs never create one. Create one to set limits, or to keep groups of sessions in separate pools, and pass it to `Session(engine=...)`.

| Argument | Meaning |
|---|---|
| `max_host_connections` | Cap on connections per host for the engine's sessions that use the shared pool or no pool. 0 means no limit. httpperf queues requests above it and reports the wait as `queue_time`. |
| `max_total_connections` | Cap on all connections for the same sessions. 0 means no limit. |
| `max_idle_connections` | How many connections each pool keeps (the shared pool, and each session's own pool). Must be at least 1. |
| `buffer_size` | Bytes curl reads from a connection at a time, from 1024 to 10 MB. The default, curl's own, suits many small responses. For large downloads 256 KB is much faster (here 14 to 23 GB/s with `discard_body`, 10 to 15 GB/s streamed), but each transfer in flight has its own buffer, so it costs memory at high connection counts. |
| `host_stats` | `True` keeps counts for each host, read with `engine.host_stats()`. See [Statistics](#statistics). |
| `json_dumps`, `json_loads` | The JSON functions for the engine's sessions, unless a session sets its own. The defaults use the standard library: compact output, and bodies decoded as UTF-8. See [JSON](#json). |

Each asyncio loop has its own default engine, so sessions without an engine work in any thread, and in each `asyncio.run()`, without setup. A loop's default engine lasts as long as the loop, and is closed once the loop has been closed and no session uses it.

An engine binds to the asyncio loop running when it is first used, and so does a session. An engine serves one loop at a time, and using it from a second live loop raises `RuntimeError`. Once its loop is closed, as at the end of `asyncio.run()`, it binds to the next one and keeps its pool. So a session kept between `asyncio.run()` calls keeps its connections. To run loops in several threads at once with your own engines, give each thread its own `Engine`.

On free-threaded Python (3.14t and later), httpperf runs without the GIL, so threads that each run their own loop and engine run their Python code in parallel. That is what helps when Python work on each response is the limit, as it usually is in a real load test. On 4 cores, with 20 µs of Python work per response, 4 threads went from 42k requests per second with the GIL to 111k without it; with 100 µs of work, from 9.6k to 34k. With little Python work per response the GIL costs little, since the curl threads never hold it.

`engine.close()` stops the curl thread and closes the shared pool's connections, and later requests raise `RuntimeError`. Requests still in flight fail with `RequestError`, and a `ResourceWarning` says how many there were. `with` and `async with` call `close()` on exit. An engine that is no longer referenced is closed automatically. See [docs/connections.md](docs/connections.md) for how these fit together.

`engine.stats()` gives the counts for all the engine's sessions. See [Statistics](#statistics).

Each engine has one curl thread. When the Python code does real work on each response, Python is the limit and one engine is plenty. A simple load driver that does little per response can make the curl thread the limit instead. Several engines can share one asyncio loop, so spreading sessions over them drives more throughput:

```python
engines = [httpperf.Engine() for _ in range(4)]
sessions = [httpperf.Session(engine=engines[i % len(engines)]) for i in range(100)]
```

Each engine has its own connections and limits. On 4 cores against a local server, 4 engines took a closed loop of 100 requests from 104k to 223k requests per second for small responses, and from 60k to 183k for 100 KB ones, at which point the Python thread was the limit.

### Session methods

- `await session.get(url, **kwargs)`, and likewise `post`, `put`, `patch`, `delete`, `head` and `options`.
- `await session.request(method, url, **kwargs)`.
- `session.close()`: closes the session's connections once its requests finish. Later requests raise `RuntimeError`. `with` and `async with` call it on exit.
- `session.cookies()`: the cookie jar as a dict.
- `session.get_cookie_list()`, `session.add_cookie_list(cookies)`, `session.erase_all_cookies()` and `session.erase_session_cookies()`: work with `Cookie` objects. These are ordinary methods, not coroutines.
- `session.set_response_callback(callback)`: called with each `Response`, including each hop of a redirect.
- `session.stats()`: the session's request and connection counts. See [Statistics](#statistics).
- `session.engine`: the session's `Engine`.

### Request arguments

| Argument | Meaning |
|---|---|
| `params` | Query parameters: a dict or list of pairs (lists as values repeat the key), or a ready-made string. Added to any query already in the URL. `None` values are left out. |
| `headers` | Dict of headers. `None` values are left out. |
| `headers_list` | List of `"Name: value"` strings, for repeated headers. |
| `cookies` | Dict of cookies to send with this request (added to the session jar). |
| `cookie_list` | List of `Cookie` objects. |
| `auth` | `(username, password)` for basic auth. |
| `data` | Request body: `str` or `bytes` as is, with no `Content-Type` unless you set one, or a dict or list of pairs, form-encoded with `Content-Type: application/x-www-form-urlencoded` unless you set it, leaving out `None` values. A POST, PUT or PATCH without a body sends `Content-Length: 0`. A file, or another iterable of `bytes` (sync or async), is streamed: see [Large bodies](#large-bodies). |
| `json` | Object to send as JSON, encoded with the session's `json_dumps`. Sets `Content-Type: application/json` unless you set it. |
| `files` | Dict (or list of pairs) of files to send as `multipart/form-data`, with any `data` dict as extra fields. A value is an open file, or `(filename, content)` or `(filename, content, content_type)`, where content is `bytes`, `str` or a file. Files are read into memory. |
| `cert` | `(certfile, keyfile)`, overriding the session's certificate. |
| `verify` | `True`, `False`, a CA bundle path or PEM CA certificates as bytes, overriding the session's `verify`. |
| `timeout`, `connect_timeout` | Override the session's. `timeout=0` removes the session's limit for this request. |
| `allow_redirects` | Follow redirects (default `True`; `False` for `head`). |
| `max_redirects` | Default 5. Raises `RequestError("Max Redirects")` beyond it. |

Redirects follow the same rules as requests. A 303 turns any method but HEAD into a GET, and a 301 or 302 turns a POST into a GET; the body and its `Content-Type`, `Content-Length` and `Transfer-Encoding` headers are then dropped. Other redirects keep the method and body. Other headers are kept, except that `Authorization` and `auth` are dropped when the redirect leaves the origin: another scheme, host or port.
| `discard_body` | `True` drops the response body as it arrives, for when only the status, headers or timings matter. See [Large bodies](#large-bodies). |
| `stream` | `True` returns as soon as the headers arrive, and the body is read with `iter_content()` or `read()`. See [Large bodies](#large-bodies). |

Request bodies are sent without `Expect: 100-continue`, as in requests, unless you set that header. curl would otherwise add it to bodies over 1 MB and wait up to a second for the server to reply `100 Continue` before sending the body.

Failed transfers raise `httpperf.RequestError` with curl's detailed error message, such as `Failed to connect to 127.0.0.1 port 8080 after 0 ms: Couldn't connect to server`. Its `code` is curl's error code. Timeouts raise `httpperf.Timeout`, a subclass, so they can be counted separately.

### Large bodies

By default the whole response body is kept in memory. For large downloads there are two other ways.

`discard_body=True` drops the body on the curl thread as it arrives. Use it when only the status, headers or timings matter; it is the fastest way:

```python
r = await session.get(url, discard_body=True)
r.status_code, r.download_size, r.total_time
```

`r.body` is then `None`, and `r.text` and `r.json()` raise `ValueError`. `r.download_size` gives the body's size. When a redirect is followed, every response's body is dropped, including those in `history`.

`stream=True` works as in requests, with `async for`. The request returns once the headers arrive, and the body is read as it comes:

```python
with open("big.iso", "wb") as f:
    async with await session.get(url, stream=True) as r:
        async for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)
```

- `iter_content(chunk_size)` gives what has arrived, in chunks of at most `chunk_size` bytes. It doesn't wait to fill a chunk, so chunks are often smaller. Without a size, each chunk is everything that has arrived since the last one. `await r.read()` reads the whole body into `r.body`.
- `r.body`, `r.text` and `r.json()` raise `RuntimeError` until the body has been read, as do the timings and sizes, since they aren't known until the transfer ends.
- httpperf buffers up to 1 MB of each response. When Python falls behind, it stops reading that connection until Python catches up, so the server is held up by TCP back-pressure. Other requests carry on, and the curl thread never waits. The time spent stopped is `r.paused_time`.
- Closing the response, leaving `async with`, breaking out of `iter_content()` or dropping the response stops the transfer and closes its connection. Reading to the end leaves the connection open for reuse.
- A transfer that fails part way, say because the connection drops, raises `RequestError` from `iter_content()` or `read()`.
- Redirects are followed as usual, and each redirect's body is read into its `history` entry.

Uploads stream the same way. Pass an open file, or any object with `read()`, as `data`, or an iterable or async iterable of `bytes`:

```python
with open("big.iso", "rb") as f:
    r = await session.put(url, data=f)
```

- A regular file or a `BytesIO` is sent with a `Content-Length`, as is anything when you set a `Content-Length` header. Anything else is sent chunked (on HTTP/1.1).
- httpperf reads ahead up to 1 MB. When the source falls behind, curl pauses that transfer until there is more to send; that time is in `r.paused_time` too.
- An exception from the source, such as a failing `read()`, stops the transfer and is raised from the request.
- A streamed upload is never sent twice. If a reused connection turns out to have been closed by the server, the request raises `RequestError`, and it's up to the caller to retry (see below). Bodies given as `bytes` are resent by curl, which it does for any request whose reused connection had closed.
- A redirect that keeps the body (see [Request arguments](#request-arguments)) sends it again: a seekable file is rewound to where it started, and anything else raises `RequestError`.

To retry a streamed upload, open the source again for each attempt:

```python
async def put_file(session, url, path, attempts=2):
    for attempt in range(attempts):
        with open(path, "rb") as f:
            try:
                return await session.put(url, data=f)
            except httpperf.RequestError:
                if attempt == attempts - 1:
                    raise
```

`RequestError` includes `Timeout`; put an `except httpperf.Timeout: raise` first if a slow server shouldn't be retried. In a benchmark, count the failed attempts too: each one is a real error, even if the retry succeeded.

Streaming is for benchmarking large transfers, such as how fast a server sends or receives a big file, and for checking what arrives. Each chunk passes through Python, so it can't sustain high request rates. For those, use `discard_body=True` or keep the body in memory.

### Response

| Attribute | Meaning |
|---|---|
| `status_code` | HTTP status. |
| `ok` | `True` if the status is below 400. |
| `reason` | Reason phrase from the status line, or the standard phrase for HTTP/2 and HTTP/3, which send none. |
| `raise_for_status()` | Raises `httpperf.HTTPError` (a `RequestError`, with the `response`) for 4xx and 5xx statuses. |
| `elapsed` | `total_time` as a `timedelta`. |
| `url` | Effective URL. |
| `redirect_url` | Location of a redirect, or `None`. |
| `history` | Earlier responses in a redirect chain. |
| `http_version` | `"HTTP/1.0"`, `"HTTP/1.1"`, `"HTTP/2"` or `"HTTP/3"`. |
| `headers` | Case-insensitive mapping of the response headers. Repeated headers are joined with `, `, as in requests. `headers_tuple` keeps repeats and order. Both skip interim responses such as `100 Continue`. `header` is the raw text of every header block, decoded as latin-1. |
| `body` | Bytes, or `None` if the request used `discard_body`. |
| `iter_content(chunk_size=None)`, `read()`, `close()` | Read a streamed body in chunks or whole, or stop it. See [Large bodies](#large-bodies). |
| `text` | Body decoded with `encoding`. |
| `encoding` | From the `Content-Type` charset, else latin-1 (`utf-8` for JSON). Can be set. |
| `json()` | Body parsed with the session's `json_loads`, once. By default the standard library, always as UTF-8, as RFC 8259 requires. |
| `cookies`, `cookielist` | The cookies this response set, from its `Set-Cookie` headers, as a dict or as `Cookie` objects. The session's whole jar is `session.cookies()`. |
| `primary_ip` | Address connected to. |
| `upload_size`, `download_size` | Bytes, as integers. |
| `request` | The `Request`: `method`, `url`, `headers`, `cookies`, `auth`, `data`. |

### JSON

By default httpperf uses the standard library's `json`: compact output for `json=`, and response bodies decoded as UTF-8 for `json()`. It needs no extra dependency and has the most scrutinised parser, which matters because load tests often fetch responses from third parties.

If JSON costs too much CPU, pass faster functions as `json_dumps` and `json_loads`, on the `Engine` for all its sessions or on one `Session`. [orjson](https://github.com/ijl/orjson) is a good choice for encoding, and [pysimdjson](https://github.com/TkTech/pysimdjson) for decoding. Create the parser once, at module scope, since it keeps its buffers between documents:

```python
import httpperf
import orjson
import simdjson

parser = simdjson.Parser()
engine = httpperf.Engine(json_dumps=orjson.dumps, json_loads=parser.parse)


async def journey():
    async with httpperf.Session(engine=engine) as session:
        r = await session.post("https://example.com/search", json={"q": "httpperf"})
        return r.json()["items"][3]["name"]
```

Things to know:

- orjson is about 8 times faster than `json` on larger payloads, but turns `NaN` into `null` and refuses integers over 64 bits.
- `parser.parse` is lazy: only the values you touch become Python objects. That was about 8 times faster than `json.loads` for reading one field of a 500 KB document. The result is read-only and not a real `dict` or `list`; `.as_dict()` and `.as_list()` convert it. Documents stay valid after the parser moves on to the next one.
- To get real `dict`s and `list`s from simdjson, use `json_loads=lambda body: parser.parse(body, True)`.
- Request bodies that never change can be encoded once and sent as `data=`, with `headers={"Content-Type": "application/json"}`.

## WebSockets

`httpperf.websockets` follows the asyncio API of the [websockets](https://websockets.readthedocs.io/) library, so most code only needs a different import:

```python
from httpperf import websockets

async with websockets.connect("wss://example.com/stream", additional_headers={"Authorization": "Bearer x"}) as ws:
    await ws.send("hello")  # str is sent as text, bytes as binary, an iterable as fragments
    message = await ws.recv()
    async for message in ws:  # ends when the server closes normally
        ...
```

`connect()` can also be awaited: `ws = await websockets.connect(url)`, then `await ws.close()`.

### WebSockets on a session

By default each connection gets its own session on the event loop's default engine. Pass `session=` to open it on a session of your own instead. It then uses that session's engine, its cookies, its client certificate and `verify`, and its `connect_timeout`; the session's request timeout doesn't apply, so the connection can stay open. A common use is logging in over HTTP and then opening WebSockets with the cookies that set:

```python
async with httpperf.Session() as session:
    await session.post("https://example.com/login", data={"user": "u", "password": "p"})
    async with websockets.connect("wss://example.com/stream", session=session) as ws:
        ...
```

A WebSocket holds its connection for as long as it's open, and that connection counts towards the engine's and session's connection limits. With `max_host_connections=10`, ten open WebSockets to a host leave no room for HTTP requests to it, which then wait. The handshake may reuse an idle HTTP/1.1 connection from the pool, but once upgraded the connection belongs to the WebSocket and is closed with it. In `stats()`, an open WebSocket shows as `running`; its request, connection and bytes are counted when it closes.

| `connect()` argument | Meaning |
|---|---|
| `additional_headers` | Headers for the handshake: a dict or list of pairs. |
| `subprotocols` | Subprotocols to offer; the one chosen is `ws.subprotocol`. |
| `origin`, `user_agent_header` | `Origin` and `User-Agent` headers. |
| `ssl` | An `ssl.SSLContext`. Its CA certificates and whether it checks certificates are used; anything else in it, such as a client certificate, isn't. |
| `verify` | As for a session: `True`, `False`, a CA bundle path or PEM CA certificates as bytes. The default checks certificates. |
| `open_timeout` | Seconds allowed for the connection and handshake (default 10). Raises `TimeoutError`. |
| `ping_interval`, `ping_timeout` | Keepalive: a ping every 20 s, and the connection is closed with 1011 if its pong takes over 20 s. `None` turns either off. The server's pings are always answered. |
| `close_timeout` | Seconds to wait for the closing handshake to finish (default 10): for the server's close frame after `close()`, and for the server to drop the connection after it closes. Then the connection is dropped. |
| `max_size` | Largest message accepted, in bytes (default 1 MiB, `None` for no limit). A larger one closes the connection with 1009. |
| `max_queue` | Messages kept waiting for `recv()` (default 16) before reading from the server pauses. |
| `session` | An httpperf `Session` to open the connection on: see [WebSockets on a session](#websockets-on-a-session). By default each connection has its own session. |
| `compression`, `logger` | Accepted for compatibility and ignored: see below. |

The connection has `send()`, `recv()`, `close(code=1000, reason="")`, `wait_closed()`, `ping()` (returns a waiter that gives the round-trip time when the pong arrives), `pong()`, and the attributes `latency`, `close_code`, `close_reason`, `subprotocol` and `response` (the handshake `Response`).

The exceptions have the same names and meanings as in websockets: `WebSocketException`, `ConnectionClosed` with `ConnectionClosedOK` (both sides closed with 1000, 1001 or no code) and `ConnectionClosedError` (anything else, including a dropped connection), `InvalidHandshake`, `InvalidStatus` (the server refused the upgrade; `response` is its response) and `InvalidURI`. A failure to connect raises `httpperf.RequestError`. `ConnectionClosed` has `rcvd` and `sent`, the close frames received and sent (each a `Close` with `code` and `reason`, or `None`), and `code` and `reason` from the frame received (1006 and "" if none was).

Differences from websockets:

- No compression. curl doesn't support WebSocket extensions, so `permessage-deflate` isn't offered, whatever `compression` says. Servers that require it refuse the connection.
- HTTP/1.1 only. curl doesn't do WebSockets over HTTP/2 or HTTP/3.
- No sync client, no reconnecting `async for ws in connect(...)`, and no server.
- Needs libcurl 8.16 or later with WebSocket support, which the wheels have. `httpperf._httpperf.websockets` is 1 when it's available; otherwise `connect()` raises `RuntimeError`.
- curl masks outgoing data one byte at a time, so sending large messages is slow (about 0.3 GB/s for each connection); receiving is fast.

## Timings

All in seconds.

| Attribute | Meaning |
|---|---|
| `start_time`, `end_time` | Wall-clock times (`time.time()`) when the request was made and when Python received the response. With `stream=True`, `end_time` is when Python received the last of the body. |
| `namelookup_time` | DNS, measured by curl from the start of the transfer. |
| `connect_time` | TCP connect complete. 0 when the connection was reused. |
| `appconnect_time` | TLS handshake complete. 0 for plain http and reused connections. |
| `pretransfer_time` | Ready to send. |
| `posttransfer_time` | Request fully sent. `starttransfer_time − posttransfer_time` is the server's time to first byte, without connection setup or upload. `None` with libcurl older than 8.10. |
| `starttransfer_time` | First byte received. |
| `total_time` | Transfer complete. |
| `queue_time` | Time waiting in httpperf's queue for a connection, under the engine or session limits. 0 if the request did not wait. curl's timings start after it. |
| `paused_time` | Time the transfer was stopped waiting on Python: to read a streamed body, or to supply more of a streamed upload. It is part of `total_time`. 0 otherwise. |
| `client_lag` | `end_time − start_time − total_time − queue_time`: time the request spent waiting on Python, before it was sent and after it finished. Measured with the monotonic clock, so clock changes don't affect it. |

curl measures its timings on the curl thread, so a busy Python process does not inflate them. `client_lag` is how you see that Python is the bottleneck: if it grows, the client cannot keep up, and achieved request rates describe the client rather than the server. Report it alongside response times (p50, p99, max).

`queue_time` is real waiting that a user would see (a browser's "stalled" time), but it is client-side, so keep it out of server latency.

## Statistics

`session.stats()` and `engine.stats()` return a dict of counts since the session or engine was created. The engine's counts cover all its sessions. Reading them is cheap, so a load test can sample them every second and report the differences as rates.

| Key | Meaning |
|---|---|
| `requests` | Requests that got a response, whatever its status. Each hop of a redirect counts. |
| `errors` | Requests that failed with `RequestError`, including timeouts. |
| `cancelled` | Requests cancelled before they finished. |
| `http_versions` | `requests` by HTTP version: a dict with keys `"HTTP/1.0"`, `"HTTP/1.1"`, `"HTTP/2"` and `"HTTP/3"`. |
| `connections` | New connections opened. A reused connection doesn't count. |
| `tls_handshakes` | New connections that did a TLS handshake. |
| `bytes_sent`, `bytes_received` | Application-layer bytes only: request and response headers and bodies, with bodies as transferred (so compressed if the server compressed them). TLS, HTTP/2 and HTTP/3 framing, and TCP/IP overheads aren't counted. |
| `running` | Requests with curl now. |
| `queued` | Requests now waiting for a connection under the engine or session limits. |
| `connections_open` | Connections open now, idle or in use, including any still connecting. For the engine, every pool. For a session with `connections="session"`, its own pool, and 0 once it is closed. `None` for other sessions, whose connections belong to the engine. |

The counts are updated on the curl thread, so they can lag a moment behind: a cancelled request is counted just after the cancel.

`engine.host_stats()` gives the same counts for each host the engine has sent requests to, keyed by origin, such as `"https://api.example.com:443"`. Different spellings of one host, such as an upper-case scheme or a default port left out, share an entry. `connections_open` is `None` per host. Host stats are off by default; turn them on with `Engine(host_stats=True)`. They cost nothing measurable, but every host is kept until the engine goes, so a client sending requests to a very large number of hosts grows by a few hundred bytes for each.

```python
engine = httpperf.Engine(host_stats=True)
...
for origin, counts in engine.host_stats().items():
    print(origin, counts["requests"], counts["errors"])
```

## Known limitations

- curl closes expired idle connections lazily, the next time the pool needs a connection, so the server sees the close late. See [docs/connections.md](docs/connections.md).
- Requests in flight when `Engine.close()` is called never complete.
- A gzip response body made of several gzip members fails with `RequestError`: curl decodes only the first member and treats the rest as an error. Servers rarely send them.
- With a system libcurl older than 8.12, `tls="shared"` doesn't share TLS sessions across the engine: curl keeps them per curl handle, so a new connection only resumes if it happens to reuse the handle of an earlier request to that host. The wheels bundle a newer curl.

## Development

```sh
make test
make lint               # ruff and clang-format, check only
make lint-fix           # apply formatting and fixes
make clean              # remove build output, caches and profiler output (keeps .venv)
make distclean          # also remove .venv
```

Each target first creates `.venv` with uv, if needed, and installs httpperf in editable mode with the dev tools. It reinstalls when `setup.py`, `pyproject.toml` or the C sources change, so C edits are rebuilt automatically. `make venv` does only that step.

`make venv PY_VERSION=3.8 VENV=.venv-3.8` builds another venv, and `VENV=.venv-3.8` on `make test` or `make lint` uses it.

`make ci-local` runs CI's test job with Podman: every Python version, in the manylinux_2_34 image against the curl the wheels bundle, four at a time, on a copy of the working tree. `PYTHONS="3.9 3.14t"` picks versions, `JOBS` sets how many run at once, and `PYTEST_ARGS` is passed to pytest. Logs go to `.ci-local/`. The first run builds curl into a local image (a few minutes); it's rebuilt only when `tools/build-libcurl.sh` changes.

To release, set `__version__` in `httpperf/version.py` (for example `1.0rc1` or `1.0`), commit and push to main, then run `make release`. It tags `v<version>` and pushes the tag, and the release workflow builds the wheels and publishes them to PyPI.

The tests run local servers: pytest-httpbin, small threaded `http.server` servers for connection counting and TLS, and hypercorn for HTTP/2 and HTTP/3 (skipped on Python 3.8, where hypercorn's HTTP/3 server hangs on shutdown). Client certificates are generated with `openssl`. The httpbin server is single-threaded, so keep concurrency in tests low.

Set `HTTPPERF_PROFILE=1` when building to instrument the extension. See [docs/profiling.md](docs/profiling.md).

## Licence

MIT. See [LICENSE](LICENSE). [NOTICE](NOTICE) covers the bundled Redis event library and the libraries included in the binary wheels.
