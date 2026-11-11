# acurl

An asyncio HTTP client for load testing, built on libcurl.

## Design

A load-testing client has three jobs: drive as much load as possible from each machine, report the server's response times rather than time spent waiting on the client, and read responses promptly so the server never sees a slow reader. acurl is built around these.

All network I/O runs in a small C event loop (the vendored `ae` reactor from Redis) on its own thread, which never takes the GIL. Python hands requests to that thread and collects finished responses through pipes:

```
asyncio loop ──req_in pipe──▶ curl thread (ae + curl multi) ──▶ sockets
     ▲                               │
     └──────── req_out pipe ◀────────┘
```

Bytes are read off the wire as they arrive, whatever Python is doing. If Python falls behind, finished responses wait in the pipe rather than in socket buffers, so the client simply sends new requests more slowly, and curl's timings stay accurate. The cost is the hand-off itself, and the curl thread uses roughly one extra core under load. That is deliberate: a spare core is cheap, and response times you can't trust are worthless.

`Response.client_lag` shows how far Python is behind (see [Timings](#timings)).

## Install

acurl needs the libcurl development headers and a C compiler. It supports Python 3.8 to 3.14.

```sh
sudo apt install libcurl4-openssl-dev   # Debian/Ubuntu
pip install .
```

## Quick start

```python
import asyncio

import acurl


async def main():
    async with acurl.EventLoop() as el:
        async with el.session() as session:
            r = await session.get("https://example.com/")
            print(r.status_code, r.http_version, r.total_time, r.client_lag)


asyncio.run(main())
```

One `EventLoop` per process (or per asyncio loop) is normal. Sessions are cheap: create one per virtual user or journey.

## API

### `EventLoop(loop=None, same_thread=False, max_host_connections=0, max_total_connections=0, max_idle_connections=1000)`

Starts the curl thread and connects it to an asyncio loop (the running loop if `loop` is not given).

| Argument | Meaning |
|---|---|
| `same_thread` | Run the curl loop on the asyncio thread instead, polling every millisecond. Only for debugging and profiling with Python tools. |
| `max_host_connections` | Process-wide cap on connections per host for sessions using the shared pool or no pool. 0 means no limit. curl queues requests above it. |
| `max_total_connections` | Process-wide cap on all connections for the same sessions. 0 means no limit. |
| `max_idle_connections` | How many connections each pool keeps (the shared pool, and each session's own pool). Must be at least 1. |

`el.stop()` stops the curl thread. Requests still in flight are abandoned. `with` and `async with` call `stop()` on exit.

### `el.session(...)`

| Argument | Default | Meaning |
|---|---|---|
| `connections` | `"shared"` | `"shared"`: the process-wide pool. `"session"`: the session's own pool. `"none"`: a new connection for every request. |
| `tls` | `"shared"` | Where TLS session tickets are kept. `"shared"`: process-wide, so new connections resume their handshake. `"session"`: per session, so each session starts with a full handshake. |
| `http_version` | `"HTTP/2"` | `"HTTP/2"`: HTTP/2 over TLS when the server offers it, with requests waiting to share one multiplexed connection. Plain http uses HTTP/1.1. `"HTTP/1.1"`: HTTP/1.1 only. |
| `idle_timeout` | `118` | Seconds a connection may sit idle and still be reused. |
| `max_connection_age` | `0` | Seconds after which a connection is no longer reused, however busy it is. 0 means no limit. |
| `max_host_connections` | `0` | Per-session cap on connections per host. Needs `connections="session"` or `"none"`. |
| `max_connections` | `0` | Per-session cap on connections in use. Needs `connections="session"` or `"none"`. |
| `cert` | `None` | `(certfile, keyfile)` paths for a client certificate, used by every request unless the request overrides it. |

Cookies always belong to the session, whatever the connection settings. See [docs/connections.md](docs/connections.md) for how these fit together.

### Session methods

- `await session.get(url, **kwargs)`, and likewise `post`, `put`, `patch`, `delete`, `head` and `options`.
- `await session.request(method, url, **kwargs)`.
- `session.close()`: closes the session's connections once its requests finish. Later requests raise `RuntimeError`. `with` and `async with` call it on exit.
- `session.cookies()`: the cookie jar as a dict.
- `await session.get_cookie_list()`, `await session.add_cookie_list(cookies)`, `await session.erase_all_cookies()` and `await session.erase_session_cookies()`: work with `Cookie` objects.
- `session.set_response_callback(callback)`: called with each `Response`, including each hop of a redirect.

### Request arguments

| Argument | Meaning |
|---|---|
| `headers` | Dict of headers. |
| `headers_list` | List of `"Name: value"` strings, for repeated headers. |
| `cookies` | Dict of cookies to send with this request (added to the session jar). |
| `cookie_list` | List of `Cookie` objects. |
| `auth` | `(username, password)` for basic auth. |
| `data` | Request body, `str` or `bytes`. |
| `json` | Object to send as JSON. Sets `Content-Type: application/json` unless you set it. |
| `cert` | `(certfile, keyfile)`, overriding the session's certificate. |
| `allow_redirects` | Follow redirects (default `True`; `False` for `head`). |
| `max_redirects` | Default 5. Raises `RequestError("Max Redirects")` beyond it. |

Failed transfers raise `acurl.RequestError` with curl's error message.

### Response

| Attribute | Meaning |
|---|---|
| `status_code` | HTTP status. |
| `url` | Effective URL. |
| `redirect_url` | Location of a redirect, or `None`. |
| `history` | Earlier responses in a redirect chain. |
| `http_version` | `"HTTP/1.0"`, `"HTTP/1.1"`, `"HTTP/2"` or `"HTTP/3"`. |
| `headers` | Dict of response headers. `headers_tuple` keeps repeats and order; `header` is the raw header block. |
| `body` | Bytes. |
| `text` | Body decoded with `encoding`. |
| `encoding` | From the `Content-Type` charset, else latin-1 (`utf-8` for JSON). Can be set. |
| `json()` | Body parsed as JSON (always UTF-8, as RFC 8259 requires). |
| `cookies`, `cookielist` | The session's cookie jar at the time you read it (not only the cookies this response set). Empty once the session is closed. |
| `primary_ip` | Address connected to. |
| `upload_size`, `download_size` | Bytes, as integers. |
| `request` | The `Request`: `method`, `url`, `headers`, `cookies`, `auth`, `data`. |

## Timings

All in seconds.

| Attribute | Meaning |
|---|---|
| `start_time`, `end_time` | Wall-clock times (`time.time()`) when the request was made and when Python received the response. |
| `namelookup_time` | DNS, measured by curl from the start of the transfer. |
| `connect_time` | TCP connect complete. 0 when the connection was reused. |
| `appconnect_time` | TLS handshake complete. 0 for plain http and reused connections. |
| `pretransfer_time` | Ready to send. |
| `starttransfer_time` | First byte received. |
| `total_time` | Transfer complete. |
| `queue_time` | Time waiting in acurl's per-session queue for a connection (see `max_host_connections`). 0 if the request did not wait. |
| `client_lag` | `end_time − start_time − total_time − queue_time`: time the request spent waiting on Python, before it was sent and after it finished. |

curl measures its timings on the curl thread, so a busy Python process does not inflate them. `client_lag` is how you see that Python is the bottleneck: if it grows, the client cannot keep up, and achieved request rates describe the client rather than the server. Report it alongside response times (p50, p99, max).

`queue_time` is real waiting that a user would see (a browser's "stalled" time), but it is client-side, so keep it out of server latency.

## Known limitations

- TLS certificates are never verified.
- `get_cookie_list`, `add_cookie_list` and the erase methods make a round trip through the curl thread. `session.cookies()` does not.
- curl closes expired idle connections lazily, the next time the pool needs a connection, so the server sees the close late. See [docs/connections.md](docs/connections.md).
- Requests in flight when `EventLoop.stop()` is called never complete.
- HTTP/3 is not supported yet, though libcurl can do it.

## Development

```sh
uv venv -p 3.13 .venv
uv pip install -e . --group dev
make lint               # ruff and clang-format, check only
make lint-fix           # apply formatting and fixes
.venv/bin/python -m pytest
```

The tests run local servers: pytest-httpbin, small threaded `http.server` servers for connection counting and TLS, and hypercorn for HTTP/2. Client certificates are generated with `openssl`. The httpbin server is single-threaded, so keep concurrency in tests low.

Set `ACURL_PROFILE=1` when building to instrument the extension. See [docs/profiling.md](docs/profiling.md).
