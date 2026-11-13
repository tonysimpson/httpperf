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

Wheels are published for Linux on x86_64 and aarch64, for Python 3.9 to 3.15, with glibc 2.34 or later (Ubuntu 22.04, Debian 12, RHEL 9 and later) or musl (Alpine). They include their own libcurl, with HTTP/2 and HTTP/3.

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

## API

### `Session(*, engine=None, ...)`

All arguments are keyword-only. A session can be created anywhere, even outside a running asyncio loop.

| Argument | Default | Meaning |
|---|---|---|
| `engine` | `None` | The `Engine` to use. `None` means the default engine of the asyncio loop the session is first used in, created when first needed and shared by every session in that loop without one. |
| `connections` | `"shared"` | `"shared"`: the engine's pool. `"session"`: the session's own pool. `"none"`: a new connection for every request. |
| `tls` | `"shared"` | Where TLS session tickets are kept. `"shared"`: in the engine, so new connections resume their handshake. `"session"`: per session, so each session starts with a full handshake. |
| `http_version` | `"HTTP/2"` | `"HTTP/2"`: HTTP/2 over TLS when the server offers it, with requests waiting to share one multiplexed connection. Plain http uses HTTP/1.1. `"HTTP/1.1"`: HTTP/1.1 only. `"HTTP/3"`: HTTP/3 over QUIC only, with no fallback, so requests fail if the server does not offer it. |
| `idle_timeout` | `118` | Seconds a connection may sit idle and still be reused. |
| `max_connection_age` | `0` | Seconds after which a connection is no longer reused, however busy it is. 0 means no limit. |
| `max_host_connections` | `0` | Per-session cap on connections per host. Needs `connections="session"` or `"none"`. |
| `max_connections` | `0` | Per-session cap on connections in use. Needs `connections="session"` or `"none"`. |
| `cert` | `None` | `(certfile, keyfile)` paths for a client certificate, used by every request unless the request overrides it. |
| `timeout` | `None` | Seconds allowed for each request, from the start of the transfer to the last byte (each redirect hop separately). `None` or 0 means no limit. Prefer this to `asyncio.wait_for` or cancelling the task: a cancelled request still runs to the end in curl and holds its connection until then, while a timeout stops it. |
| `connect_timeout` | `None` | Seconds allowed to connect, TLS included. `None` means curl's default of 300 s. |
| `verify` | `False` | Server certificate checking. `False`: none, as most load tests want. `True`: check against the system CA bundle: `SSL_CERT_FILE` if set, otherwise libcurl's own default, or the usual system locations if it has none. A path: check against that CA bundle. Requests can override it. |
| `json_dumps` | `None` | Function that turns a `json=` argument into the request body (`str` or `bytes`). `None` means the engine's. |
| `json_loads` | `None` | Function that turns the response body (`bytes`) into the result of `json()`. `None` means the engine's. |

Proxies come from the environment, as curl reads them: `http_proxy` (not `HTTP_PROXY`, which curl ignores for security), `https_proxy` or `HTTPS_PROXY`, then `all_proxy` or `ALL_PROXY`, with `no_proxy` or `NO_PROXY` for exceptions. A session reads them once, when it is created, so later changes apply to new sessions only.

Cookies always belong to the session, whatever the connection settings.

### `Engine(*, max_host_connections=0, max_total_connections=0, max_idle_connections=1000, json_dumps=..., json_loads=...)`

What sessions share: the curl thread, the shared connection pool, the TLS session cache and the limits. Most programs never create one. Create one to set limits, or to keep groups of sessions in separate pools, and pass it to `Session(engine=...)`.

| Argument | Meaning |
|---|---|
| `max_host_connections` | Cap on connections per host for the engine's sessions that use the shared pool or no pool. 0 means no limit. httpperf queues requests above it and reports the wait as `queue_time`. |
| `max_total_connections` | Cap on all connections for the same sessions. 0 means no limit. |
| `max_idle_connections` | How many connections each pool keeps (the shared pool, and each session's own pool). Must be at least 1. |
| `json_dumps`, `json_loads` | The JSON functions for the engine's sessions, unless a session sets its own. The defaults use the standard library: compact output, and bodies decoded as UTF-8. See [JSON](#json). |

Each asyncio loop has its own default engine, so sessions without an engine work in any thread, and in each `asyncio.run()`, without setup. A loop's default engine lasts as long as the loop, and is closed once the loop has been closed and no session uses it.

An engine binds to the asyncio loop running when it is first used, and so does a session. An engine serves one loop at a time, and using it from a second live loop raises `RuntimeError`. Once its loop is closed, as at the end of `asyncio.run()`, it binds to the next one and keeps its pool. So a session kept between `asyncio.run()` calls keeps its connections. To run loops in several threads at once with your own engines, give each thread its own `Engine`.

`engine.close()` stops the curl thread and closes the shared pool's connections, and later requests raise `RuntimeError`. Requests still in flight fail with `RequestError`, and a `ResourceWarning` says how many there were. `with` and `async with` call `close()` on exit. An engine that is no longer referenced is closed automatically. See [docs/connections.md](docs/connections.md) for how these fit together.

### Session methods

- `await session.get(url, **kwargs)`, and likewise `post`, `put`, `patch`, `delete`, `head` and `options`.
- `await session.request(method, url, **kwargs)`.
- `session.close()`: closes the session's connections once its requests finish. Later requests raise `RuntimeError`. `with` and `async with` call it on exit.
- `session.cookies()`: the cookie jar as a dict.
- `session.get_cookie_list()`, `session.add_cookie_list(cookies)`, `session.erase_all_cookies()` and `session.erase_session_cookies()`: work with `Cookie` objects. These are ordinary methods, not coroutines.
- `session.set_response_callback(callback)`: called with each `Response`, including each hop of a redirect.

### Request arguments

| Argument | Meaning |
|---|---|
| `params` | Query parameters: a dict or list of pairs (lists as values repeat the key), or a ready-made string. Added to any query already in the URL. |
| `headers` | Dict of headers. |
| `headers_list` | List of `"Name: value"` strings, for repeated headers. |
| `cookies` | Dict of cookies to send with this request (added to the session jar). |
| `cookie_list` | List of `Cookie` objects. |
| `auth` | `(username, password)` for basic auth. |
| `data` | Request body: `str` or `bytes` as is, with no `Content-Type` unless you set one, or a dict or list of pairs, form-encoded with `Content-Type: application/x-www-form-urlencoded` unless you set it. |
| `json` | Object to send as JSON, encoded with the session's `json_dumps`. Sets `Content-Type: application/json` unless you set it. |
| `files` | Dict (or list of pairs) of files to send as `multipart/form-data`, with any `data` dict as extra fields. A value is an open file, or `(filename, content)` or `(filename, content, content_type)`, where content is `bytes`, `str` or a file. Files are read into memory. |
| `cert` | `(certfile, keyfile)`, overriding the session's certificate. |
| `verify` | `True`, `False` or a CA bundle path, overriding the session's `verify`. |
| `timeout`, `connect_timeout` | Override the session's. `timeout=0` removes the session's limit for this request. |
| `allow_redirects` | Follow redirects (default `True`; `False` for `head`). |
| `max_redirects` | Default 5. Raises `RequestError("Max Redirects")` beyond it. |

Failed transfers raise `httpperf.RequestError` with curl's detailed error message, such as `Failed to connect to 127.0.0.1 port 8080 after 0 ms: Couldn't connect to server`. Its `code` is curl's error code. Timeouts raise `httpperf.Timeout`, a subclass, so they can be counted separately.

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
| `body` | Bytes. |
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

## Timings

All in seconds.

| Attribute | Meaning |
|---|---|
| `start_time`, `end_time` | Wall-clock times (`time.time()`) when the request was made and when Python received the response. |
| `namelookup_time` | DNS, measured by curl from the start of the transfer. |
| `connect_time` | TCP connect complete. 0 when the connection was reused. |
| `appconnect_time` | TLS handshake complete. 0 for plain http and reused connections. |
| `pretransfer_time` | Ready to send. |
| `posttransfer_time` | Request fully sent. `starttransfer_time − posttransfer_time` is the server's time to first byte, without connection setup or upload. `None` with libcurl older than 8.10. |
| `starttransfer_time` | First byte received. |
| `total_time` | Transfer complete. |
| `queue_time` | Time waiting in httpperf's queue for a connection, under the engine or session limits. 0 if the request did not wait. curl's timings start after it. |
| `client_lag` | `end_time − start_time − total_time − queue_time`: time the request spent waiting on Python, before it was sent and after it finished. Measured with the monotonic clock, so clock changes don't affect it. |

curl measures its timings on the curl thread, so a busy Python process does not inflate them. `client_lag` is how you see that Python is the bottleneck: if it grows, the client cannot keep up, and achieved request rates describe the client rather than the server. Report it alongside response times (p50, p99, max).

`queue_time` is real waiting that a user would see (a browser's "stalled" time), but it is client-side, so keep it out of server latency.

## Known limitations

- curl closes expired idle connections lazily, the next time the pool needs a connection, so the server sees the close late. See [docs/connections.md](docs/connections.md).
- Requests in flight when `Engine.close()` is called never complete.

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

To release, set `__version__` in `httpperf/version.py` (for example `1.0rc1` or `1.0`), commit and push to main, then run `make release`. It tags `v<version>` and pushes the tag, and the release workflow builds the wheels and publishes them to PyPI.

The tests run local servers: pytest-httpbin, small threaded `http.server` servers for connection counting and TLS, and hypercorn for HTTP/2 and HTTP/3 (skipped on Python 3.8, where hypercorn's HTTP/3 server hangs on shutdown). Client certificates are generated with `openssl`. The httpbin server is single-threaded, so keep concurrency in tests low.

Set `HTTPPERF_PROFILE=1` when building to instrument the extension. See [docs/profiling.md](docs/profiling.md).

## Licence

MIT. See [LICENSE](LICENSE). [NOTICE](NOTICE) covers the bundled Redis event library and the libraries included in the binary wheels.
