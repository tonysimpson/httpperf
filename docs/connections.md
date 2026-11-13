# Connections

How a virtual user holds connections changes what the server sees: how many TCP connections and TLS handshakes it handles, how load spreads across a load balancer, and where `TIME_WAIT` lands. In httpperf that is an explicit choice made per session, not a side effect of the implementation.

## The settings

### Where connections live: `connections`

| Value | Pool | What the server sees |
|---|---|---|
| `"shared"` (default) | One pool per engine (by default, one per asyncio loop), held by the curl multi handle. | A handful of long-lived connections, however many sessions there are. Like a single HTTP client object (requests, httpx) shared across a program. |
| `"session"` | The session's own pool, held in its curl share. | Keep-alive within a session, a fresh connection for each new session. Like a browser profile per user. |
| `"none"` | No reuse: `CURLOPT_FRESH_CONNECT` and `CURLOPT_FORBID_REUSE`. | A new connection, and TLS handshake, for every request. |

Cookies are always per session, so sharing connections never shares cookies.

### TLS session tickets: `tls`

| Value | Ticket cache | Effect on new connections |
|---|---|---|
| `"shared"` (default) | The engine's (its curl multi handle's cache). | Resumed handshake: cheaper for both ends. |
| `"session"` | The session's share. | Full handshake for each session's first connection, as a new visitor would see. |

curl matches cached tickets on the client certificate as well as the host, so sessions with different certificates never resume each other's TLS sessions.

`tls` only matters when a new connection is opened, so with `connections="shared"` it rarely makes a difference. With `connections="session"` it chooses between new visitors (`tls="session"`) and returning visitors (`tls="shared"`).

### Protocol: `http_version`

`"HTTP/2"` (default) asks for HTTP/2 over TLS and, for `https://` URLs, sets `CURLOPT_PIPEWAIT`, so requests started together wait to share one multiplexed connection rather than each opening their own. Plain `http://` stays on HTTP/1.1, without `PIPEWAIT`, since there is nothing to wait for. `"HTTP/1.1"` never uses HTTP/2. `"HTTP/3"` uses QUIC and nothing else, also with `CURLOPT_PIPEWAIT`. curl can instead race QUIC against TCP and fall back, but that opens two connections for one, so httpperf does not offer it. `Response.http_version` reports what was negotiated.

### Limits

Where a limit is set depends on where the connections live.

| Setting | Set on | Applies to | Done by |
|---|---|---|---|
| `max_host_connections`, `max_total_connections` | `Engine` | `"shared"` and `"none"` sessions together, unless the session has its own limits | httpperf, in a queue on the curl thread |
| `max_host_connections`, `max_connections` | session | that session only (`"session"` or `"none"`) | httpperf, in a queue on the curl thread |
| `max_idle_connections` | `Engine` | each pool separately | curl, which closes the oldest idle connection when a pool is over the limit |

httpperf does the queueing, not curl, because curl starts its timers before a request waits in its queue, so the wait would be counted as response time. All the limits count connections in use, which for HTTP/1.1 means requests in flight. Once a host has answered over HTTP/2 or HTTP/3, its requests share one connection and are not held back by `max_host_connections`. Until the first response shows that, a burst to a new host is capped like HTTP/1.1. Time spent in the queue is reported as `Response.queue_time` and kept out of `client_lag`.

Waiting requests queue per host, first come first served. When a connection frees up, hosts with waiting requests take turns, so under `max_total_connections` one busy host cannot starve the others, and a host that is stuck at its own limit does not slow down requests to other hosts.

A host's HTTP/2 status follows its latest successful response, so a host that falls back to HTTP/1.1 is counted per request again. Failed requests leave it unchanged. httpperf counts a multiplexed host as one connection, even if curl opens a second one when the server's limit on concurrent streams is reached.

Time spent waiting in the queue has no limit: `timeout` starts when the request reaches curl. To bound the whole wait, use `asyncio.wait_for`, which cancels the request wherever it is.

httpperf keeps what it has learnt about each host until the host has had nothing in flight or waiting for 60 seconds, so talking to many hosts over time does not grow memory without bound.

Idle connections do not count towards `max_connections`, so a session can hold more open connections than the cap if it talks to several hosts. `max_idle_connections` bounds those.

### Connection lifetime

| Setting | Meaning |
|---|---|
| `idle_timeout` (session, default 118 s) | A connection idle for longer is not reused. |
| `max_connection_age` (session, default none) | A connection older than this is not reused, however busy. Useful for spreading load across new load-balancer back ends. |
| `session.close()` | Closes the session's own connections once its requests finish. |
| `Engine.close()` | Closes the shared pool's connections. |

Both times are whole seconds. curl does not close expired connections on a timer: it notices them the next time that pool needs a connection, or when the session is closed. So the client stops reusing a connection at the right time, but the server only sees it close later.

With `connections="session"`, a session's connections stay open until `close()` is called or the session is garbage-collected. Responses do not keep them open.

## Examples

These are starting points, not exact models of any product. Check the behaviour of the client you want to imitate.

One HTTP client object shared by a service, capped at 100 connections:

```python
engine = httpperf.Engine(max_total_connections=100)
session = httpperf.Session(engine=engine)
```

A browser, new visitor, one session per journey:

```python
async with httpperf.Session(connections="session", tls="session", max_host_connections=6) as session:
    ...
```

6 per host is the usual browser limit for HTTP/1.1. Over HTTP/2 the session uses one connection per host.

A returning visitor: the same, with `tls="shared"`, so handshakes resume.

A client that never reuses connections:

```python
session = httpperf.Session(connections="none")
```

## Checking the model

- `connect_time` is 0 when a request reused a connection, and above 0 when it opened one. `appconnect_time` is the same for the TLS handshake.
- `http_version` shows whether HTTP/2 was negotiated.
- `queue_time` shows requests waiting for a connection slot.

Counting new connections at the server (or from `/proc/net/snmp` `ActiveOpens` on the client) is the most direct check.

## Client-side limits to watch

- Ephemeral ports: a client that opens and closes many connections holds them in `TIME_WAIT` for about 60 s, which limits new connections to roughly 470 per second per client address and server address (with about 28,000 ephemeral ports). Beyond that, use more source addresses, widen `net.ipv4.ip_local_port_range`, or enable `net.ipv4.tcp_tw_reuse`.
- TLS CPU: full handshakes for every session (`tls="session"`) or every request (`connections="none"`) cost the client CPU. `client_lag` shows when this starts to limit the load.
