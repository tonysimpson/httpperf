# hx

The start of an HTTP client library to replace libcurl in httpperf. It lives here until its API settles, then moves to its own repository. httpperf will include only its public header.

So far:

- `include/hx.h`, the public interface: a client driven by the caller's loop (socket readiness, one timer, a clock), requests started, paused and cancelled, upload bodies given with `hx_request_send` and `hx_request_end_body` as `SEND_READY` asks for them, and events pulled with `hx_client_events`.
- `src/client.c`, the connection layer: plain HTTP/1.1 over TCP (no TLS yet), trying each address a name resolves to in turn, a keep-alive pool that drops connections the server closes (capped at `max_idle` idle connections in all, each reused only until `max_age_ns`), pools closed on request with `hx_client_close_pool` and counted through the optional `connection` hook, idle, request and connect timeouts, cancel, and timestamps and byte counts in every `DONE`. A request on a reused connection that fails before any response byte is sent again once on a new one, unless its body was streamed; an upload checks an idle connection is still open first, and keeps a response that arrives after its body can no longer be sent. Event data stays valid until the next `hx_client_events` call.
- `src/dns.c`, name lookups: `getaddrinfo` on up to four worker threads, results handed back through a pipe the caller watches. The client shares one lookup between requests for the same host and port and keeps results for `dns_ttl_ns` (60 seconds by default).
- `src/decode.c`, gzip and deflate (with or without the zlib header) bodies decoded with zlib, including stacked codings such as `gzip, gzip`. A request asks for it with `decode`; an unknown coding fails only once body bytes arrive, as in curl.
- `src/cookie.c`, a cookie jar after RFC 6265 (host-only and domain cookies, paths, Max-Age and Expires in the three HTTP date formats, secure and prefixed names) that reads and writes curl's cookie lines.
- `src/h1.c`, the sans-I/O HTTP/1.1 core, which writes requests and parses responses (with llhttp) into events.
- Tests in `tests/`. `make hx-test` builds and runs them with ASan and UBSan.

`vendor/llhttp` is llhttp 9.4.3, from the `release/v9.4.3` tag of github.com/nodejs/llhttp (archive SHA-256 `1eb813c7437b31a87496a1cd3ed79f00746720f5e7e29c79b42c02cb69f36c39`), unchanged. MIT licence, in `vendor/llhttp/LICENSE-MIT`.
