# hx

The start of an HTTP client library to replace libcurl in httpperf. It lives here until its API settles, then moves to its own repository. httpperf will include only its public header.

So far:

- `include/hx.h`, the public interface: a client driven by the caller's loop (socket readiness, one timer, a clock), requests started and cancelled, and events pulled with `hx_client_events`.
- `src/client.c`, the connection layer: plain HTTP/1.1 over TCP to IP addresses (no DNS or TLS yet), a keep-alive pool that drops connections the server closes, idle, request and connect timeouts, cancel, and timestamps and byte counts in every `DONE`. Event data stays valid until the next `hx_client_events` call.
- `src/h1.c`, the sans-I/O HTTP/1.1 core, which writes requests and parses responses (with llhttp) into events.
- Tests in `tests/`. `make hx-test` builds and runs them with ASan and UBSan.

`vendor/llhttp` is llhttp 9.4.3, from the `release/v9.4.3` tag of github.com/nodejs/llhttp (archive SHA-256 `1eb813c7437b31a87496a1cd3ed79f00746720f5e7e29c79b42c02cb69f36c39`), unchanged. MIT licence, in `vendor/llhttp/LICENSE-MIT`.
