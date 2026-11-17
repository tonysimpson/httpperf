# hx

The start of an HTTP client library to replace libcurl in httpperf. It lives here until its API settles, then moves to its own repository. httpperf will include only its public header.

So far: the sans-I/O HTTP/1.1 core in `src/h1.c`, which writes requests and parses responses (with llhttp) into events, and its tests in `tests/`. `make hx-test` builds and runs them with ASan and UBSan.

`vendor/llhttp` is llhttp 9.4.3, from the `release/v9.4.3` tag of github.com/nodejs/llhttp (archive SHA-256 `1eb813c7437b31a87496a1cd3ed79f00746720f5e7e29c79b42c02cb69f36c39`), unchanged. MIT licence, in `vendor/llhttp/LICENSE-MIT`.
