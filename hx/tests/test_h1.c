#include <stdio.h>
#include <string.h>

#include "../src/h1.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

static char written[4096];

static const char *request(hx_h1_request r) {
    hx_buf out = {0};
    int rc = hx_h1_write_request(&out, &r);
    snprintf(written, sizeof(written), "%.*s", rc == 0 ? (int)out.len : 0, out.data);
    hx_buf_free(&out);
    return rc == 0 ? written : NULL;
}

static const hx_h1_request GET = {"GET", "/a?b=1", "example.com:8080", NULL, 0, NULL, 0, HX_H1_NO_UPLOAD};

static void test_a_get_has_a_request_line_and_host(void) {
    CHECK(strcmp(request(GET), "GET /a?b=1 HTTP/1.1\r\nHost: example.com:8080\r\n\r\n") == 0);
}

static void test_a_given_host_is_not_added_again(void) {
    const char *headers[] = {"host: other"};
    hx_h1_request r = GET;
    r.headers = headers;
    r.header_count = 1;
    CHECK(strcmp(request(r), "GET /a?b=1 HTTP/1.1\r\nhost: other\r\n\r\n") == 0);
}

static void test_a_body_gets_a_content_length(void) {
    hx_h1_request r = GET;
    r.method = "POST";
    r.body = "hello";
    r.body_len = 5;
    CHECK(strcmp(request(r), "POST /a?b=1 HTTP/1.1\r\nHost: example.com:8080\r\nContent-Length: 5\r\n\r\nhello") == 0);
}

static void test_an_upload_of_unknown_size_is_chunked(void) {
    hx_h1_request r = GET;
    r.upload_size = HX_H1_CHUNKED;
    CHECK(strstr(request(r), "\r\nTransfer-Encoding: chunked\r\n\r\n") != NULL);
}

static void test_chunks_are_framed(void) {
    hx_buf out = {0};
    hx_h1_write_chunk(&out, "hello world 12345", 17);
    hx_h1_write_chunk(&out, NULL, 0);
    const char expected[] = "11\r\nhello world 12345\r\n0\r\n\r\n";
    CHECK(out.len == sizeof(expected) - 1 && memcmp(out.data, expected, out.len) == 0);
    hx_buf_free(&out);
}

static void test_a_newline_in_a_header_is_refused(void) {
    const char *headers[] = {"X: a\r\nY: b"};
    hx_h1_request r = GET;
    r.headers = headers;
    r.header_count = 1;
    CHECK(request(r) == NULL);
}

typedef struct {
    char head[1024];
    char body[1024];
    size_t body_len;
    int done;
    size_t used;
    int error;
} result;

static result parse(const char *response, size_t step, bool head_request, int max) {
    result out = {0};
    hx_h1 h;
    hx_h1_init(&h, head_request);
    hx_h1_event events[8];
    size_t len = strlen(response), at = 0;
    while (at < len && !out.done && !out.error) {
        size_t n = len - at < step ? len - at : step, used;
        int count = hx_h1_feed(&h, response + at, n, &used, events, max);
        if (count < 0) {
            out.error = count;
            break;
        }
        for (int i = 0; i < count; i++) {
            if (events[i].type == HX_H1_HEADERS) {
                snprintf(out.head, sizeof(out.head), "%d %d %.*s", h.status, h.version, (int)events[i].len,
                         events[i].data);
            } else if (events[i].type == HX_H1_BODY) {
                memcpy(out.body + out.body_len, events[i].data, events[i].len);
                out.body_len += events[i].len;
            } else {
                out.done = h.keep_alive ? 2 : 1;
            }
        }
        at += used;
    }
    if (!out.done && !out.error) {
        int count = hx_h1_finish(&h, events, max);
        out.done = count > 0 && events[count - 1].type == HX_H1_DONE;
        out.error = count < 0 ? count : 0;
    }
    out.used = at;
    hx_h1_free(&h);
    return out;
}

static const char *OK = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A:  b \r\n\r\nhello";

static void test_a_response_gives_headers_body_and_done(void) {
    result r = parse(OK, 1000, false, 8);
    CHECK(strcmp(r.head, "200 11 HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: b \r\n\r\n") == 0);
    CHECK(r.body_len == 5 && memcmp(r.body, "hello", 5) == 0);
    CHECK(r.done == 2);
}

static void test_a_response_fed_a_byte_at_a_time_is_the_same(void) {
    result whole = parse(OK, 1000, false, 8), bytes = parse(OK, 1, false, 8);
    CHECK(strcmp(whole.head, bytes.head) == 0 && bytes.body_len == 5 && bytes.done == 2);
}

static void test_one_event_at_a_time_loses_nothing(void) {
    result r =
        parse("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n", 1000, false, 1);
    CHECK(r.body_len == 5 && memcmp(r.body, "abcde", 5) == 0 && r.done == 2);
}

static void test_a_chunked_body_is_decoded(void) {
    result r =
        parse("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n", 7, false, 8);
    CHECK(r.body_len == 5 && memcmp(r.body, "abcde", 5) == 0 && r.done == 2);
}

static void test_a_100_continue_is_not_the_response(void) {
    result r = parse("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n", 1000, false, 8);
    CHECK(strcmp(r.head, "204 11 HTTP/1.1 204 No Content\r\n\r\n") == 0 && r.done == 2);
}

static void test_a_head_response_has_no_body(void) {
    result r = parse("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n", 1000, true, 8);
    CHECK(r.body_len == 0 && r.done == 2);
}

static void test_a_body_can_end_with_the_connection(void) {
    result r = parse("HTTP/1.0 200 OK\r\n\r\nto the end", 1000, false, 8);
    CHECK(r.body_len == 10 && r.done == 1);
}

static void test_bytes_after_the_response_are_left(void) {
    result r = parse("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokHTTP/1.1", 1000, false, 8);
    CHECK(r.used == strlen("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"));
}

static void test_a_status_without_a_reason(void) {
    result r = parse("HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n", 1000, false, 8);
    CHECK(strcmp(r.head, "200 11 HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n") == 0);
}

static void test_an_http_1_0_status_line_keeps_its_version(void) {
    result r = parse("HTTP/1.0 404 Not Found\r\n\r\n", 1000, false, 8);
    CHECK(strcmp(r.head, "404 10 HTTP/1.0 404 Not Found\r\n\r\n") == 0);
}

static void test_a_second_response_on_the_connection_is_parsed(void) {
    const char *two =
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\naHTTP/1.1 404 Not Found\r\nContent-Length: 1\r\n\r\nb";
    hx_h1 h;
    hx_h1_init(&h, false);
    hx_h1_event events[8];
    size_t used, first;
    hx_h1_feed(&h, two, strlen(two), &first, events, 8);
    hx_h1_next(&h, false);
    int count = hx_h1_feed(&h, two + first, strlen(two) - first, &used, events, 8);
    CHECK(count == 3 && h.status == 404 && events[1].len == 1 && events[1].data[0] == 'b');
    CHECK(first + used == strlen(two));
    hx_h1_free(&h);
}

static void test_garbage_is_an_error(void) { CHECK(parse("NOT HTTP\r\n\r\n", 1000, false, 8).error == HX_EPROTO); }

static void test_a_response_cut_short_is_an_error(void) {
    CHECK(parse("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhel", 1000, false, 8).error == HX_EPROTO);
}

int main(void) {
    test_a_get_has_a_request_line_and_host();
    test_a_given_host_is_not_added_again();
    test_a_body_gets_a_content_length();
    test_an_upload_of_unknown_size_is_chunked();
    test_chunks_are_framed();
    test_a_newline_in_a_header_is_refused();
    test_a_response_gives_headers_body_and_done();
    test_a_response_fed_a_byte_at_a_time_is_the_same();
    test_one_event_at_a_time_loses_nothing();
    test_a_chunked_body_is_decoded();
    test_a_100_continue_is_not_the_response();
    test_a_head_response_has_no_body();
    test_a_body_can_end_with_the_connection();
    test_bytes_after_the_response_are_left();
    test_a_status_without_a_reason();
    test_an_http_1_0_status_line_keeps_its_version();
    test_a_second_response_on_the_connection_is_parsed();
    test_garbage_is_an_error();
    test_a_response_cut_short_is_an_error();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
