#include <stdio.h>
#include <string.h>

#include "../src/cookie.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

#define NOW 1700000000

static char text[4096];

static void set(hx_jar *j, const char *value, const char *host, const char *path) {
    hx_jar_set_cookie(j, value, strlen(value), host, path, false, NOW);
}

static const char *header(hx_jar *j, const char *host, const char *path) {
    hx_buf out = {0};
    bool any = hx_jar_header(j, host, path, false, NOW, &out);
    snprintf(text, sizeof(text), "%.*s", any ? (int)out.len : 0, any ? out.data : "");
    hx_buf_free(&out);
    return text;
}

static const char *lines(hx_jar *j) {
    hx_buf out = {0};
    hx_jar_lines(j, NOW, &out);
    snprintf(text, sizeof(text), "%.*s", (int)out.len, out.len ? out.data : "");
    hx_buf_free(&out);
    return text;
}

static void test_a_cookie_is_sent_back_to_its_host(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/"), "a=1") == 0);
    hx_jar_free(j);
}

static void test_a_host_only_cookie_is_not_sent_to_a_subdomain(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/");
    CHECK(strcmp(header(j, "www.example.com", "/"), "") == 0);
    hx_jar_free(j);
}

static void test_a_domain_cookie_is_sent_to_a_subdomain(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Domain=.example.com", "example.com", "/");
    CHECK(strcmp(header(j, "www.example.com", "/"), "a=1") == 0);
    hx_jar_free(j);
}

static void test_a_domain_the_host_is_not_in_is_refused(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Domain=other.com", "example.com", "/");
    CHECK(hx_jar_count(j) == 0);
    hx_jar_free(j);
}

static void test_the_default_path_is_the_request_directory(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/docs/page?x=1");
    CHECK(strcmp(header(j, "example.com", "/docs/other"), "a=1") == 0);
    CHECK(strcmp(header(j, "example.com", "/"), "") == 0);
    hx_jar_free(j);
}

static void test_a_path_matches_only_whole_segments(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Path=/docs", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/docs/x"), "a=1") == 0);
    CHECK(strcmp(header(j, "example.com", "/docsx"), "") == 0);
    hx_jar_free(j);
}

static void test_the_same_name_domain_and_path_replaces(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/");
    set(j, "a=2", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/"), "a=2") == 0 && hx_jar_count(j) == 1);
    hx_jar_free(j);
}

static void test_max_age_zero_deletes(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/");
    set(j, "a=1; Max-Age=0", "example.com", "/");
    CHECK(hx_jar_count(j) == 0);
    hx_jar_free(j);
}

static void test_an_expires_in_the_past_deletes(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1", "example.com", "/");
    set(j, "a=1; Expires=Thu, 01 Jan 1970 00:00:00 GMT", "example.com", "/");
    CHECK(hx_jar_count(j) == 0);
    hx_jar_free(j);
}

static void test_max_age_beats_expires(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Max-Age=60; Expires=Thu, 01 Jan 1970 00:00:00 GMT", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/"), "a=1") == 0);
    hx_jar_free(j);
}

static void test_a_secure_cookie_from_plain_http_is_refused(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Secure", "example.com", "/");
    CHECK(hx_jar_count(j) == 0);
    hx_jar_free(j);
}

static void test_a_cookie_without_an_equals_is_refused(void) {
    hx_jar *j = hx_jar_new();
    set(j, "bad", "example.com", "/");
    set(j, "=1", "example.com", "/");
    CHECK(hx_jar_count(j) == 0);
    hx_jar_free(j);
}

static void test_values_are_kept_as_sent(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=\"x=y\"", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/"), "a=\"x=y\"") == 0);
    hx_jar_free(j);
}

static void test_longer_paths_are_sent_first(void) {
    hx_jar *j = hx_jar_new();
    set(j, "a=1; Path=/", "example.com", "/");
    set(j, "b=2; Path=/docs", "example.com", "/");
    CHECK(strcmp(header(j, "example.com", "/docs/x"), "b=2; a=1") == 0);
    hx_jar_free(j);
}

static void test_lines_round_trip(void) {
    hx_jar *j = hx_jar_new();
    hx_jar_add_line(j, "#HttpOnly_.example.com\tTRUE\t/\tFALSE\t0\ta\t1", NOW);
    CHECK(strcmp(lines(j), "#HttpOnly_.example.com\tTRUE\t/\tFALSE\t0\ta\t1\n") == 0);
    hx_jar_free(j);
}

static void test_a_line_without_a_value_is_an_empty_value(void) {
    hx_jar *j = hx_jar_new();
    hx_jar_add_line(j, "example.com\tFALSE\t/\tFALSE\t0\ta", NOW);
    CHECK(strcmp(header(j, "example.com", "/"), "a=") == 0);
    hx_jar_free(j);
}

static void test_clearing_session_cookies_keeps_lasting_ones(void) {
    hx_jar *j = hx_jar_new();
    hx_jar_add_line(j, "example.com\tFALSE\t/\tFALSE\t0\tbrief\t1", NOW);
    hx_jar_add_line(j, "example.com\tFALSE\t/\tFALSE\t2000000000\tlasting\t1", NOW);
    hx_jar_clear(j, true);
    CHECK(strcmp(header(j, "example.com", "/"), "lasting=1") == 0);
    hx_jar_free(j);
}

static void test_dates_in_the_three_http_formats(void) {
    const char *dates[] = {"Sun, 06 Nov 1994 08:49:37 GMT", "Sunday, 06-Nov-94 08:49:37 GMT",
                           "Sun Nov  6 08:49:37 1994"};
    for (int i = 0; i < 3; i++) {
        CHECK(hx_cookie_date(dates[i], strlen(dates[i])) == 784111777);
    }
}

static void test_a_nonsense_date_is_refused(void) { CHECK(hx_cookie_date("soon", 4) == -1); }

int main(void) {
    test_a_cookie_is_sent_back_to_its_host();
    test_a_host_only_cookie_is_not_sent_to_a_subdomain();
    test_a_domain_cookie_is_sent_to_a_subdomain();
    test_a_domain_the_host_is_not_in_is_refused();
    test_the_default_path_is_the_request_directory();
    test_a_path_matches_only_whole_segments();
    test_the_same_name_domain_and_path_replaces();
    test_max_age_zero_deletes();
    test_an_expires_in_the_past_deletes();
    test_max_age_beats_expires();
    test_a_secure_cookie_from_plain_http_is_refused();
    test_a_cookie_without_an_equals_is_refused();
    test_values_are_kept_as_sent();
    test_longer_paths_are_sent_first();
    test_lines_round_trip();
    test_a_line_without_a_value_is_an_empty_value();
    test_clearing_session_cookies_keeps_lasting_ones();
    test_dates_in_the_three_http_formats();
    test_a_nonsense_date_is_refused();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
