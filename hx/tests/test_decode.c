#include <stdio.h>
#include <string.h>

#include "../src/decode.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

static size_t compress_with(int window, const char *in, size_t len, char *out, size_t cap) {
    z_stream z = {0};
    deflateInit2(&z, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY);
    z.next_in = (Bytef *)in;
    z.avail_in = (uInt)len;
    z.next_out = (Bytef *)out;
    z.avail_out = (uInt)cap;
    deflate(&z, Z_FINISH);
    size_t n = z.total_out;
    deflateEnd(&z);
    return n;
}

static size_t gzip(const char *in, size_t len, char *out, size_t cap) {
    return compress_with(16 + MAX_WBITS, in, len, out, cap);
}

static hx_buf decode(const char *coding, const char *in, size_t len, size_t step, int *rc) {
    hx_decoder d = {0};
    hx_buf out = {0};
    *rc = hx_decoder_start(&d, coding, strlen(coding));
    for (size_t at = 0; *rc == 1 && at < len; at += step) {
        if (hx_decoder_feed(&d, in + at, len - at < step ? len - at : step, &out) == -1) {
            *rc = -1;
        }
    }
    hx_decoder_free(&d);
    return out;
}

static bool decodes_to(const char *coding, const char *in, size_t len, size_t step, const char *want) {
    int rc;
    hx_buf out = decode(coding, in, len, step, &rc);
    bool ok = rc == 1 && out.len == strlen(want) && memcmp(out.data, want, out.len) == 0;
    hx_buf_free(&out);
    return ok;
}

static void test_gzip(void) {
    char z[256];
    size_t n = gzip("hello", 5, z, sizeof(z));
    CHECK(decodes_to("gzip", z, n, n, "hello"));
}

static void test_deflate_with_a_zlib_header(void) {
    char z[256];
    size_t n = compress_with(MAX_WBITS, "hello", 5, z, sizeof(z));
    CHECK(decodes_to("deflate", z, n, n, "hello"));
}

static void test_deflate_without_a_zlib_header(void) {
    char z[256];
    size_t n = compress_with(-MAX_WBITS, "hello", 5, z, sizeof(z));
    CHECK(decodes_to("deflate", z, n, n, "hello"));
}

static void test_raw_deflate_one_byte_at_a_time(void) {
    char z[256];
    size_t n = compress_with(-MAX_WBITS, "hello", 5, z, sizeof(z));
    CHECK(decodes_to("deflate", z, n, 1, "hello"));
}

static void test_gzip_twice(void) {
    char once[256], twice[256];
    size_t n = gzip("hello", 5, once, sizeof(once));
    size_t m = gzip(once, n, twice, sizeof(twice));
    CHECK(decodes_to("gzip, gzip", twice, m, m, "hello"));
}

static void test_deflate_then_gzip_is_undone_in_reverse(void) {
    char once[256], twice[256];
    size_t n = compress_with(MAX_WBITS, "hello", 5, once, sizeof(once));
    size_t m = gzip(once, n, twice, sizeof(twice));
    CHECK(decodes_to("deflate, gzip", twice, m, 3, "hello"));
}

static void test_a_big_body_one_byte_at_a_time(void) {
    static char big[100000], z[120000];
    for (size_t i = 0; i < sizeof(big) - 1; i++) {
        big[i] = (char)('a' + (i * 7919) % 26);
    }
    size_t n = gzip(big, sizeof(big) - 1, z, sizeof(z));
    CHECK(decodes_to("gzip", z, n, 1, big));
}

static void test_identity_alone_needs_no_decoding(void) {
    hx_decoder d = {0};
    CHECK(hx_decoder_start(&d, "identity", 8) == 0);
    hx_decoder_free(&d);
}

static void test_an_unknown_coding_is_refused(void) {
    hx_decoder d = {0};
    CHECK(hx_decoder_start(&d, "br", 2) == -1);
    hx_decoder_free(&d);
}

static void test_corrupt_gzip_is_an_error(void) {
    int rc;
    hx_buf out = decode("gzip", "not gzip at all", 15, 15, &rc);
    CHECK(rc == -1);
    hx_buf_free(&out);
}

int main(void) {
    test_gzip();
    test_deflate_with_a_zlib_header();
    test_deflate_without_a_zlib_header();
    test_raw_deflate_one_byte_at_a_time();
    test_gzip_twice();
    test_deflate_then_gzip_is_undone_in_reverse();
    test_a_big_body_one_byte_at_a_time();
    test_identity_alone_needs_no_decoding();
    test_an_unknown_coding_is_refused();
    test_corrupt_gzip_is_an_error();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
