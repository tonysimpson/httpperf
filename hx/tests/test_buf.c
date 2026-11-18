#include <stdio.h>
#include <string.h>

#include "../src/buf.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

static void test_text_starts_empty(void) {
    char out[8] = "junk";
    hx_text t = hx_text_on(out, sizeof(out));
    CHECK(t.len == 0 && strcmp(out, "") == 0);
}

static void test_strings_are_joined(void) {
    char out[16];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_str(&t, "ab");
    hx_text_str(&t, "cd");
    CHECK(t.len == 4 && strcmp(out, "abcd") == 0);
}

static void test_a_long_string_is_cut_to_fit(void) {
    char out[4];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_str(&t, "abcdef");
    CHECK(t.len == 3 && strcmp(out, "abc") == 0);
}

static void test_a_number_is_written_in_decimal(void) {
    char out[32];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_uint(&t, 18446744073709551615ULL, 0);
    CHECK(strcmp(out, "18446744073709551615") == 0);
}

static void test_zero_is_one_digit(void) {
    char out[8];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_uint(&t, 0, 0);
    CHECK(strcmp(out, "0") == 0);
}

static void test_a_number_is_padded_to_the_width(void) {
    char out[8];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_uint(&t, 7, 3);
    CHECK(strcmp(out, "007") == 0);
}

static void test_a_long_number_keeps_its_leading_digits(void) {
    char out[4];
    hx_text t = hx_text_on(out, sizeof(out));
    hx_text_uint(&t, 12345, 0);
    CHECK(t.len == 3 && strcmp(out, "123") == 0);
}

int main(void) {
    test_text_starts_empty();
    test_strings_are_joined();
    test_a_long_string_is_cut_to_fit();
    test_a_number_is_written_in_decimal();
    test_zero_is_one_digit();
    test_a_number_is_padded_to_the_width();
    test_a_long_number_keeps_its_leading_digits();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
