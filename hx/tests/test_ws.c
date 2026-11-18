#include <stdio.h>
#include <string.h>

#include "../src/ws.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

typedef struct {
    uint32_t flags[8];
    char data[256];
    size_t len;
    int count;
} seen;

static void record(void *ctx, uint32_t flags, const char *data, size_t len) {
    seen *s = ctx;
    if (s->count < 8) {
        s->flags[s->count] = flags;
    }
    s->count++;
    memcpy(s->data + s->len, data, len);
    s->len += len;
}

static int read_bytes(hx_ws_reader *r, const char *data, size_t len, seen *s) {
    return hx_ws_read(r, data, len, record, s);
}

static void test_a_text_frame_is_one_final_record(void) {
    hx_ws_reader r = {0};
    seen s = {0};
    CHECK(read_bytes(&r, "\x81\x02hi", 4, &s) == 0);
    CHECK(s.count == 1 && s.flags[0] == (HX_WS_TEXT | HX_WS_FINAL) && s.len == 2 && memcmp(s.data, "hi", 2) == 0);
}

static void test_a_frame_split_anywhere_arrives_whole(void) {
    const char frame[] = "\x82\x05hello";
    for (size_t cut = 1; cut < 7; cut++) {
        hx_ws_reader r = {0};
        seen s = {0};
        read_bytes(&r, frame, cut, &s);
        read_bytes(&r, frame + cut, 7 - cut, &s);
        CHECK(s.len == 5 && memcmp(s.data, "hello", 5) == 0);
        CHECK(s.flags[s.count - 1] == (HX_WS_BINARY | HX_WS_FINAL));
    }
}

static void test_a_16_bit_length_is_read(void) {
    char frame[4 + 300] = "\x82\x7e\x01\x2c";
    memset(frame + 4, 'x', 300);
    hx_ws_reader r = {0};
    seen s = {0};
    CHECK(read_bytes(&r, frame, 4, &s) == 0 && r.in_frame && r.left == 300);
}

static void test_fragments_carry_their_kind_and_cont(void) {
    hx_ws_reader r = {0};
    seen s = {0};
    read_bytes(&r,
               "\x01\x01"
               "a"
               "\x80\x01"
               "b",
               6, &s);
    CHECK(s.count == 2 && s.flags[0] == (HX_WS_TEXT | HX_WS_CONT | HX_WS_FINAL) &&
          s.flags[1] == (HX_WS_TEXT | HX_WS_FINAL));
}

static void test_an_empty_close_is_one_record(void) {
    hx_ws_reader r = {0};
    seen s = {0};
    read_bytes(&r, "\x88\x00", 2, &s);
    CHECK(s.count == 1 && s.flags[0] == (HX_WS_CLOSE | HX_WS_FINAL) && s.len == 0);
}

static void test_a_masked_frame_from_the_server_is_an_error(void) {
    hx_ws_reader r = {0};
    seen s = {0};
    CHECK(read_bytes(&r, "\x81\x81\x01\x02\x03\x04x", 7, &s) == -1);
}

static void test_a_continuation_without_a_start_is_an_error(void) {
    hx_ws_reader r = {0};
    seen s = {0};
    CHECK(read_bytes(&r, "\x80\x01x", 3, &s) == -1);
}

static void test_a_written_frame_is_masked_and_reads_back(void) {
    hx_ws_writer w = {0};
    uint8_t head[14];
    size_t n = hx_ws_begin_frame(&w, head, HX_WS_TEXT, 5);
    char payload[] = "hello";
    hx_ws_mask(&w, payload, 5);
    CHECK(n == 6 && head[0] == 0x81 && head[1] == (0x80 | 5) && w.left == 0);
    for (int i = 0; i < 5; i++) {
        payload[i] = (char)(payload[i] ^ head[2 + (i & 3)]);
    }
    CHECK(memcmp(payload, "hello", 5) == 0);
}

static void test_a_long_frame_uses_the_64_bit_length(void) {
    hx_ws_writer w = {0};
    uint8_t head[14];
    CHECK(hx_ws_begin_frame(&w, head, HX_WS_BINARY, 70000) == 14 && head[1] == (0x80 | 127));
}

static void test_written_fragments_continue_the_message(void) {
    hx_ws_writer w = {0};
    uint8_t first[14], second[14];
    hx_ws_begin_frame(&w, first, HX_WS_TEXT | HX_WS_CONT, 0);
    hx_ws_begin_frame(&w, second, HX_WS_TEXT, 0);
    CHECK(first[0] == 0x01 && second[0] == 0x80);
}

static void test_a_ping_inside_a_message_does_not_end_it(void) {
    hx_ws_writer w = {0};
    uint8_t a[14], b[14], c[14];
    hx_ws_begin_frame(&w, a, HX_WS_TEXT | HX_WS_CONT, 0);
    hx_ws_begin_frame(&w, b, HX_WS_PING, 0);
    hx_ws_begin_frame(&w, c, HX_WS_TEXT, 0);
    CHECK(b[0] == 0x89 && c[0] == 0x80);
}

int main(void) {
    test_a_text_frame_is_one_final_record();
    test_a_frame_split_anywhere_arrives_whole();
    test_a_16_bit_length_is_read();
    test_fragments_carry_their_kind_and_cont();
    test_an_empty_close_is_one_record();
    test_a_masked_frame_from_the_server_is_an_error();
    test_a_continuation_without_a_start_is_an_error();
    test_a_written_frame_is_masked_and_reads_back();
    test_a_long_frame_uses_the_64_bit_length();
    test_written_fragments_continue_the_message();
    test_a_ping_inside_a_message_does_not_end_it();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
