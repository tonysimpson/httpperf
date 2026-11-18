#ifndef HX_WS_H
#define HX_WS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define HX_WS_TEXT 1u
#define HX_WS_BINARY 2u
#define HX_WS_CONT 4u
#define HX_WS_CLOSE 8u
#define HX_WS_PING 16u
#define HX_WS_PONG 64u
#define HX_WS_FINAL 0x80000000u

typedef struct {
    bool in_message;
    uint8_t mask[4];
    uint64_t left;
    uint64_t at;
} hx_ws_writer;

size_t hx_ws_begin_frame(hx_ws_writer *w, uint8_t out[14], uint32_t flags, uint64_t len);
void hx_ws_mask(hx_ws_writer *w, char *data, size_t len);

typedef void (*hx_ws_emit)(void *ctx, uint32_t flags, const char *data, size_t len);

typedef struct {
    uint8_t head[14];
    size_t head_len;
    bool in_frame;
    uint64_t left;
    uint32_t flags;
    uint32_t kind;
} hx_ws_reader;

int hx_ws_read(hx_ws_reader *r, const char *data, size_t len, hx_ws_emit emit, void *ctx);

#endif
