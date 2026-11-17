#ifndef HX_BUF_H
#define HX_BUF_H

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} hx_buf;

static inline bool hx_buf_reserve(hx_buf *b, size_t extra) {
    if (b->len + extra <= b->cap) {
        return true;
    }
    size_t cap = b->cap ? b->cap : 256;
    while (cap < b->len + extra) {
        cap *= 2;
    }
    char *data = realloc(b->data, cap);
    if (data == NULL) {
        return false;
    }
    b->data = data;
    b->cap = cap;
    return true;
}

static inline bool hx_buf_append(hx_buf *b, const char *data, size_t len) {
    if (len == 0) {
        return true;
    }
    if (!hx_buf_reserve(b, len)) {
        return false;
    }
    memcpy(b->data + b->len, data, len);
    b->len += len;
    return true;
}

static inline bool hx_buf_append_str(hx_buf *b, const char *s) { return hx_buf_append(b, s, strlen(s)); }

static inline void hx_buf_free(hx_buf *b) {
    free(b->data);
    *b = (hx_buf){0};
}

#endif
