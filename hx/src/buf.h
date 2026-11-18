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

typedef struct {
    char *data;
    size_t cap;
    size_t len;
} hx_text;

static inline hx_text hx_text_on(char *data, size_t cap) {
    data[0] = '\0';
    return (hx_text){data, cap, 0};
}

static inline void hx_text_str(hx_text *t, const char *s) {
    while (*s != '\0' && t->len + 1 < t->cap) {
        t->data[t->len++] = *s++;
    }
    t->data[t->len] = '\0';
}

static inline void hx_text_uint(hx_text *t, unsigned long long v, int width) {
    char digits[24];
    int n = 0;
    do {
        digits[n++] = (char)('0' + v % 10);
        v /= 10;
    } while (v != 0);
    while (n < width) {
        digits[n++] = '0';
    }
    while (n > 0 && t->len + 1 < t->cap) {
        t->data[t->len++] = digits[--n];
    }
    t->data[t->len] = '\0';
}

static inline void hx_buf_free(hx_buf *b) {
    free(b->data);
    *b = (hx_buf){0};
}

#endif
