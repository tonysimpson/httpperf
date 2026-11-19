#include "ws.h"

#include "compat.h"

static uint8_t opcode(hx_ws_writer *w, uint32_t flags, bool *fin) {
    *fin = true;
    if (flags & HX_WS_CLOSE) {
        return 8;
    }
    if (flags & HX_WS_PING) {
        return 9;
    }
    if (flags & HX_WS_PONG) {
        return 10;
    }
    uint8_t op = w->in_message ? 0 : (flags & HX_WS_BINARY) ? 2 : 1;
    *fin = !(flags & HX_WS_CONT);
    w->in_message = !*fin;
    return op;
}

size_t hx_ws_begin_frame(hx_ws_writer *w, uint8_t out[14], uint32_t flags, uint64_t len) {
    bool fin;
    uint8_t op = opcode(w, flags, &fin);
    size_t n = 0;
    out[n++] = (uint8_t)((fin ? 0x80 : 0) | op);
    if (len < 126) {
        out[n++] = (uint8_t)(0x80 | len);
    } else if (len <= 0xffff) {
        out[n++] = 0x80 | 126;
        out[n++] = (uint8_t)(len >> 8);
        out[n++] = (uint8_t)len;
    } else {
        out[n++] = 0x80 | 127;
        for (int i = 7; i >= 0; i--) {
            out[n++] = (uint8_t)(len >> (i * 8));
        }
    }
    if (hx_random_bytes(w->mask, sizeof(w->mask)) != 0) {
        w->mask[0] = (uint8_t)(len * 31 + 7);
    }
    for (int i = 0; i < 4; i++) {
        out[n++] = w->mask[i];
    }
    w->left = len;
    w->at = 0;
    return n;
}

void hx_ws_mask(hx_ws_writer *w, char *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        data[i] = (char)(data[i] ^ w->mask[(w->at + i) & 3]);
    }
    w->at += len;
    w->left -= len;
}

static size_t head_size(const uint8_t *h, size_t have) {
    if (have < 2) {
        return 2;
    }
    size_t n = 2 + ((h[1] & 0x7f) == 126 ? 2 : (h[1] & 0x7f) == 127 ? 8 : 0);
    return n + (h[1] & 0x80 ? 4 : 0);
}

static int start_frame(hx_ws_reader *r) {
    uint8_t op = r->head[0] & 0x0f;
    bool fin = r->head[0] & 0x80;
    if (r->head[1] & 0x80 || r->head[0] & 0x70) {
        return -1;
    }
    uint64_t len = r->head[1] & 0x7f;
    if (len == 126) {
        len = (uint64_t)r->head[2] << 8 | r->head[3];
    } else if (len == 127) {
        len = 0;
        for (int i = 0; i < 8; i++) {
            len = len << 8 | r->head[2 + i];
        }
    }
    if (op == 0 || op == 1 || op == 2) {
        if ((op == 0) != (r->kind != 0)) {
            return -1;
        }
        if (op != 0) {
            r->kind = op == 1 ? HX_WS_TEXT : HX_WS_BINARY;
        }
        r->flags = r->kind | (fin ? 0 : HX_WS_CONT);
        if (fin) {
            r->kind = 0;
        }
    } else if (op >= 8 && op <= 10 && fin && len < 126) {
        r->flags = op == 8 ? HX_WS_CLOSE : op == 9 ? HX_WS_PING : HX_WS_PONG;
    } else {
        return -1;
    }
    r->left = len;
    r->in_frame = true;
    r->head_len = 0;
    return 0;
}

int hx_ws_read(hx_ws_reader *r, const char *data, size_t len, hx_ws_emit emit, void *ctx) {
    size_t at = 0;
    while (at < len || (r->in_frame && r->left == 0)) {
        if (!r->in_frame) {
            size_t need = head_size(r->head, r->head_len);
            while (r->head_len < need && at < len) {
                r->head[r->head_len++] = (uint8_t)data[at++];
                need = head_size(r->head, r->head_len);
            }
            if (r->head_len < need) {
                return 0;
            }
            if (start_frame(r) == -1) {
                return -1;
            }
            continue;
        }
        size_t n = len - at < r->left ? len - at : (size_t)r->left;
        r->left -= n;
        bool last = r->left == 0;
        emit(ctx, r->flags | (last ? HX_WS_FINAL : 0), data + at, n);
        at += n;
        if (last) {
            r->in_frame = false;
        }
    }
    return 0;
}
