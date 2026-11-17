#include "decode.h"

#include <strings.h>

static bool is_space(char c) { return c == ' ' || c == '\t' || c == ','; }

int hx_decoder_start(hx_decoder *d, const char *value, size_t len) {
    bool gzip[HX_MAX_CODINGS];
    int count = 0;
    size_t i = 0;
    while (i < len) {
        while (i < len && is_space(value[i])) {
            i++;
        }
        size_t start = i;
        while (i < len && !is_space(value[i])) {
            i++;
        }
        size_t n = i - start;
        if (n == 0) {
            continue;
        }
        const char *name = value + start;
        if (n == 8 && strncasecmp(name, "identity", 8) == 0) {
            continue;
        }
        bool is_gzip = (n == 4 && strncasecmp(name, "gzip", 4) == 0) || (n == 6 && strncasecmp(name, "x-gzip", 6) == 0);
        if ((!is_gzip && !(n == 7 && strncasecmp(name, "deflate", 7) == 0)) || count == HX_MAX_CODINGS) {
            return -1;
        }
        gzip[count++] = is_gzip;
    }
    hx_decoder_end(d);
    for (int j = 0; j < count; j++) {
        d->gzip[j] = gzip[count - 1 - j];
    }
    d->count = count;
    return count > 0 ? 1 : 0;
}

static int begin(hx_decoder *d, int i, bool raw) {
    d->z[i] = (z_stream){0};
    if (inflateInit2(&d->z[i], d->gzip[i] ? 16 + MAX_WBITS : raw ? -MAX_WBITS : MAX_WBITS) != Z_OK) {
        return -1;
    }
    d->started[i] = true;
    return 0;
}

static int inflate_into(hx_decoder *d, int i, const unsigned char *in, size_t len, hx_buf *out) {
    if (d->ended[i] || len == 0) {
        return 0;
    }
    if (!d->started[i]) {
        if (!d->gzip[i] && !d->have_first[i] && len == 1) {
            d->first[i] = in[0];
            d->have_first[i] = true;
            return 0;
        }
        unsigned char b0 = d->have_first[i] ? d->first[i] : in[0];
        unsigned char b1 = d->have_first[i] ? in[0] : in[1];
        bool zlib_header = (b0 & 0x0f) == 8 && ((b0 << 8) | b1) % 31 == 0;
        if (begin(d, i, !d->gzip[i] && !zlib_header) == -1) {
            return -1;
        }
        if (d->have_first[i] && inflate_into(d, i, &d->first[i], 1, out) == -1) {
            return -1;
        }
    }
    z_stream *z = &d->z[i];
    z->next_in = (Bytef *)in;
    z->avail_in = (uInt)len;
    for (;;) {
        if (!hx_buf_reserve(out, 16384)) {
            return -1;
        }
        z->next_out = (Bytef *)out->data + out->len;
        z->avail_out = (uInt)(out->cap - out->len);
        int rc = inflate(z, Z_NO_FLUSH);
        out->len = out->cap - z->avail_out;
        if (rc == Z_STREAM_END) {
            d->ended[i] = true;
            return 0;
        }
        if (rc == Z_BUF_ERROR) {
            return 0;
        }
        if (rc != Z_OK) {
            return -1;
        }
        if (z->avail_in == 0 && z->avail_out != 0) {
            return 0;
        }
    }
}

int hx_decoder_feed(hx_decoder *d, const char *in, size_t len, hx_buf *out) {
    const unsigned char *data = (const unsigned char *)in;
    for (int i = 0; i < d->count; i++) {
        hx_buf *to = i == d->count - 1 ? out : &d->tmp[i % 2];
        if (to != out) {
            to->len = 0;
        }
        if (inflate_into(d, i, data, len, to) == -1) {
            return -1;
        }
        data = (const unsigned char *)to->data;
        len = to->len;
    }
    return 0;
}

void hx_decoder_end(hx_decoder *d) {
    for (int i = 0; i < d->count; i++) {
        if (d->started[i]) {
            inflateEnd(&d->z[i]);
        }
        d->started[i] = d->ended[i] = d->have_first[i] = false;
    }
    d->count = 0;
}

void hx_decoder_free(hx_decoder *d) {
    hx_decoder_end(d);
    hx_buf_free(&d->tmp[0]);
    hx_buf_free(&d->tmp[1]);
}
