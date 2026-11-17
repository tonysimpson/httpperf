#ifndef HX_DECODE_H
#define HX_DECODE_H

#include <stdbool.h>
#include <stddef.h>
#include <zlib.h>

#include "buf.h"

#define HX_MAX_CODINGS 4

typedef struct {
    z_stream z[HX_MAX_CODINGS];
    bool gzip[HX_MAX_CODINGS];
    bool started[HX_MAX_CODINGS];
    bool ended[HX_MAX_CODINGS];
    unsigned char first[HX_MAX_CODINGS];
    bool have_first[HX_MAX_CODINGS];
    int count;
    hx_buf tmp[2];
} hx_decoder;

int hx_decoder_start(hx_decoder *d, const char *value, size_t len);
int hx_decoder_feed(hx_decoder *d, const char *in, size_t len, hx_buf *out);
void hx_decoder_end(hx_decoder *d);
void hx_decoder_free(hx_decoder *d);

#endif
