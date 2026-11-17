#ifndef HX_H1_H
#define HX_H1_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../vendor/llhttp/llhttp.h"
#include "buf.h"

typedef struct {
    const char *method;
    const char *target;
    const char *host;
    const char *const *headers;
    size_t header_count;
    const char *body;
    size_t body_len;
    int64_t upload_size;
} hx_h1_request;

#define HX_H1_NO_UPLOAD (-2)
#define HX_H1_CHUNKED (-1)

int hx_h1_write_request(hx_buf *out, const hx_h1_request *r);
int hx_h1_write_chunk(hx_buf *out, const char *data, size_t len);

typedef enum { HX_H1_HEADERS = 1, HX_H1_BODY, HX_H1_DONE } hx_h1_event_type;

typedef struct {
    hx_h1_event_type type;
    const char *data;
    size_t len;
} hx_h1_event;

typedef struct {
    llhttp_t parser;
    llhttp_settings_t settings;
    hx_buf head;
    bool line_started;
    bool head_request;
    bool keep_alive;
    int status;
    int version;
    hx_h1_event *events;
    int count;
    int max;
} hx_h1;

void hx_h1_init(hx_h1 *h, bool head_request);
void hx_h1_free(hx_h1 *h);
void hx_h1_next(hx_h1 *h, bool head_request);
int hx_h1_feed(hx_h1 *h, const char *data, size_t len, size_t *used, hx_h1_event *events, int max);
int hx_h1_finish(hx_h1 *h, hx_h1_event *events, int max);
const char *hx_h1_error(const hx_h1 *h);

#define HX_EINVAL (-1)
#define HX_ENOMEM (-2)
#define HX_EPROTO (-3)

#endif
