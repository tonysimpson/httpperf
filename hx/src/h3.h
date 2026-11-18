#ifndef HX_H3_H
#define HX_H3_H

#include <openssl/ssl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "ng.h"

typedef struct hx_h3 hx_h3;

typedef struct {
    void (*header)(void *user, void *stream, const uint8_t *name, size_t namelen, const uint8_t *value,
                   size_t valuelen);
    void (*headers_done)(void *user, void *stream);
    void (*data)(void *user, void *stream, const uint8_t *data, size_t len);
    void (*want_body)(void *user, void *stream);
    void (*closed)(void *user, void *stream, uint64_t code);
} hx_h3_handler;

enum {
    HX_H3_OK = 0,
    HX_H3_BLOCKED = 1,
    HX_H3_CLOSED = -1,
    HX_H3_FAILED = -2,
    HX_H3_TLS = -3,
    HX_H3_TIMEOUT = -4,
    HX_H3_NOMEM = -5,
    HX_H3_PROTOCOL = -6,
};

enum { HX_H3_REJECTED = 0x010b, HX_H3_NO_ERROR = 0x0100 };

bool hx_h3_available(void);
int hx_h3_configure(SSL_CTX *ctx);
hx_h3 *hx_h3_new(int fd, SSL *ssl, const hx_h3_handler *handler, void *user, int64_t now, int64_t handshake_timeout);
int hx_h3_read(hx_h3 *h, int64_t now, int *sys_errno);
int hx_h3_write(hx_h3 *h, int64_t now, int *sys_errno);
int64_t hx_h3_expiry(const hx_h3 *h);
int hx_h3_expire(hx_h3 *h, int64_t now);
bool hx_h3_ready(const hx_h3 *h);
bool hx_h3_goaway(const hx_h3 *h);
bool hx_h3_can_open(const hx_h3 *h);
int64_t hx_h3_open(hx_h3 *h, const nghttp2_nv *nva, size_t n, bool body, void *stream);
int hx_h3_send(hx_h3 *h, int64_t id, const char *data, size_t len, bool end);
void hx_h3_consume(hx_h3 *h, int64_t id, size_t n);
void hx_h3_reset(hx_h3 *h, int64_t id);
void hx_h3_cancel(hx_h3 *h, int64_t id);
SSL *hx_h3_ssl(const hx_h3 *h);
void hx_h3_close(hx_h3 *h, int64_t now);
void hx_h3_free(hx_h3 *h);

#endif
