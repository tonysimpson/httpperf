#ifndef HX_TLS_H
#define HX_TLS_H

#include <openssl/ssl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../include/hx.h"

typedef struct hx_tls_ctx {
    SSL_CTX *ctx;
    SSL_CTX *quic;
    uint64_t id;
    bool verify;
    hx_http http;
    char *ca_file, *ca_path, *ca_pem, *cert_file, *key_file;
    size_t ca_pem_len;
    struct hx_tls_ctx *next;
} hx_tls_ctx;

typedef struct hx_tls_session {
    char key[320];
    SSL_SESSION *session;
    struct hx_tls_session *next;
} hx_tls_session;

typedef struct {
    hx_tls_ctx *ctxs;
    uint64_t next_id;
    hx_tls_session *sessions;
    size_t session_count;
} hx_tls;

hx_tls_ctx *hx_tls_ctx_for(hx_tls *t, const hx_request *r, hx_error *error);
SSL *hx_tls_new(hx_tls *t, hx_tls_ctx *x, const char *host, const char *session_key, void *conn);
SSL *hx_tls_new_quic(hx_tls *t, hx_tls_ctx *x, const char *host, const char *session_key, void *conn);
void *hx_tls_conn(const SSL *ssl);
void hx_tls_save(hx_tls *t, const char *key, SSL_SESSION *s);
void hx_tls_free(hx_tls *t);

#endif
