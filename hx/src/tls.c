#include "tls.h"

#include "h3.h"

#include <arpa/inet.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_SESSIONS 256

int hx_tls_on_session(SSL *ssl, SSL_SESSION *s);

static bool same(const char *a, const char *b) { return a == b || (a != NULL && b != NULL && strcmp(a, b) == 0); }

static char *dup_or_null(const char *s) { return s != NULL ? strdup(s) : NULL; }

static bool matches(const hx_tls_ctx *x, const hx_request *r) {
    return x->verify == r->verify && x->http == r->http && same(x->ca_file, r->ca_file) &&
           same(x->ca_path, r->ca_path) && same(x->cert_file, r->cert_file) && same(x->key_file, r->key_file) &&
           (x->ca_pem != NULL) == (r->ca_pem != NULL) &&
           (r->ca_pem == NULL || (x->ca_pem_len == r->ca_pem_len && memcmp(x->ca_pem, r->ca_pem, r->ca_pem_len) == 0));
}

static bool load_pem(SSL_CTX *ctx, const char *pem, size_t len) {
    X509_STORE *store = SSL_CTX_get_cert_store(ctx);
    BIO *bio = BIO_new_mem_buf(len > 0 ? pem : "", (int)len);
    if (bio == NULL) {
        return false;
    }
    X509 *cert;
    while ((cert = PEM_read_bio_X509(bio, NULL, NULL, NULL)) != NULL) {
        X509_STORE_add_cert(store, cert);
        X509_free(cert);
    }
    ERR_clear_error();
    BIO_free(bio);
    return true;
}

static SSL_CTX *make_ctx(const hx_request *r, bool quic, hx_error *error) {
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == NULL) {
        *error = HX_ERR_NOMEM;
        return NULL;
    }
    static const unsigned char h1[] = "\x08http/1.1", both[] = "\x02h2\x08http/1.1", h3[] = "\x02h3";
    if (quic) {
        SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
        SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
        SSL_CTX_sess_set_new_cb(ctx, hx_tls_on_session);
        SSL_CTX_set_alpn_protos(ctx, h3, sizeof(h3) - 1);
        if (hx_h3_configure(ctx) != 0) {
            SSL_CTX_free(ctx);
            *error = HX_ERR_NOMEM;
            return NULL;
        }
    } else {
        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
        SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
        SSL_CTX_sess_set_new_cb(ctx, hx_tls_on_session);
        if (r->http != HX_HTTP1) {
            SSL_CTX_set_alpn_protos(ctx, both, sizeof(both) - 1);
        } else {
            SSL_CTX_set_alpn_protos(ctx, h1, sizeof(h1) - 1);
        }
    }
    SSL_CTX_set_verify(ctx, r->verify ? SSL_VERIFY_PEER : SSL_VERIFY_NONE, NULL);
    bool ok = true;
    if (r->verify) {
        if (r->ca_pem != NULL) {
            ok = load_pem(ctx, r->ca_pem, r->ca_pem_len);
        } else if (r->ca_file != NULL || r->ca_path != NULL) {
            ok = SSL_CTX_load_verify_locations(ctx, r->ca_file, r->ca_path) == 1;
        } else {
            ok = SSL_CTX_set_default_verify_paths(ctx) == 1;
        }
        if (!ok) {
            *error = HX_ERR_TLS_CA;
        }
    }
    if (ok && (r->cert_file != NULL || r->key_file != NULL)) {
        ok =
            r->cert_file != NULL && r->key_file != NULL && SSL_CTX_use_certificate_chain_file(ctx, r->cert_file) == 1 &&
            SSL_CTX_use_PrivateKey_file(ctx, r->key_file, SSL_FILETYPE_PEM) == 1 && SSL_CTX_check_private_key(ctx) == 1;
        if (!ok) {
            *error = HX_ERR_TLS_CERT;
        }
    }
    ERR_clear_error();
    if (!ok) {
        SSL_CTX_free(ctx);
        return NULL;
    }
    return ctx;
}

hx_tls_ctx *hx_tls_ctx_for(hx_tls *t, const hx_request *r, hx_error *error) {
    for (hx_tls_ctx *x = t->ctxs; x != NULL; x = x->next) {
        if (matches(x, r)) {
            return x;
        }
    }
    hx_tls_ctx *x = calloc(1, sizeof(hx_tls_ctx));
    if (x == NULL) {
        *error = HX_ERR_NOMEM;
        return NULL;
    }
    x->ctx = make_ctx(r, false, error);
    if (x->ctx != NULL && r->http == HX_HTTP3) {
        x->quic = make_ctx(r, true, error);
        if (x->quic == NULL) {
            SSL_CTX_free(x->ctx);
            x->ctx = NULL;
        }
    }
    x->verify = r->verify;
    x->http = r->http;
    x->ca_file = dup_or_null(r->ca_file);
    x->ca_path = dup_or_null(r->ca_path);
    x->cert_file = dup_or_null(r->cert_file);
    x->key_file = dup_or_null(r->key_file);
    if (r->ca_pem != NULL && (x->ca_pem = malloc(r->ca_pem_len + 1)) != NULL) {
        memcpy(x->ca_pem, r->ca_pem, r->ca_pem_len);
        x->ca_pem_len = r->ca_pem_len;
    }
    if (x->ctx == NULL) {
        free(x->ca_file);
        free(x->ca_path);
        free(x->cert_file);
        free(x->key_file);
        free(x->ca_pem);
        free(x);
        return NULL;
    }
    x->id = ++t->next_id;
    x->next = t->ctxs;
    t->ctxs = x;
    return x;
}

static bool is_ip(const char *host) {
    unsigned char buf[16];
    return strchr(host, ':') != NULL || inet_pton(AF_INET, host, buf) == 1;
}

static SSL *new_ssl(SSL_CTX *ctx, const hx_tls_ctx *x, const char *host) {
    SSL *ssl = SSL_new(ctx);
    if (ssl == NULL) {
        return NULL;
    }
    bool ip = is_ip(host);
    if (!ip) {
        SSL_set_tlsext_host_name(ssl, host);
    }
    if (x->verify) {
        X509_VERIFY_PARAM *param = SSL_get0_param(ssl);
        if (ip) {
            X509_VERIFY_PARAM_set1_ip_asc(param, host);
        } else {
            X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
            X509_VERIFY_PARAM_set1_host(param, host, 0);
        }
    }
    return ssl;
}

static int conn_index = -1;
static pthread_once_t conn_once = PTHREAD_ONCE_INIT;

static void make_conn_index(void) { conn_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL); }

void *hx_tls_conn(const SSL *ssl) { return conn_index >= 0 ? SSL_get_ex_data(ssl, conn_index) : NULL; }

static SSL *resuming(hx_tls *t, SSL_CTX *ctx, hx_tls_ctx *x, const char *host, const char *session_key, void *conn) {
    pthread_once(&conn_once, make_conn_index);
    SSL *ssl = conn_index >= 0 ? new_ssl(ctx, x, host) : NULL;
    if (ssl == NULL) {
        return NULL;
    }
    SSL_set_ex_data(ssl, conn_index, conn);
    for (hx_tls_session *s = t->sessions; s != NULL; s = s->next) {
        if (strcmp(s->key, session_key) == 0) {
            SSL_set_session(ssl, s->session);
            break;
        }
    }
    return ssl;
}

SSL *hx_tls_new(hx_tls *t, hx_tls_ctx *x, const char *host, const char *session_key, void *conn) {
    return resuming(t, x->ctx, x, host, session_key, conn);
}

SSL *hx_tls_new_quic(hx_tls *t, hx_tls_ctx *x, const char *host, const char *session_key, void *conn) {
    return x->quic != NULL ? resuming(t, x->quic, x, host, session_key, conn) : NULL;
}

void hx_tls_save(hx_tls *t, const char *key, SSL_SESSION *session) {
    for (hx_tls_session **p = &t->sessions; *p != NULL; p = &(*p)->next) {
        if (strcmp((*p)->key, key) == 0) {
            hx_tls_session *s = *p;
            *p = s->next;
            SSL_SESSION_free(s->session);
            s->session = session;
            s->next = t->sessions;
            t->sessions = s;
            return;
        }
    }
    if (t->session_count >= MAX_SESSIONS) {
        hx_tls_session **p = &t->sessions;
        while ((*p)->next != NULL) {
            p = &(*p)->next;
        }
        SSL_SESSION_free((*p)->session);
        free(*p);
        *p = NULL;
        t->session_count--;
    }
    hx_tls_session *s = calloc(1, sizeof(hx_tls_session));
    if (s == NULL) {
        SSL_SESSION_free(session);
        return;
    }
    snprintf(s->key, sizeof(s->key), "%s", key);
    s->session = session;
    s->next = t->sessions;
    t->sessions = s;
    t->session_count++;
}

void hx_tls_free(hx_tls *t) {
    while (t->ctxs != NULL) {
        hx_tls_ctx *x = t->ctxs;
        t->ctxs = x->next;
        SSL_CTX_free(x->ctx);
        SSL_CTX_free(x->quic);
        free(x->ca_file);
        free(x->ca_path);
        free(x->cert_file);
        free(x->key_file);
        free(x->ca_pem);
        free(x);
    }
    while (t->sessions != NULL) {
        hx_tls_session *s = t->sessions;
        t->sessions = s->next;
        SSL_SESSION_free(s->session);
        free(s);
    }
    t->session_count = 0;
}
