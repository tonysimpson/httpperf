#include "h3.h"

#ifdef HX_H3

#include <errno.h>
#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_boringssl.h>
#include <openssl/rand.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#define PACKET_CAP 65536

typedef struct block {
    struct block *next;
    size_t len;
    char data[];
} block;

typedef struct h3s {
    hx_h3 *h;
    int64_t id;
    void *user;
    block *head, *tail, *next_out;
    size_t out_off;
    uint64_t acked;
    bool end;
    bool waiting;
    struct h3s *prev, *next;
} h3s;

struct hx_h3 {
    int fd;
    SSL *ssl;
    ngtcp2_crypto_conn_ref ref;
    ngtcp2_conn *conn;
    nghttp3_conn *h3;
    const hx_h3_handler *handler;
    void *user;
    struct sockaddr_storage local, remote;
    socklen_t local_len, remote_len;
    int64_t ts;
    bool ready;
    bool goaway;
    bool nomem;
    h3s *streams;
    size_t pending;
    uint8_t in[PACKET_CAP];
    uint8_t out[PACKET_CAP];
};

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) { return ((hx_h3 *)ref->user_data)->conn; }

static void rand_cb(uint8_t *dest, size_t len, const ngtcp2_rand_ctx *ctx) {
    if (RAND_bytes(dest, (int)len) != 1) {
        memset(dest, 0, len);
    }
}

static int new_cid(ngtcp2_conn *conn, ngtcp2_cid *cid, ngtcp2_stateless_reset_token *token, size_t len, void *user) {
    if (RAND_bytes(cid->data, (int)len) != 1 || RAND_bytes(token->data, sizeof(token->data)) != 1) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    cid->datalen = len;
    return 0;
}

static void stream_free(h3s *s) {
    hx_h3 *h = s->h;
    if (s->prev != NULL) {
        s->prev->next = s->next;
    } else {
        h->streams = s->next;
    }
    if (s->next != NULL) {
        s->next->prev = s->prev;
    }
    while (s->head != NULL) {
        block *b = s->head;
        s->head = b->next;
        free(b);
    }
    free(s);
}

static void extend(hx_h3 *h, int64_t id, size_t n) {
    ngtcp2_conn_extend_max_stream_offset(h->conn, id, n);
    ngtcp2_conn_extend_max_offset(h->conn, n);
}

static int on_acked(nghttp3_conn *conn, int64_t id, uint64_t len, void *user, void *stream) {
    h3s *s = stream;
    if (s == NULL) {
        return 0;
    }
    s->acked += len;
    while (s->head != NULL && s->head != s->next_out && s->acked >= s->head->len) {
        block *b = s->head;
        s->acked -= b->len;
        s->head = b->next;
        if (s->tail == b) {
            s->tail = NULL;
        }
        free(b);
    }
    return 0;
}

static int on_stream_close(nghttp3_conn *conn, int64_t id, uint64_t code, void *user, void *stream) {
    h3s *s = stream;
    if (s == NULL) {
        return 0;
    }
    hx_h3 *h = s->h;
    void *mine = s->user;
    s->user = NULL;
    if (mine != NULL) {
        h->handler->closed(h->user, mine, code);
    }
    stream_free(s);
    return 0;
}

static int on_data(nghttp3_conn *conn, int64_t id, const uint8_t *data, size_t len, void *user, void *stream) {
    hx_h3 *h = user;
    h3s *s = stream;
    if (s == NULL || s->user == NULL) {
        extend(h, id, len);
        return 0;
    }
    h->handler->data(h->user, s->user, data, len);
    return 0;
}

static int on_deferred(nghttp3_conn *conn, int64_t id, size_t consumed, void *user, void *stream) {
    extend(user, id, consumed);
    return 0;
}

static int on_header(nghttp3_conn *conn, int64_t id, int32_t token, nghttp3_rcbuf *name, nghttp3_rcbuf *value,
                     uint8_t flags, void *user, void *stream) {
    hx_h3 *h = user;
    h3s *s = stream;
    if (s == NULL || s->user == NULL) {
        return 0;
    }
    nghttp3_vec n = nghttp3_rcbuf_get_buf(name), v = nghttp3_rcbuf_get_buf(value);
    h->handler->header(h->user, s->user, n.base, n.len, v.base, v.len);
    return 0;
}

static int on_headers_done(nghttp3_conn *conn, int64_t id, int fin, void *user, void *stream) {
    hx_h3 *h = user;
    h3s *s = stream;
    if (s != NULL && s->user != NULL) {
        h->handler->headers_done(h->user, s->user);
    }
    return 0;
}

static int on_stop_sending(nghttp3_conn *conn, int64_t id, uint64_t code, void *user, void *stream) {
    int rv = ngtcp2_conn_shutdown_stream_write(((hx_h3 *)user)->conn, 0, id, code);
    return rv != 0 && rv != NGTCP2_ERR_STREAM_NOT_FOUND ? NGHTTP3_ERR_CALLBACK_FAILURE : 0;
}

static int on_reset(nghttp3_conn *conn, int64_t id, uint64_t code, void *user, void *stream) {
    int rv = ngtcp2_conn_shutdown_stream_read(((hx_h3 *)user)->conn, 0, id, code);
    return rv != 0 && rv != NGTCP2_ERR_STREAM_NOT_FOUND ? NGHTTP3_ERR_CALLBACK_FAILURE : 0;
}

static int on_shutdown(nghttp3_conn *conn, int64_t id, void *user) {
    ((hx_h3 *)user)->goaway = true;
    return 0;
}

static const nghttp3_callbacks h3_callbacks = {
    .acked_stream_data = on_acked,
    .stream_close = on_stream_close,
    .recv_data = on_data,
    .deferred_consume = on_deferred,
    .recv_header = on_header,
    .end_headers = on_headers_done,
    .stop_sending = on_stop_sending,
    .reset_stream = on_reset,
    .shutdown = on_shutdown,
};

static int start_h3(hx_h3 *h) {
    nghttp3_settings settings;
    nghttp3_settings_default(&settings);
    if (nghttp3_conn_client_new(&h->h3, &h3_callbacks, &settings, NULL, h) != 0) {
        h->h3 = NULL;
        return -1;
    }
    int64_t control, encoder, decoder;
    if (ngtcp2_conn_open_uni_stream(h->conn, &control, NULL) != 0 ||
        nghttp3_conn_bind_control_stream(h->h3, control) != 0 ||
        ngtcp2_conn_open_uni_stream(h->conn, &encoder, NULL) != 0 ||
        ngtcp2_conn_open_uni_stream(h->conn, &decoder, NULL) != 0 ||
        nghttp3_conn_bind_qpack_streams(h->h3, encoder, decoder) != 0) {
        return -1;
    }
    return 0;
}

static int on_rx_key(ngtcp2_conn *conn, ngtcp2_encryption_level level, void *user) {
    hx_h3 *h = user;
    if (level != NGTCP2_ENCRYPTION_LEVEL_1RTT || h->h3 != NULL) {
        return 0;
    }
    return start_h3(h) == 0 ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

static int on_handshake_done(ngtcp2_conn *conn, void *user) {
    ((hx_h3 *)user)->ready = true;
    return 0;
}

static int on_stream_data(ngtcp2_conn *conn, uint32_t flags, int64_t id, uint64_t offset, const uint8_t *data,
                          size_t len, void *user, void *stream) {
    hx_h3 *h = user;
    if (h->h3 == NULL) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    nghttp3_ssize n =
        nghttp3_conn_read_stream2(h->h3, id, data, len, (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0, (uint64_t)h->ts);
    if (n < 0) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    extend(h, id, (size_t)n);
    return 0;
}

static int on_acked_offset(ngtcp2_conn *conn, int64_t id, uint64_t offset, uint64_t len, void *user, void *stream) {
    hx_h3 *h = user;
    int rv = h->h3 != NULL ? nghttp3_conn_add_ack_offset(h->h3, id, len) : 0;
    return rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND ? NGTCP2_ERR_CALLBACK_FAILURE : 0;
}

static int on_quic_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t id, uint64_t code, void *user,
                                void *stream) {
    hx_h3 *h = user;
    if (h->h3 == NULL) {
        return 0;
    }
    if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET)) {
        code = NGHTTP3_H3_NO_ERROR;
    }
    int rv = nghttp3_conn_close_stream(h->h3, id, code);
    return rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND ? NGTCP2_ERR_CALLBACK_FAILURE : 0;
}

static int on_quic_reset(ngtcp2_conn *conn, int64_t id, uint64_t size, uint64_t code, void *user, void *stream) {
    hx_h3 *h = user;
    int rv = h->h3 != NULL ? nghttp3_conn_shutdown_stream_read(h->h3, id) : 0;
    return rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND ? NGTCP2_ERR_CALLBACK_FAILURE : 0;
}

static int on_quic_stop_sending(ngtcp2_conn *conn, int64_t id, uint64_t code, void *user, void *stream) {
    hx_h3 *h = user;
    int rv = h->h3 != NULL ? nghttp3_conn_shutdown_stream_read(h->h3, id) : 0;
    return rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND ? NGTCP2_ERR_CALLBACK_FAILURE : 0;
}

static int on_max_stream_data(ngtcp2_conn *conn, int64_t id, uint64_t max, void *user, void *stream) {
    hx_h3 *h = user;
    int rv = h->h3 != NULL ? nghttp3_conn_unblock_stream(h->h3, id) : 0;
    return rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND ? NGTCP2_ERR_CALLBACK_FAILURE : 0;
}

static const ngtcp2_callbacks quic_callbacks = {
    .client_initial = ngtcp2_crypto_client_initial_cb,
    .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
    .handshake_completed = on_handshake_done,
    .encrypt = ngtcp2_crypto_encrypt_cb,
    .decrypt = ngtcp2_crypto_decrypt_cb,
    .hp_mask = ngtcp2_crypto_hp_mask_cb,
    .recv_stream_data = on_stream_data,
    .acked_stream_data_offset = on_acked_offset,
    .stream_close = on_quic_stream_close,
    .recv_retry = ngtcp2_crypto_recv_retry_cb,
    .rand = rand_cb,
    .update_key = ngtcp2_crypto_update_key_cb,
    .stream_reset = on_quic_reset,
    .extend_max_stream_data = on_max_stream_data,
    .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
    .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
    .stream_stop_sending = on_quic_stop_sending,
    .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
    .recv_rx_key = on_rx_key,
    .get_new_connection_id2 = new_cid,
    .get_path_challenge_data2 = ngtcp2_crypto_get_path_challenge_data2_cb,
};

bool hx_h3_available(void) { return true; }

int hx_h3_configure(SSL_CTX *ctx) { return ngtcp2_crypto_boringssl_configure_client_context(ctx); }

static ngtcp2_path path_of(hx_h3 *h) {
    return (ngtcp2_path){
        {(struct sockaddr *)&h->local, h->local_len}, {(struct sockaddr *)&h->remote, h->remote_len}, NULL};
}

hx_h3 *hx_h3_new(int fd, SSL *ssl, const hx_h3_handler *handler, void *user, int64_t now, int64_t handshake_timeout) {
    hx_h3 *h = calloc(1, sizeof(hx_h3));
    if (h == NULL) {
        return NULL;
    }
    h->fd = fd;
    h->handler = handler;
    h->user = user;
    h->local_len = sizeof(h->local);
    h->remote_len = sizeof(h->remote);
    ngtcp2_cid dcid = {.datalen = 18}, scid = {.datalen = 8};
    if (getsockname(fd, (struct sockaddr *)&h->local, &h->local_len) != 0 ||
        getpeername(fd, (struct sockaddr *)&h->remote, &h->remote_len) != 0 ||
        RAND_bytes(dcid.data, (int)dcid.datalen) != 1 || RAND_bytes(scid.data, (int)scid.datalen) != 1) {
        free(h);
        return NULL;
    }
    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = (ngtcp2_tstamp)now;
    if (handshake_timeout > 0) {
        settings.handshake_timeout = (ngtcp2_duration)handshake_timeout;
    }
    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_stream_data_bidi_local = 1 << 20;
    params.initial_max_stream_data_uni = 1 << 20;
    params.initial_max_data = 1 << 26;
    params.initial_max_streams_bidi = 0;
    params.initial_max_streams_uni = 100;
    ngtcp2_path path = path_of(h);
    if (ngtcp2_conn_client_new(&h->conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1, &quic_callbacks, &settings, &params,
                               NULL, h) != 0) {
        free(h);
        return NULL;
    }
    h->ssl = ssl;
    h->ref.get_conn = get_conn;
    h->ref.user_data = h;
    SSL_set_app_data(ssl, &h->ref);
    SSL_set_connect_state(ssl);
    ngtcp2_conn_set_tls_native_handle(h->conn, ssl);
    return h;
}

static int failure(hx_h3 *h, int rv) {
    if (h->nomem || rv == NGTCP2_ERR_NOMEM) {
        return HX_H3_NOMEM;
    }
    if (rv == NGTCP2_ERR_DRAINING || rv == NGTCP2_ERR_CLOSING || rv == NGTCP2_ERR_DROP_CONN ||
        rv == NGTCP2_ERR_IDLE_CLOSE) {
        return HX_H3_CLOSED;
    }
    if (rv == NGTCP2_ERR_CRYPTO) {
        return HX_H3_TLS;
    }
    if (rv == NGTCP2_ERR_HANDSHAKE_TIMEOUT) {
        return HX_H3_TIMEOUT;
    }
    return HX_H3_PROTOCOL;
}

int hx_h3_read(hx_h3 *h, int64_t now, int *sys_errno) {
    for (;;) {
        ssize_t n = recv(h->fd, h->in, sizeof(h->in), 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return HX_H3_OK;
        }
        if (n < 0) {
            *sys_errno = errno;
            return HX_H3_FAILED;
        }
        h->ts = now;
        ngtcp2_path path = path_of(h);
        ngtcp2_pkt_info pi = {0};
        int rv = ngtcp2_conn_read_pkt(h->conn, &path, &pi, h->in, (size_t)n, (ngtcp2_tstamp)now);
        if (rv != 0) {
            return failure(h, rv);
        }
    }
}

static int flush_pending(hx_h3 *h, int *sys_errno) {
    while (h->pending > 0) {
        ssize_t n = send(h->fd, h->out, h->pending, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS)) {
            return HX_H3_BLOCKED;
        }
        if (n < 0) {
            *sys_errno = errno;
            return HX_H3_FAILED;
        }
        h->pending = 0;
    }
    return HX_H3_OK;
}

int hx_h3_write(hx_h3 *h, int64_t now, int *sys_errno) {
    int rc = flush_pending(h, sys_errno);
    if (rc != HX_H3_OK) {
        return rc;
    }
    size_t cap = ngtcp2_conn_get_path_max_tx_udp_payload_size(h->conn);
    for (;;) {
        int64_t id = -1;
        int fin = 0;
        nghttp3_vec vec[16];
        nghttp3_ssize count = 0;
        if (h->h3 != NULL && ngtcp2_conn_get_max_data_left(h->conn) > 0) {
            count = nghttp3_conn_writev_stream(h->h3, &id, &fin, vec, 16);
            if (count < 0) {
                return h->nomem ? HX_H3_NOMEM : HX_H3_PROTOCOL;
            }
        }
        ngtcp2_ssize used = -1;
        uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
        ngtcp2_ssize n = ngtcp2_conn_writev_stream(h->conn, NULL, NULL, h->out, cap, &used, flags, id,
                                                   (const ngtcp2_vec *)vec, (size_t)count, (ngtcp2_tstamp)now);
        if (n == NGTCP2_ERR_STREAM_DATA_BLOCKED) {
            nghttp3_conn_block_stream(h->h3, id);
            continue;
        }
        if (n == NGTCP2_ERR_STREAM_SHUT_WR) {
            nghttp3_conn_shutdown_stream_write(h->h3, id);
            continue;
        }
        if (used >= 0 && id >= 0 && nghttp3_conn_add_write_offset(h->h3, id, (size_t)used) != 0) {
            return HX_H3_PROTOCOL;
        }
        if (n == NGTCP2_ERR_WRITE_MORE) {
            continue;
        }
        if (n < 0) {
            return failure(h, (int)n);
        }
        if (n == 0) {
            ngtcp2_conn_update_pkt_tx_time(h->conn, (ngtcp2_tstamp)now);
            return HX_H3_OK;
        }
        h->pending = (size_t)n;
        rc = flush_pending(h, sys_errno);
        if (rc != HX_H3_OK) {
            return rc;
        }
    }
}

int64_t hx_h3_expiry(const hx_h3 *h) {
    ngtcp2_tstamp t = ngtcp2_conn_get_expiry2(h->conn);
    return t == UINT64_MAX || t > INT64_MAX ? 0 : (int64_t)t;
}

int hx_h3_expire(hx_h3 *h, int64_t now) {
    int rv = ngtcp2_conn_handle_expiry(h->conn, (ngtcp2_tstamp)now);
    return rv == 0 ? HX_H3_OK : failure(h, rv);
}

bool hx_h3_ready(const hx_h3 *h) { return h->ready && h->h3 != NULL; }

bool hx_h3_goaway(const hx_h3 *h) { return h->goaway; }

bool hx_h3_can_open(const hx_h3 *h) {
    return hx_h3_ready(h) && !h->goaway && ngtcp2_conn_get_streams_bidi_left2(h->conn) > 0 &&
           !ngtcp2_conn_in_closing_period2(h->conn) && !ngtcp2_conn_in_draining_period2(h->conn);
}

static nghttp3_ssize read_body(nghttp3_conn *conn, int64_t id, nghttp3_vec *vec, size_t veccnt, uint32_t *pflags,
                               void *user, void *stream) {
    hx_h3 *h = user;
    h3s *s = stream;
    size_t n = 0;
    while (n < veccnt && s->next_out != NULL) {
        block *b = s->next_out;
        if (b->len > s->out_off) {
            vec[n].base = (uint8_t *)b->data + s->out_off;
            vec[n].len = b->len - s->out_off;
            n++;
        }
        s->next_out = b->next;
        s->out_off = 0;
    }
    if (s->next_out == NULL) {
        if (s->end) {
            *pflags |= NGHTTP3_DATA_FLAG_EOF;
        } else if (n == 0) {
            s->waiting = true;
            if (s->user != NULL && h->handler->want_body != NULL) {
                h->handler->want_body(h->user, s->user);
            }
            return NGHTTP3_ERR_WOULDBLOCK;
        } else if (s->user != NULL && h->handler->want_body != NULL) {
            h->handler->want_body(h->user, s->user);
        }
    }
    return (nghttp3_ssize)n;
}

int64_t hx_h3_open(hx_h3 *h, const nghttp2_nv *nva, size_t n, bool body, void *stream) {
    h3s *s = calloc(1, sizeof(h3s));
    nghttp3_nv *nv = calloc(n > 0 ? n : 1, sizeof(nghttp3_nv));
    if (s == NULL || nv == NULL) {
        free(s);
        free(nv);
        return -1;
    }
    for (size_t i = 0; i < n; i++) {
        nv[i] = (nghttp3_nv){nva[i].name, nva[i].value, nva[i].namelen, nva[i].valuelen, NGHTTP3_NV_FLAG_NONE};
    }
    int64_t id;
    if (ngtcp2_conn_open_bidi_stream(h->conn, &id, NULL) != 0) {
        free(s);
        free(nv);
        return -1;
    }
    s->h = h;
    s->id = id;
    s->user = stream;
    nghttp3_data_reader reader = {read_body};
    if (nghttp3_conn_submit_request(h->h3, id, nv, n, body ? &reader : NULL, s) != 0) {
        free(s);
        free(nv);
        ngtcp2_conn_shutdown_stream(h->conn, 0, id, NGHTTP3_H3_INTERNAL_ERROR);
        return -1;
    }
    free(nv);
    s->next = h->streams;
    if (h->streams != NULL) {
        h->streams->prev = s;
    }
    h->streams = s;
    return id;
}

int hx_h3_send(hx_h3 *h, int64_t id, const char *data, size_t len, bool end) {
    h3s *s = nghttp3_conn_get_stream_user_data(h->h3, id);
    if (s == NULL || s->end) {
        return -1;
    }
    if (len > 0) {
        block *b = malloc(sizeof(block) + len);
        if (b == NULL) {
            return -1;
        }
        b->next = NULL;
        b->len = len;
        memcpy(b->data, data, len);
        if (s->tail != NULL) {
            s->tail->next = b;
        } else {
            s->head = b;
        }
        s->tail = b;
        if (s->next_out == NULL) {
            s->next_out = b;
            s->out_off = 0;
        }
    }
    s->end = end;
    if (s->waiting) {
        s->waiting = false;
        nghttp3_conn_resume_stream(h->h3, id);
    }
    return 0;
}

void hx_h3_consume(hx_h3 *h, int64_t id, size_t n) { extend(h, id, n); }

void hx_h3_reset(hx_h3 *h, int64_t id) { ngtcp2_conn_shutdown_stream(h->conn, 0, id, NGHTTP3_H3_INTERNAL_ERROR); }

void hx_h3_cancel(hx_h3 *h, int64_t id) {
    h3s *s = h->h3 != NULL ? nghttp3_conn_get_stream_user_data(h->h3, id) : NULL;
    if (s != NULL) {
        s->user = NULL;
    }
    ngtcp2_conn_shutdown_stream(h->conn, 0, id, NGHTTP3_H3_REQUEST_CANCELLED);
}

SSL *hx_h3_ssl(const hx_h3 *h) { return h->ssl; }

void hx_h3_close(hx_h3 *h, int64_t now) {
    if (ngtcp2_conn_in_closing_period2(h->conn) || ngtcp2_conn_in_draining_period2(h->conn)) {
        return;
    }
    ngtcp2_ccerr err;
    ngtcp2_ccerr_default(&err);
    if (h->h3 != NULL) {
        ngtcp2_ccerr_set_application_error(&err, NGHTTP3_H3_NO_ERROR, NULL, 0);
    }
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_ssize n =
        ngtcp2_conn_write_connection_close(h->conn, &ps.path, NULL, h->out, sizeof(h->out), &err, (ngtcp2_tstamp)now);
    if (n > 0) {
        send(h->fd, h->out, (size_t)n, MSG_DONTWAIT);
    }
}

void hx_h3_free(hx_h3 *h) {
    while (h->streams != NULL) {
        stream_free(h->streams);
    }
    nghttp3_conn_del(h->h3);
    ngtcp2_conn_del(h->conn);
    if (h->ssl != NULL) {
        SSL_set_app_data(h->ssl, NULL);
        SSL_free(h->ssl);
    }
    free(h);
}

#else

bool hx_h3_available(void) { return false; }
int hx_h3_configure(SSL_CTX *ctx) { return -1; }
hx_h3 *hx_h3_new(int fd, SSL *ssl, const hx_h3_handler *handler, void *user, int64_t now, int64_t handshake_timeout) {
    return NULL;
}
int hx_h3_read(hx_h3 *h, int64_t now, int *sys_errno) { return HX_H3_FAILED; }
int hx_h3_write(hx_h3 *h, int64_t now, int *sys_errno) { return HX_H3_FAILED; }
int64_t hx_h3_expiry(const hx_h3 *h) { return 0; }
int hx_h3_expire(hx_h3 *h, int64_t now) { return HX_H3_FAILED; }
bool hx_h3_ready(const hx_h3 *h) { return false; }
bool hx_h3_goaway(const hx_h3 *h) { return true; }
bool hx_h3_can_open(const hx_h3 *h) { return false; }
int64_t hx_h3_open(hx_h3 *h, const nghttp2_nv *nva, size_t n, bool body, void *stream) { return -1; }
int hx_h3_send(hx_h3 *h, int64_t id, const char *data, size_t len, bool end) { return -1; }
void hx_h3_consume(hx_h3 *h, int64_t id, size_t n) {}
void hx_h3_reset(hx_h3 *h, int64_t id) {}
void hx_h3_cancel(hx_h3 *h, int64_t id) {}
SSL *hx_h3_ssl(const hx_h3 *h) { return NULL; }
void hx_h3_close(hx_h3 *h, int64_t now) {}
void hx_h3_free(hx_h3 *h) {}

#endif
