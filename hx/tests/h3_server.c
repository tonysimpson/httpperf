#include "h3_server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#ifdef HX_H3_OSSL
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#else
#include <ngtcp2/ngtcp2_crypto_boringssl.h>
#endif
#include <nghttp3/nghttp3.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#define SCID_LEN 18
#define SLOW_DELAY (300 * NGTCP2_MILLISECONDS)

typedef struct h3_conn h3_conn;

typedef struct h3_stream {
    struct h3_stream *next;
    h3_conn *owner;
    int64_t id;
    char path[64];
    size_t received;
    char text[32];
    const char *body;
    size_t len;
    ngtcp2_tstamp due;
    bool refuse;
    bool refused;
    bool hold;
} h3_stream;

struct h3_conn {
    h3_conn *next;
    ngtcp2_conn *quic;
    nghttp3_conn *http;
    SSL *ssl;
    ngtcp2_crypto_conn_ref ref;
#ifdef HX_H3_OSSL
    ngtcp2_crypto_ossl_ctx *ossl;
#endif
    struct sockaddr_storage peer;
    socklen_t peer_len;
    ngtcp2_ccerr error;
    uint8_t close_packet[NGTCP2_MAX_UDP_PAYLOAD_SIZE];
    size_t close_len;
    ngtcp2_tstamp close_deadline;
    bool refusing;
    bool hang_up;
    bool dead;
    h3_stream *streams;
};

typedef struct {
    ngtcp2_cid cid;
    h3_conn *conn;
} cid_entry;

static struct {
    int fd;
    int wake[2];
    pthread_t thread;
    SSL_CTX *ctx;
    uint8_t secret[32];
    struct sockaddr_in local;
    h3_conn *conns;
    cid_entry *cids;
    size_t cid_count, cid_cap;
} srv = {.fd = -1, .wake = {-1, -1}};

static int connections;
static int refuse_next;
static char big_body[100000];

static ngtcp2_tstamp now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ngtcp2_tstamp)ts.tv_sec * NGTCP2_SECONDS + (ngtcp2_tstamp)ts.tv_nsec;
}

static void associate(const ngtcp2_cid *cid, h3_conn *c) {
    if (srv.cid_count == srv.cid_cap) {
        srv.cid_cap = srv.cid_cap ? srv.cid_cap * 2 : 16;
        srv.cids = realloc(srv.cids, srv.cid_cap * sizeof(*srv.cids));
    }
    srv.cids[srv.cid_count++] = (cid_entry){*cid, c};
}

static void dissociate(const ngtcp2_cid *cid) {
    for (size_t i = 0; i < srv.cid_count; i++) {
        if (ngtcp2_cid_eq(&srv.cids[i].cid, cid)) {
            srv.cids[i] = srv.cids[--srv.cid_count];
            return;
        }
    }
}

static void dissociate_conn(h3_conn *c) {
    for (size_t i = 0; i < srv.cid_count;) {
        if (srv.cids[i].conn == c) {
            srv.cids[i] = srv.cids[--srv.cid_count];
        } else {
            i++;
        }
    }
}

static h3_conn *find_conn(const uint8_t *dcid, size_t len) {
    ngtcp2_cid cid;
    ngtcp2_cid_init(&cid, dcid, len);
    for (size_t i = 0; i < srv.cid_count; i++) {
        if (ngtcp2_cid_eq(&srv.cids[i].cid, &cid)) {
            return srv.cids[i].conn;
        }
    }
    return NULL;
}

static void free_conn(h3_conn *c) {
    dissociate_conn(c);
    if (c->http != NULL) {
        nghttp3_conn_del(c->http);
    }
    if (c->quic != NULL) {
        ngtcp2_conn_del(c->quic);
    }
#ifdef HX_H3_OSSL
    ngtcp2_crypto_ossl_ctx_del(c->ossl);
#endif
    SSL_free(c->ssl);
    while (c->streams != NULL) {
        h3_stream *s = c->streams;
        c->streams = s->next;
        free(s);
    }
    free(c);
}

static void unlink_stream(h3_stream *s) {
    for (h3_stream **p = &s->owner->streams; *p != NULL; p = &(*p)->next) {
        if (*p == s) {
            *p = s->next;
            return;
        }
    }
}

static void consume(h3_conn *c, int64_t id, size_t n) {
    ngtcp2_conn_extend_max_stream_offset(c->quic, id, n);
    ngtcp2_conn_extend_max_offset(c->quic, n);
}

static nghttp3_ssize read_body(nghttp3_conn *http, int64_t id, nghttp3_vec *vec, size_t veccnt, uint32_t *flags,
                               void *user, void *stream_user) {
    h3_stream *s = stream_user;
    if (s->hold) {
        return NGHTTP3_ERR_WOULDBLOCK;
    }
    vec[0].base = (uint8_t *)s->body;
    vec[0].len = s->len;
    *flags |= NGHTTP3_DATA_FLAG_EOF;
    return 1;
}

static nghttp3_nv header(const char *name, const char *value) {
    return (nghttp3_nv){(uint8_t *)name, (uint8_t *)value, strlen(name), strlen(value), NGHTTP3_NV_FLAG_NONE};
}

static int respond(h3_stream *s) {
    const char *status = "200";
    if (strcmp(s->path, "/") == 0 || strcmp(s->path, "/slow") == 0 || strcmp(s->path, "/refuse") == 0) {
        s->body = "ok";
    } else if (strcmp(s->path, "/big") == 0) {
        s->body = big_body;
        s->len = sizeof(big_body);
    } else if (strcmp(s->path, "/count") == 0) {
        snprintf(s->text, sizeof(s->text), "%zu", s->received);
        s->body = s->text;
    } else if (strcmp(s->path, "/resumed") == 0) {
        s->body = SSL_session_reused(s->owner->ssl) ? "1" : "0";
    } else {
        status = "404";
        s->body = "not found";
    }
    if (s->len == 0) {
        s->len = strlen(s->body);
    }
    char length[32];
    snprintf(length, sizeof(length), "%zu", s->len);
    nghttp3_nv headers[] = {header(":status", status), header("content-type", "text/plain"),
                            header("content-length", length)};
    nghttp3_data_reader reader = {read_body};
    return nghttp3_conn_submit_response(s->owner->http, s->id, headers, 3, &reader);
}

static int on_begin_headers(nghttp3_conn *http, int64_t id, void *user, void *stream_user) {
    h3_conn *c = user;
    h3_stream *s = calloc(1, sizeof(*s));
    s->owner = c;
    s->id = id;
    s->next = c->streams;
    c->streams = s;
    nghttp3_conn_set_stream_user_data(http, id, s);
    return 0;
}

static int on_header(nghttp3_conn *http, int64_t id, int32_t token, nghttp3_rcbuf *name, nghttp3_rcbuf *value,
                     uint8_t flags, void *user, void *stream_user) {
    h3_stream *s = stream_user;
    nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
    if (s != NULL && token == NGHTTP3_QPACK_TOKEN__PATH && v.len < sizeof(s->path)) {
        memcpy(s->path, v.base, v.len);
        s->path[v.len] = '\0';
    }
    return 0;
}

static int on_end_headers(nghttp3_conn *http, int64_t id, int fin, void *user, void *stream_user) {
    h3_conn *c = user;
    h3_stream *s = stream_user;
    if (s == NULL) {
        return 0;
    }
    if (strcmp(s->path, "/refuse") == 0 && (c->refusing || __atomic_exchange_n(&refuse_next, 0, __ATOMIC_SEQ_CST))) {
        c->refusing = true;
        s->refuse = true;
    } else if (strcmp(s->path, "/hang-up") == 0) {
        s->hold = true;
        nghttp3_nv status = header(":status", "200");
        nghttp3_data_reader reader = {read_body};
        if (nghttp3_conn_submit_response(http, id, &status, 1, &reader) != 0) {
            return NGHTTP3_ERR_CALLBACK_FAILURE;
        }
        c->hang_up = true;
    }
    return 0;
}

static int on_data(nghttp3_conn *http, int64_t id, const uint8_t *data, size_t len, void *user, void *stream_user) {
    h3_stream *s = stream_user;
    if (s != NULL) {
        s->received += len;
    }
    consume(user, id, len);
    return 0;
}

static int on_deferred_consume(nghttp3_conn *http, int64_t id, size_t n, void *user, void *stream_user) {
    consume(user, id, n);
    return 0;
}

static int on_end_stream(nghttp3_conn *http, int64_t id, void *user, void *stream_user) {
    h3_stream *s = stream_user;
    if (s == NULL || s->refuse || s->hold) {
        return 0;
    }
    if (strcmp(s->path, "/slow") == 0) {
        s->due = now() + SLOW_DELAY;
        return 0;
    }
    return respond(s) == 0 ? 0 : NGHTTP3_ERR_CALLBACK_FAILURE;
}

static int on_stop_sending(nghttp3_conn *http, int64_t id, uint64_t code, void *user, void *stream_user) {
    h3_conn *c = user;
    return ngtcp2_conn_shutdown_stream_read(c->quic, 0, id, code) == 0 ? 0 : NGHTTP3_ERR_CALLBACK_FAILURE;
}

static int on_reset_stream(nghttp3_conn *http, int64_t id, uint64_t code, void *user, void *stream_user) {
    h3_conn *c = user;
    return ngtcp2_conn_shutdown_stream_write(c->quic, 0, id, code) == 0 ? 0 : NGHTTP3_ERR_CALLBACK_FAILURE;
}

static int on_http_stream_close(nghttp3_conn *http, int64_t id, uint64_t code, void *user, void *stream_user) {
    h3_stream *s = stream_user;
    if (s != NULL) {
        unlink_stream(s);
        free(s);
    }
    return 0;
}

static void random_bytes(uint8_t *dest, size_t len) { RAND_bytes(dest, len); }

static int setup_http(h3_conn *c) {
    if (c->http != NULL) {
        return 0;
    }
    if (ngtcp2_conn_get_streams_uni_left(c->quic) < 3) {
        return -1;
    }
    nghttp3_callbacks callbacks = {
        .stream_close = on_http_stream_close,
        .recv_data = on_data,
        .deferred_consume = on_deferred_consume,
        .begin_headers = on_begin_headers,
        .recv_header = on_header,
        .end_headers = on_end_headers,
        .stop_sending = on_stop_sending,
        .end_stream = on_end_stream,
        .reset_stream = on_reset_stream,
        .rand = random_bytes,
    };
    nghttp3_settings settings;
    nghttp3_settings_default(&settings);
    settings.qpack_max_dtable_capacity = 4096;
    settings.qpack_blocked_streams = 100;
    if (nghttp3_conn_server_new(&c->http, &callbacks, &settings, nghttp3_mem_default(), c) != 0) {
        return -1;
    }
    const ngtcp2_transport_params *params = ngtcp2_conn_get_local_transport_params(c->quic);
    nghttp3_conn_set_max_client_streams_bidi(c->http, params->initial_max_streams_bidi);
    int64_t control, encoder, decoder;
    if (ngtcp2_conn_open_uni_stream(c->quic, &control, NULL) != 0 ||
        nghttp3_conn_bind_control_stream(c->http, control) != 0 ||
        ngtcp2_conn_open_uni_stream(c->quic, &encoder, NULL) != 0 ||
        ngtcp2_conn_open_uni_stream(c->quic, &decoder, NULL) != 0 ||
        nghttp3_conn_bind_qpack_streams(c->http, encoder, decoder) != 0) {
        return -1;
    }
    return 0;
}

static int on_handshake_completed(ngtcp2_conn *quic, void *user) {
    __atomic_add_fetch(&connections, 1, __ATOMIC_SEQ_CST);
    return 0;
}

static int on_recv_stream_data(ngtcp2_conn *quic, uint32_t flags, int64_t id, uint64_t offset, const uint8_t *data,
                               size_t len, void *user, void *stream_user) {
    h3_conn *c = user;
    if (c->http == NULL) {
        return 0;
    }
    nghttp3_ssize n = nghttp3_conn_read_stream2(c->http, id, data, len, flags & NGTCP2_STREAM_DATA_FLAG_FIN,
                                                ngtcp2_conn_get_timestamp(quic));
    if (n < 0) {
        ngtcp2_ccerr_set_application_error(&c->error, nghttp3_err_infer_quic_app_error_code((int)n), NULL, 0);
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    consume(c, id, (size_t)n);
    return 0;
}

static int on_acked_stream_data_offset(ngtcp2_conn *quic, int64_t id, uint64_t offset, uint64_t len, void *user,
                                       void *stream_user) {
    h3_conn *c = user;
    if (c->http != NULL && nghttp3_conn_add_ack_offset(c->http, id, len) != 0) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
}

static int on_stream_close(ngtcp2_conn *quic, uint32_t flags, int64_t id, uint64_t code, void *user,
                           void *stream_user) {
    h3_conn *c = user;
    if (c->http == NULL) {
        return 0;
    }
    if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET)) {
        code = NGHTTP3_H3_NO_ERROR;
    }
    int rv = nghttp3_conn_close_stream(c->http, id, code);
    if (rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND) {
        ngtcp2_ccerr_set_application_error(&c->error, nghttp3_err_infer_quic_app_error_code(rv), NULL, 0);
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
}

static int on_stream_reset(ngtcp2_conn *quic, int64_t id, uint64_t final_size, uint64_t code, void *user,
                           void *stream_user) {
    h3_conn *c = user;
    if (c->http != NULL && nghttp3_conn_shutdown_stream_read(c->http, id) != 0) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
}

static int on_stream_stop_sending(ngtcp2_conn *quic, int64_t id, uint64_t code, void *user, void *stream_user) {
    return on_stream_reset(quic, id, 0, code, user, stream_user);
}

static int on_extend_max_remote_streams_bidi(ngtcp2_conn *quic, uint64_t max_streams, void *user) {
    h3_conn *c = user;
    if (c->http != NULL) {
        nghttp3_conn_set_max_client_streams_bidi(c->http, max_streams);
    }
    return 0;
}

static int on_extend_max_stream_data(ngtcp2_conn *quic, int64_t id, uint64_t max_data, void *user, void *stream_user) {
    h3_conn *c = user;
    if (c->http != NULL && nghttp3_conn_unblock_stream(c->http, id) != 0) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
}

static int on_recv_tx_key(ngtcp2_conn *quic, ngtcp2_encryption_level level, void *user) {
    if (level != NGTCP2_ENCRYPTION_LEVEL_1RTT) {
        return 0;
    }
    return setup_http(user) == 0 ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

static void quic_rand(uint8_t *dest, size_t len, const ngtcp2_rand_ctx *ctx) { RAND_bytes(dest, len); }

static int on_get_new_connection_id(ngtcp2_conn *quic, ngtcp2_cid *cid, uint8_t *token, size_t len, void *user) {
    if (RAND_bytes(cid->data, len) != 1) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    cid->datalen = len;
    if (ngtcp2_crypto_generate_stateless_reset_token(token, srv.secret, sizeof(srv.secret), cid) != 0) {
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    associate(cid, user);
    return 0;
}

static int on_remove_connection_id(ngtcp2_conn *quic, const ngtcp2_cid *cid, void *user) {
    dissociate(cid);
    return 0;
}

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) { return ((h3_conn *)ref->user_data)->quic; }

static const ngtcp2_callbacks quic_callbacks = {
    .recv_client_initial = ngtcp2_crypto_recv_client_initial_cb,
    .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
    .handshake_completed = on_handshake_completed,
    .encrypt = ngtcp2_crypto_encrypt_cb,
    .decrypt = ngtcp2_crypto_decrypt_cb,
    .hp_mask = ngtcp2_crypto_hp_mask_cb,
    .recv_stream_data = on_recv_stream_data,
    .acked_stream_data_offset = on_acked_stream_data_offset,
    .rand = quic_rand,
    .remove_connection_id = on_remove_connection_id,
    .update_key = ngtcp2_crypto_update_key_cb,
    .stream_reset = on_stream_reset,
    .extend_max_remote_streams_bidi = on_extend_max_remote_streams_bidi,
    .extend_max_stream_data = on_extend_max_stream_data,
    .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
    .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
    .stream_stop_sending = on_stream_stop_sending,
    .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
    .recv_tx_key = on_recv_tx_key,
    .get_new_connection_id = on_get_new_connection_id,
    .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
    .stream_close = on_stream_close,
};

static ngtcp2_path path_to(struct sockaddr_storage *peer, socklen_t peer_len) {
    return (ngtcp2_path){.local = {(struct sockaddr *)&srv.local, sizeof(srv.local)},
                         .remote = {(struct sockaddr *)peer, peer_len}};
}

static h3_conn *new_conn(const ngtcp2_pkt_hd *hd, struct sockaddr_storage *peer, socklen_t peer_len, ngtcp2_tstamp ts) {
    h3_conn *c = calloc(1, sizeof(*c));
    c->peer = *peer;
    c->peer_len = peer_len;
    c->ref = (ngtcp2_crypto_conn_ref){get_conn, c};
    ngtcp2_ccerr_default(&c->error);
    ngtcp2_cid scid = {.datalen = SCID_LEN};
    RAND_bytes(scid.data, scid.datalen);
    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = ts;
    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_stream_data_bidi_local = 256 * 1024;
    params.initial_max_stream_data_bidi_remote = 1024 * 1024;
    params.initial_max_stream_data_uni = 256 * 1024;
    params.initial_max_data = 4 * 1024 * 1024;
    params.initial_max_streams_bidi = 128;
    params.initial_max_streams_uni = 16;
    params.max_idle_timeout = 10 * NGTCP2_SECONDS;
    params.active_connection_id_limit = 7;
    params.original_dcid = hd->dcid;
    params.original_dcid_present = 1;
    params.stateless_reset_token_present = 1;
    ngtcp2_crypto_generate_stateless_reset_token(params.stateless_reset_token, srv.secret, sizeof(srv.secret), &scid);
    ngtcp2_path path = path_to(&c->peer, c->peer_len);
    if (ngtcp2_conn_server_new(&c->quic, &hd->scid, &scid, &path, hd->version, &quic_callbacks, &settings, &params,
                               NULL, c) != 0) {
        free(c);
        return NULL;
    }
    c->ssl = SSL_new(srv.ctx);
    SSL_set_app_data(c->ssl, &c->ref);
    SSL_set_accept_state(c->ssl);
#ifdef HX_H3_OSSL
    ngtcp2_crypto_ossl_ctx_new(&c->ossl, c->ssl);
    ngtcp2_crypto_ossl_configure_server_session(c->ssl);
    ngtcp2_conn_set_tls_native_handle(c->quic, c->ossl);
#else
    ngtcp2_conn_set_tls_native_handle(c->quic, c->ssl);
#endif
    associate(&hd->dcid, c);
    associate(&scid, c);
    c->next = srv.conns;
    srv.conns = c;
    return c;
}

static void send_datagram(h3_conn *c, const uint8_t *data, size_t len) {
    sendto(srv.fd, data, len, 0, (struct sockaddr *)&c->peer, c->peer_len);
}

static void start_closing(h3_conn *c, ngtcp2_tstamp ts) {
    if (c->close_deadline != 0 || c->dead) {
        return;
    }
    c->close_deadline = ts + 3 * ngtcp2_conn_get_pto(c->quic);
    if (ngtcp2_conn_in_draining_period(c->quic)) {
        return;
    }
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(c->quic, &ps.path, &pi, c->close_packet,
                                                        sizeof(c->close_packet), &c->error, ts);
    if (n <= 0) {
        c->dead = true;
        return;
    }
    c->close_len = (size_t)n;
    send_datagram(c, c->close_packet, c->close_len);
}

static void fail(h3_conn *c, int rv, ngtcp2_tstamp ts) {
    if (rv == NGTCP2_ERR_DRAINING) {
        start_closing(c, ts);
        return;
    }
    if (rv == NGTCP2_ERR_DROP_CONN || rv == NGTCP2_ERR_IDLE_CLOSE) {
        c->dead = true;
        return;
    }
    if (c->error.error_code == 0) {
        if (rv == NGTCP2_ERR_CRYPTO) {
            ngtcp2_ccerr_set_tls_alert(&c->error, ngtcp2_conn_get_tls_alert(c->quic), NULL, 0);
        } else {
            ngtcp2_ccerr_set_liberr(&c->error, rv, NULL, 0);
        }
    }
    start_closing(c, ts);
}

static int write_packets(h3_conn *c, ngtcp2_tstamp ts) {
    uint8_t buf[NGTCP2_MAX_UDP_PAYLOAD_SIZE];
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    for (;;) {
        int64_t id = -1;
        int fin = 0;
        nghttp3_vec vec[16];
        nghttp3_ssize count = 0;
        if (c->http != NULL && ngtcp2_conn_get_max_data_left(c->quic) > 0) {
            count = nghttp3_conn_writev_stream(c->http, &id, &fin, vec, 16);
            if (count < 0) {
                ngtcp2_ccerr_set_application_error(&c->error, nghttp3_err_infer_quic_app_error_code((int)count), NULL,
                                                   0);
                return NGTCP2_ERR_CALLBACK_FAILURE;
            }
        }
        ngtcp2_ssize written = -1;
        uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
        ngtcp2_pkt_info pi;
        ngtcp2_ssize n = ngtcp2_conn_writev_stream(c->quic, &ps.path, &pi, buf, sizeof(buf), &written, flags, id,
                                                   (const ngtcp2_vec *)vec, (size_t)count, ts);
        if (n == NGTCP2_ERR_STREAM_DATA_BLOCKED) {
            nghttp3_conn_block_stream(c->http, id);
            continue;
        }
        if (n == NGTCP2_ERR_STREAM_SHUT_WR) {
            nghttp3_conn_shutdown_stream_write(c->http, id);
            continue;
        }
        if (written >= 0 && nghttp3_conn_add_write_offset(c->http, id, (size_t)written) != 0) {
            return NGTCP2_ERR_CALLBACK_FAILURE;
        }
        if (n == NGTCP2_ERR_WRITE_MORE) {
            continue;
        }
        if (n < 0) {
            return (int)n;
        }
        if (n == 0) {
            break;
        }
        send_datagram(c, buf, (size_t)n);
    }
    ngtcp2_conn_update_pkt_tx_time(c->quic, ts);
    return 0;
}

static void refuse_streams(h3_conn *c) {
    for (h3_stream *s = c->streams; s != NULL; s = s->next) {
        if (s->refuse && !s->refused) {
            s->refused = true;
            ngtcp2_conn_shutdown_stream(c->quic, 0, s->id, NGHTTP3_H3_REQUEST_REJECTED);
        }
    }
}

static int answer_slow_streams(h3_conn *c, ngtcp2_tstamp ts) {
    for (h3_stream *s = c->streams; s != NULL; s = s->next) {
        if (s->due != 0 && s->due <= ts) {
            s->due = 0;
            if (respond(s) != 0) {
                return NGTCP2_ERR_CALLBACK_FAILURE;
            }
        }
    }
    return 0;
}

static void service(h3_conn *c, ngtcp2_tstamp ts) {
    if (c->dead) {
        return;
    }
    if (c->close_deadline != 0) {
        if (ts >= c->close_deadline) {
            c->dead = true;
        }
        return;
    }
    int rv = 0;
    if (ngtcp2_conn_get_expiry(c->quic) <= ts) {
        rv = ngtcp2_conn_handle_expiry(c->quic, ts);
    }
    if (rv == 0 && c->http != NULL) {
        refuse_streams(c);
        rv = answer_slow_streams(c, ts);
    }
    if (rv == 0) {
        rv = write_packets(c, ts);
    }
    if (rv != 0) {
        fail(c, rv, ts);
        return;
    }
    if (c->hang_up) {
        ngtcp2_ccerr_set_application_error(&c->error, NGHTTP3_H3_INTERNAL_ERROR, NULL, 0);
        start_closing(c, ts);
    }
}

static void read_datagram(uint8_t *data, size_t len, struct sockaddr_storage *peer, socklen_t peer_len) {
    ngtcp2_version_cid vc;
    if (ngtcp2_pkt_decode_version_cid(&vc, data, len, SCID_LEN) != 0) {
        return;
    }
    ngtcp2_tstamp ts = now();
    h3_conn *c = find_conn(vc.dcid, vc.dcidlen);
    if (c == NULL) {
        ngtcp2_pkt_hd hd;
        if (ngtcp2_accept(&hd, data, len) != 0 || (c = new_conn(&hd, peer, peer_len, ts)) == NULL) {
            return;
        }
    } else if (c->dead) {
        return;
    } else if (c->close_deadline != 0) {
        if (c->close_len > 0) {
            send_datagram(c, c->close_packet, c->close_len);
        }
        return;
    }
    ngtcp2_path path = path_to(peer, peer_len);
    ngtcp2_pkt_info pi = {0};
    int rv = ngtcp2_conn_read_pkt(c->quic, &path, &pi, data, len, ts);
    if (rv != 0) {
        fail(c, rv, ts);
    }
}

static void read_socket(void) {
    uint8_t data[65536];
    for (;;) {
        struct sockaddr_storage peer;
        socklen_t peer_len = sizeof(peer);
        ssize_t n = recvfrom(srv.fd, data, sizeof(data), 0, (struct sockaddr *)&peer, &peer_len);
        if (n < 0) {
            return;
        }
        read_datagram(data, (size_t)n, &peer, peer_len);
    }
}

static void reap(void) {
    for (h3_conn **p = &srv.conns; *p != NULL;) {
        h3_conn *c = *p;
        if (c->dead) {
            *p = c->next;
            free_conn(c);
        } else {
            p = &c->next;
        }
    }
}

static int next_timeout_ms(void) {
    ngtcp2_tstamp ts = now();
    ngtcp2_tstamp next = ts + NGTCP2_SECONDS;
    for (h3_conn *c = srv.conns; c != NULL; c = c->next) {
        ngtcp2_tstamp when = c->close_deadline != 0 ? c->close_deadline : ngtcp2_conn_get_expiry(c->quic);
        if (when < next) {
            next = when;
        }
        for (h3_stream *s = c->streams; s != NULL; s = s->next) {
            if (s->due != 0 && s->due < next) {
                next = s->due;
            }
        }
    }
    if (next <= ts) {
        return 0;
    }
    return (int)((next - ts + NGTCP2_MILLISECONDS - 1) / NGTCP2_MILLISECONDS);
}

static void *serve(void *arg) {
    for (;;) {
        struct pollfd fds[2] = {{srv.fd, POLLIN, 0}, {srv.wake[0], POLLIN, 0}};
        poll(fds, 2, next_timeout_ms());
        if (fds[1].revents != 0) {
            break;
        }
        if (fds[0].revents & POLLIN) {
            read_socket();
        }
        ngtcp2_tstamp ts = now();
        for (h3_conn *c = srv.conns; c != NULL; c = c->next) {
            service(c, ts);
        }
        reap();
    }
    return NULL;
}

static int choose_h3(SSL *ssl, const unsigned char **out, unsigned char *outlen, const unsigned char *in,
                     unsigned int inlen, void *arg) {
    for (unsigned int i = 0; i + 1 + in[i] <= inlen; i += 1 + in[i]) {
        if (in[i] == 2 && memcmp(in + i + 1, "h3", 2) == 0) {
            *out = in + i + 1;
            *outlen = 2;
            return SSL_TLSEXT_ERR_OK;
        }
    }
    return SSL_TLSEXT_ERR_ALERT_FATAL;
}

static SSL_CTX *make_context(const char *cert, const char *key) {
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
#ifdef HX_H3_OSSL
    int configured = ngtcp2_crypto_ossl_init();
#else
    int configured = ctx != NULL ? ngtcp2_crypto_boringssl_configure_server_context(ctx) : -1;
#endif
    if (ctx == NULL || configured != 0 || SSL_CTX_use_certificate_chain_file(ctx, cert) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) != 1 || SSL_CTX_check_private_key(ctx) != 1) {
        SSL_CTX_free(ctx);
        return NULL;
    }
    SSL_CTX_set_alpn_select_cb(ctx, choose_h3, NULL);
    return ctx;
}

int h3_server_start(const char *cert, const char *key) {
    memset(big_body, 'x', sizeof(big_body));
    srv.ctx = make_context(cert, key);
    if (srv.ctx == NULL) {
        fprintf(stderr, "h3_server: can't load %s and %s\n", cert, key);
        return -1;
    }
    RAND_bytes(srv.secret, sizeof(srv.secret));
    srv.fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int size = 4 * 1024 * 1024;
    setsockopt(srv.fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
    setsockopt(srv.fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    srv.local = (struct sockaddr_in){.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t len = sizeof(srv.local);
    if (bind(srv.fd, (struct sockaddr *)&srv.local, sizeof(srv.local)) != 0 ||
        getsockname(srv.fd, (struct sockaddr *)&srv.local, &len) != 0 || pipe2(srv.wake, O_CLOEXEC) != 0) {
        h3_server_stop();
        return -1;
    }
    if (pthread_create(&srv.thread, NULL, serve, NULL) != 0) {
        close(srv.wake[1]);
        srv.wake[1] = -1;
        h3_server_stop();
        return -1;
    }
    return ntohs(srv.local.sin_port);
}

int h3_server_connections(void) { return __atomic_load_n(&connections, __ATOMIC_SEQ_CST); }

void h3_server_refuse_next_connection(void) { __atomic_store_n(&refuse_next, 1, __ATOMIC_SEQ_CST); }

void h3_server_stop(void) {
    if (srv.wake[1] >= 0) {
        char byte = 0;
        if (write(srv.wake[1], &byte, 1) == 1) {
            pthread_join(srv.thread, NULL);
        }
    }
    while (srv.conns != NULL) {
        h3_conn *c = srv.conns;
        srv.conns = c->next;
        free_conn(c);
    }
    free(srv.cids);
    SSL_CTX_free(srv.ctx);
    for (int i = 0; i < 2; i++) {
        if (srv.wake[i] >= 0) {
            close(srv.wake[i]);
        }
    }
    if (srv.fd >= 0) {
        close(srv.fd);
    }
    memset(&srv, 0, sizeof(srv));
    srv.fd = -1;
    srv.wake[0] = srv.wake[1] = -1;
}
