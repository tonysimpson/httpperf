#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "ng.h"
#include <openssl/err.h>
#include <poll.h>
#include <stdio.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../include/hx.h"
#include "decode.h"
#include "dns.h"
#include "h1.h"
#include "h3.h"
#include "tls.h"

#define MAX_ADDRS 8
#define KEEP_FOR_RETRY (1 << 20)

typedef enum { CONN_CONNECTING, CONN_TUNNEL, CONN_HANDSHAKE, CONN_BUSY, CONN_IDLE, CONN_CLOSED } conn_state;

typedef struct conn {
    hx_client *client;
    hx_socket fd;
    uint64_t id;
    conn_state state;
    char key[300];
    char ip[46];
    int watching;
    hx_h1 h1;
    hx_buf out;
    size_t out_sent;
    char *rbuf;
    size_t rcap;
    int queued;
    bool pinned;
    bool want_read;
    int64_t idle_since;
    int64_t idle_timeout;
    int64_t created;
    uint64_t pool;
    bool closing;
    int64_t connect_deadline;
    struct sockaddr_storage addrs[MAX_ADDRS];
    socklen_t addr_lens[MAX_ADDRS];
    int addr_count;
    int addr_next;
    int connect_errno;
    hx_decoder decoder;
    hx_buf decoded;
    bool decoding;
    bool undecodable;
    SSL *ssl;
    hx_tls_ctx *tls;
    char session_key[320];
    const char *tls_detail;
    size_t tunnel_len;
    bool may_h2;
    bool raw;
    bool h2;
    bool h3;
    hx_h3 *quic;
    bool in_ng;
    bool dirty;
    bool close_after;
    struct conn *dirty_next;
    nghttp2_session *ng;
    size_t streams;
    struct hx_req *stream_list;
    struct hx_req *waiters;
    struct hx_req *touched;
    struct hx_req *req;
    struct conn *prev, *next;
    bool listed_idle;
    struct conn *idle_prev, *idle_next;
} conn;

typedef struct lookup {
    uint64_t id;
    char key[264];
    struct hx_req *waiting;
    struct lookup *next;
} lookup;

typedef struct cached {
    char key[264];
    struct addrinfo *addrs;
    int64_t expires;
    struct cached *next;
} cached;

struct hx_req {
    hx_client *client;
    void *user;
    conn *conn;
    hx_buf request;
    bool head;
    bool decode;
    bool fresh;
    int64_t max_age;
    uint64_t pool;
    bool upload;
    bool chunked;
    bool body_done;
    bool asked;
    bool paused;
    bool retried;
    int send_errno;
    hx_tls_ctx *tls;
    uint64_t tls_cache;
    const char *detail;
    bool close_after;
    int64_t idle_timeout;
    bool done;
    bool reused;
    bool headers_seen;
    int64_t deadline;
    int64_t times[HX_T_COUNT];
    uint64_t sent;
    uint64_t received;
    char key[300];
    char host[256];
    uint16_t port;
    char proxy_host[256];
    uint16_t proxy_port;
    char *proxy_auth;
    int64_t connect_timeout;
    bool upgrade;
    bool h2_offer;
    bool h3_offer;
    int64_t stream_id;
    bool stream_closed;
    int h2_status;
    hx_error h2_error;
    hx_buf h2_head;
    bool dec_bad;
    size_t body_off;
    size_t unconsumed;
    hx_decoder *dec;
    hx_buf dec_out;
    bool touched;
    hx_buf held;
    size_t held_off;
    bool deferred;
    bool held_dropped;
    struct hx_req *retired_next;
    struct hx_req *s_prev, *s_next, *touched_next;
    conn *waiting_on;
    struct hx_req *wait_next;
    lookup *lookup;
    struct hx_req *dns_next;
    struct hx_req *prev, *next;
};

typedef struct {
    hx_event ev;
    conn *pin;
    struct hx_req *done;
} queued;

struct hx_client {
    hx_config cfg;
    hx_io io;
    uint64_t next_conn_id;
    size_t open;
    size_t idle;
    conn *conns;
    conn *idle_head, *idle_tail;
    conn **by_fd;
    size_t by_fd_len;
    struct hx_req *reqs;
    queued *queue;
    size_t queue_len, queue_cap, queue_pos;
    conn **released;
    size_t released_len, released_cap;
    struct hx_req *retired;
    int64_t timer;
    hx_dns *dns;
    uint64_t next_lookup_id;
    lookup *lookups;
    cached *cache;
    hx_tls tls;
    conn *receiving;
    nghttp2_session_callbacks *h2_callbacks;
    nghttp2_option *h2_option;
    conn *dirty;
};

#define LINK(head, item)           \
    do {                           \
        (item)->prev = NULL;       \
        (item)->next = (head);     \
        if ((head) != NULL) {      \
            (head)->prev = (item); \
        }                          \
        (head) = (item);           \
    } while (0)

#define UNLINK(head, item)                     \
    do {                                       \
        if ((item)->prev != NULL) {            \
            (item)->prev->next = (item)->next; \
        } else {                               \
            (head) = (item)->next;             \
        }                                      \
        if ((item)->next != NULL) {            \
            (item)->next->prev = (item)->prev; \
        }                                      \
    } while (0)

static int64_t now(hx_client *c) { return c->io.now(c->io.ctx); }

static void req_free(struct hx_req *q) {
    hx_buf_free(&q->request);
    hx_buf_free(&q->h2_head);
    hx_buf_free(&q->held);
    if (q->dec != NULL) {
        hx_decoder_free(q->dec);
        free(q->dec);
    }
    hx_buf_free(&q->dec_out);
    free(q->proxy_auth);
    free(q);
}

static void connect_new(hx_client *c, struct hx_req *q);
static void process_dirty(hx_client *c);

static bool multiplexed(const conn *k) { return k->h2 || k->h3; }

static int version_of(const conn *k) { return k->h3 ? 30 : 20; }

static void mark_dirty(conn *k) {
    if (!k->dirty) {
        k->dirty = true;
        k->dirty_next = k->client->dirty;
        k->client->dirty = k;
    }
}

static void unwait(struct hx_req *q) {
    struct hx_req **p = &q->waiting_on->waiters;
    while (*p != NULL && *p != q) {
        p = &(*p)->wait_next;
    }
    if (*p == q) {
        *p = q->wait_next;
    }
    q->waiting_on = NULL;
    q->wait_next = NULL;
}

static void h2_detach(conn *k, struct hx_req *q) {
    if (k->h3 && !q->stream_closed && q->stream_id >= 0) {
        hx_h3_cancel(k->quic, q->stream_id);
        q->stream_closed = true;
    } else if (!q->stream_closed && q->stream_id > 0 && k->ng != NULL) {
        nghttp2_submit_rst_stream(k->ng, NGHTTP2_FLAG_NONE, q->stream_id, NGHTTP2_CANCEL);
        nghttp2_session_set_stream_user_data(k->ng, q->stream_id, NULL);
        q->stream_closed = true;
    }
    if (q->s_prev != NULL) {
        q->s_prev->s_next = q->s_next;
    } else {
        k->stream_list = q->s_next;
    }
    if (q->s_next != NULL) {
        q->s_next->s_prev = q->s_prev;
    }
    q->s_prev = q->s_next = NULL;
    k->streams--;
    k->idle_timeout = q->idle_timeout;
    q->conn = NULL;
    mark_dirty(k);
}

static const char *target_host(const struct hx_req *q) { return q->proxy_host[0] ? q->proxy_host : q->host; }

static uint16_t target_port(const struct hx_req *q) { return q->proxy_host[0] ? q->proxy_port : q->port; }

static void watch(conn *k, int events) {
    if (k->watching != events) {
        k->watching = events;
        k->client->io.watch(k->client->io.ctx, k->fd, events);
    }
}

static void schedule(hx_client *c, int64_t deadline) {
    if (deadline > 0 && (c->timer == 0 || deadline < c->timer)) {
        c->timer = deadline;
        c->io.set_timer(c->io.ctx, deadline);
    }
}

static queued *enqueue(hx_client *c, struct hx_req *q, hx_event_type type) {
    if (c->queue_len == c->queue_cap) {
        size_t cap = c->queue_cap ? c->queue_cap * 2 : 64;
        queued *grown = realloc(c->queue, cap * sizeof(queued));
        if (grown == NULL) {
            return NULL;
        }
        c->queue = grown;
        c->queue_cap = cap;
    }
    queued *e = &c->queue[c->queue_len++];
    *e = (queued){0};
    e->ev.type = type;
    e->ev.user = q->user;
    return e;
}

static void conn_free(conn *k) {
    if (k->ng != NULL) {
        nghttp2_session_del(k->ng);
    }
    if (k->quic != NULL) {
        hx_h3_free(k->quic);
    }
    hx_decoder_free(&k->decoder);
    hx_buf_free(&k->decoded);
    hx_h1_free(&k->h1);
    hx_buf_free(&k->out);
    free(k->rbuf);
    free(k);
}

static void idle_add(conn *k) {
    hx_client *c = k->client;
    k->listed_idle = true;
    k->idle_next = NULL;
    k->idle_prev = c->idle_tail;
    if (c->idle_tail != NULL) {
        c->idle_tail->idle_next = k;
    } else {
        c->idle_head = k;
    }
    c->idle_tail = k;
}

static void idle_remove(conn *k) {
    if (!k->listed_idle) {
        return;
    }
    hx_client *c = k->client;
    k->listed_idle = false;
    if (k->idle_prev != NULL) {
        k->idle_prev->idle_next = k->idle_next;
    } else {
        c->idle_head = k->idle_next;
    }
    if (k->idle_next != NULL) {
        k->idle_next->idle_prev = k->idle_prev;
    } else {
        c->idle_tail = k->idle_prev;
    }
    k->idle_prev = k->idle_next = NULL;
}

static void conn_close(conn *k) {
    if (k->state == CONN_CLOSED) {
        return;
    }
    hx_client *c = k->client;
    idle_remove(k);
    if (k->state == CONN_IDLE) {
        c->idle--;
    }
    if (k->ssl != NULL) {
        if (k->tls_detail == NULL && SSL_is_init_finished(k->ssl)) {
            ERR_clear_error();
            SSL_shutdown(k->ssl);
            ERR_clear_error();
        }
        SSL_free(k->ssl);
        k->ssl = NULL;
    }
    if (k->quic != NULL) {
        hx_h3_close(k->quic, now(c));
    }
    if (k->fd >= 0) {
        watch(k, 0);
        close(k->fd);
        c->by_fd[k->fd] = NULL;
    }
    k->state = CONN_CLOSED;
    c->open--;
    UNLINK(c->conns, k);
    if (k->pool != 0 && c->io.connection != NULL) {
        c->io.connection(c->io.ctx, k->pool, -1);
    }
    if (k->req != NULL) {
        k->req->conn = NULL;
        k->req = NULL;
    }
    while (k->waiters != NULL) {
        struct hx_req *w = k->waiters;
        unwait(w);
        connect_new(c, w);
    }
    if (!k->pinned && k->queued == 0 && c->receiving != k && !k->dirty) {
        conn_free(k);
    }
}

static void finish(struct hx_req *q, hx_error error, int sys_errno, bool closed) {
    if (q->done) {
        return;
    }
    hx_client *c = q->client;
    q->done = true;
    q->times[HX_T_DONE] = now(c);
    if (q->lookup != NULL) {
        struct hx_req **p = &q->lookup->waiting;
        while (*p != NULL && *p != q) {
            p = &(*p)->dns_next;
        }
        if (*p == q) {
            *p = q->dns_next;
        }
        q->lookup = NULL;
    }
    if (q->waiting_on != NULL) {
        unwait(q);
    }
    conn *k = q->conn;
    bool handshaking = k != NULL && k->h3 && k->req == q;
    bool on_h2 = k != NULL && multiplexed(k) && !handshaking;
    if (!closed && k != NULL && ((q->upload && !q->body_done) || q->upgrade)) {
        closed = true;
    }
    if (!closed && k != NULL && (k->closing || (c->cfg.max_idle > 0 && c->idle >= c->cfg.max_idle))) {
        closed = true;
    }
    if (on_h2) {
        closed = false;
    }
    queued *e = enqueue(c, q, HX_EV_DONE);
    if (e != NULL) {
        e->ev.error = error;
        e->ev.sys_errno = sys_errno;
        e->ev.detail = error != HX_OK ? q->detail : NULL;
        e->ev.status = k == NULL ? 0 : on_h2 ? q->h2_status : k->h1.status;
        e->ev.version = k == NULL ? 0 : on_h2 ? version_of(k) : k->h1.version;
        e->ev.reused = q->reused;
        e->ev.connection_closed = closed;
        e->ev.connection_id = k != NULL ? k->id : 0;
        memcpy(e->ev.times, q->times, sizeof(q->times));
        e->ev.bytes_sent = q->sent;
        e->ev.bytes_received = q->received;
        if (k != NULL) {
            memcpy(e->ev.primary_ip, k->ip, sizeof(k->ip));
        }
        e->done = q;
    }
    UNLINK(c->reqs, q);
    if (on_h2) {
        h2_detach(k, q);
    } else if (handshaking) {
        k->req = NULL;
        q->conn = NULL;
        if (k->waiters == NULL) {
            conn_close(k);
        }
    } else if (k != NULL) {
        k->req = NULL;
        q->conn = NULL;
        if (closed) {
            conn_close(k);
        } else {
            k->state = CONN_IDLE;
            c->idle++;
            idle_add(k);
            k->idle_since = q->times[HX_T_DONE];
            k->idle_timeout = q->idle_timeout;
            schedule(c, k->idle_timeout > 0 ? k->idle_since + k->idle_timeout : 0);
            watch(k, HX_READ);
        }
    }
    if (e == NULL) {
        req_free(q);
    }
}

static bool retry(conn *k, hx_error error) {
    struct hx_req *q = k->req;
    if (q == NULL || !q->reused || q->retried || q->received > 0 || q->upload ||
        (error != HX_ERR_RECEIVE && error != HX_ERR_SEND)) {
        return false;
    }
    hx_client *c = k->client;
    hx_buf sent = k->out;
    k->out = q->request;
    k->out.len = k->out_sent = 0;
    q->request = sent;
    k->req = NULL;
    q->conn = NULL;
    conn_close(k);
    q->retried = true;
    q->reused = false;
    q->sent = 0;
    q->times[HX_T_FIRST_SENT] = q->times[HX_T_LAST_SENT] = q->times[HX_T_FIRST_BYTE] = 0;
    connect_new(c, q);
    return true;
}

static void fail(conn *k, hx_error error, int sys_errno) {
    if (retry(k, error)) {
        return;
    }
    if (k->req != NULL && k->req->detail == NULL) {
        k->req->detail = k->tls_detail;
    }
    while (k->stream_list != NULL) {
        struct hx_req *q = k->stream_list;
        q->stream_closed = true;
        if (q->detail == NULL) {
            q->detail = k->tls_detail;
        }
        finish(q, error, sys_errno, true);
    }
    while (k->waiters != NULL) {
        struct hx_req *w = k->waiters;
        unwait(w);
        if (w->detail == NULL) {
            w->detail = k->tls_detail;
        }
        finish(w, error, sys_errno, false);
    }
    if (k->req != NULL) {
        finish(k->req, error, sys_errno, true);
    } else {
        conn_close(k);
    }
}

static int reading(const conn *k) { return k->req != NULL && k->req->paused ? 0 : HX_READ; }

static ssize_t tls_result(conn *k, int n) {
    int err = SSL_get_error(k->ssl, n);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        errno = EAGAIN;
        return -1;
    }
    if (err == SSL_ERROR_ZERO_RETURN) {
        return 0;
    }
    unsigned long e = ERR_peek_last_error();
    if (err == SSL_ERROR_SYSCALL && e == 0) {
        if (errno == 0 || errno == EAGAIN) {
            return 0;
        }
        return -1;
    }
    k->tls_detail = e != 0 ? ERR_reason_error_string(e) : NULL;
    ERR_clear_error();
    errno = EPROTO;
    return -1;
}

static ssize_t conn_write(conn *k, const char *data, size_t len) {
    if (k->ssl == NULL) {
        return send(k->fd, data, len, MSG_NOSIGNAL);
    }
    ERR_clear_error();
    errno = 0;
    int n = SSL_write(k->ssl, data, len > INT32_MAX ? INT32_MAX : (int)len);
    if (n > 0) {
        return n;
    }
    ssize_t r = tls_result(k, n);
    if (r == 0) {
        errno = EPIPE;
        return -1;
    }
    return r;
}

static ssize_t conn_read(conn *k) {
    if (k->ssl == NULL) {
        return recv(k->fd, k->rbuf, k->rcap, 0);
    }
    ERR_clear_error();
    errno = 0;
    int n = SSL_read(k->ssl, k->rbuf, k->rcap > INT32_MAX ? INT32_MAX : (int)k->rcap);
    return n > 0 ? n : tls_result(k, n);
}

static void send_some(conn *k) {
    struct hx_req *q = k->req;
    if (q->send_errno != 0) {
        k->out.len = k->out_sent = 0;
        return;
    }
    int writes = 0;
    int64_t first = 0;
    while (k->out_sent < k->out.len) {
        ssize_t n = conn_write(k, k->out.data + k->out_sent, k->out.len - k->out_sent);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                watch(k, reading(k) | HX_WRITE);
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (q->upload) {
                q->send_errno = errno;
                k->out.len = k->out_sent = 0;
                watch(k, reading(k));
                return;
            }
            fail(k, HX_ERR_SEND, errno);
            return;
        }
        if (q->times[HX_T_FIRST_SENT] == 0) {
            q->times[HX_T_FIRST_SENT] = first = now(k->client);
        }
        writes++;
        k->out_sent += (size_t)n;
        q->sent += (uint64_t)n;
    }
    if (q->upload) {
        k->out.len = k->out_sent = 0;
    }
    watch(k, reading(k));
    if (!q->upload || q->body_done) {
        q->times[HX_T_LAST_SENT] = writes == 1 && first != 0 ? first : now(k->client);
    } else if (!q->asked) {
        q->asked = true;
        enqueue(k->client, q, HX_EV_SEND_READY);
    }
}

static void end_decoding(conn *k) {
    k->decoding = false;
    k->undecodable = false;
    hx_decoder_end(&k->decoder);
}

static bool frame_held(struct hx_req *q, hx_buf *out) {
    if (!q->h2_offer || !q->upload) {
        return true;
    }
    q->h2_offer = false;
    bool ok = q->held.len == 0 || (q->chunked ? hx_h1_write_chunk(out, q->held.data, q->held.len) == 0
                                              : hx_buf_append(out, q->held.data, q->held.len));
    if (ok && q->body_done && q->chunked) {
        ok = hx_buf_append(out, "0\r\n\r\n", 5);
    }
    hx_buf_free(&q->held);
    return ok;
}

static void begin(conn *k, struct hx_req *q) {
    if (k->state == CONN_IDLE) {
        k->client->idle--;
    }
    idle_remove(k);
    k->req = q;
    q->conn = k;
    k->state = CONN_BUSY;
    k->out.len = 0;
    k->out_sent = 0;
    hx_buf tmp = k->out;
    k->out = q->request;
    q->request = tmp;
    if (!frame_held(q, &k->out)) {
        k->req = NULL;
        q->conn = NULL;
        finish(q, HX_ERR_NOMEM, 0, false);
        return;
    }
    hx_h1_next(&k->h1, q->head);
    if (k->decoding) {
        end_decoding(k);
    }
    send_some(k);
}

static bool track_fd(hx_client *c, conn *k) {
    if ((size_t)k->fd >= c->by_fd_len) {
        size_t len = (size_t)k->fd * 2 + 64;
        conn **grown = realloc(c->by_fd, len * sizeof(conn *));
        if (grown == NULL) {
            return false;
        }
        memset(grown + c->by_fd_len, 0, (len - c->by_fd_len) * sizeof(conn *));
        c->by_fd = grown;
        c->by_fd_len = len;
    }
    c->by_fd[k->fd] = k;
    return true;
}

static int connect_next(conn *k) {
    hx_client *c = k->client;
    while (k->addr_next < k->addr_count) {
        int i = k->addr_next++;
        struct sockaddr *sa = (struct sockaddr *)&k->addrs[i];
        int fd = socket(sa->sa_family, (k->h3 ? SOCK_DGRAM : SOCK_STREAM) | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            k->connect_errno = errno;
            continue;
        }
        int one = 1;
        if (!k->h3) {
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        }
        void *addr = sa->sa_family == AF_INET6 ? (void *)&((struct sockaddr_in6 *)sa)->sin6_addr
                                               : (void *)&((struct sockaddr_in *)sa)->sin_addr;
        inet_ntop(sa->sa_family, addr, k->ip, sizeof(k->ip));
        if (connect(fd, sa, k->addr_lens[i]) < 0 && errno != EINPROGRESS) {
            k->connect_errno = errno;
            close(fd);
            continue;
        }
        k->fd = fd;
        k->watching = 0;
        if (!track_fd(c, k)) {
            close(fd);
            k->fd = -1;
            k->connect_errno = ENOMEM;
            return -1;
        }
        watch(k, k->h3 ? HX_READ : HX_WRITE);
        return 0;
    }
    return -1;
}

static conn *conn_open(hx_client *c, struct hx_req *q, const struct addrinfo *ai, hx_error *error, int *sys_errno) {
    conn *k = calloc(1, sizeof(conn));
    char *rbuf = malloc(c->cfg.read_size);
    if (k == NULL || rbuf == NULL) {
        *error = HX_ERR_NOMEM;
        free(k);
        free(rbuf);
        return NULL;
    }
    for (; ai != NULL && k->addr_count < MAX_ADDRS; ai = ai->ai_next) {
        if (ai->ai_addrlen <= sizeof(struct sockaddr_storage)) {
            memcpy(&k->addrs[k->addr_count], ai->ai_addr, ai->ai_addrlen);
            k->addr_lens[k->addr_count++] = ai->ai_addrlen;
        }
    }
    k->client = c;
    k->fd = -1;
    k->rbuf = rbuf;
    k->rcap = c->cfg.read_size;
    memcpy(k->key, q->key, sizeof(k->key));
    k->tls = q->tls;
    k->may_h2 = q->h2_offer;
    k->h3 = q->h3_offer;
    if (q->tls != NULL) {
        snprintf(k->session_key, sizeof(k->session_key), "%llu/%s:%u/%llu%s", (unsigned long long)q->tls_cache, q->host,
                 q->port, (unsigned long long)q->tls->id, q->h3_offer ? "/h3" : "");
    }
    hx_h1_init(&k->h1, false);
    if (connect_next(k) == -1) {
        *error = HX_ERR_CONNECT;
        *sys_errno = k->connect_errno;
        hx_h1_free(&k->h1);
        free(k->rbuf);
        free(k);
        return NULL;
    }
    k->id = ++c->next_conn_id;
    k->created = now(c);
    k->pool = q->pool;
    LINK(c->conns, k);
    c->open++;
    if (k->pool != 0 && c->io.connection != NULL) {
        c->io.connection(c->io.ctx, k->pool, 1);
    }
    k->state = CONN_CONNECTING;
    k->req = q;
    q->conn = k;
    if (q->connect_timeout > 0) {
        k->connect_deadline = q->times[HX_T_START] + q->connect_timeout;
        schedule(c, k->connect_deadline);
    }
    return k;
}

static void h3_begin(conn *k);

static void open_for(hx_client *c, struct hx_req *q, const struct addrinfo *ai) {
    q->times[HX_T_DNS] = now(c);
    hx_error error = HX_OK;
    int sys_errno = 0;
    conn *k = conn_open(c, q, ai, &error, &sys_errno);
    if (k == NULL) {
        finish(q, error, sys_errno, false);
    } else if (k->h3) {
        h3_begin(k);
    }
}

static void resolve(hx_client *c, struct hx_req *q) {
    char key[264];
    snprintf(key, sizeof(key), "%s:%u", target_host(q), target_port(q));
    int64_t t = now(c);
    for (cached **p = &c->cache; *p != NULL;) {
        cached *e = *p;
        if (e->expires <= t) {
            *p = e->next;
            freeaddrinfo(e->addrs);
            free(e);
            continue;
        }
        if (strcmp(e->key, key) == 0) {
            open_for(c, q, e->addrs);
            return;
        }
        p = &e->next;
    }
    lookup *l = c->lookups;
    while (l != NULL && strcmp(l->key, key) != 0) {
        l = l->next;
    }
    if (l == NULL) {
        if (c->dns == NULL) {
            c->dns = hx_dns_new();
            if (c->dns == NULL) {
                finish(q, HX_ERR_NOMEM, 0, false);
                return;
            }
            c->io.watch(c->io.ctx, hx_dns_fd(c->dns), HX_READ);
        }
        l = calloc(1, sizeof(lookup));
        if (l == NULL || hx_dns_lookup(c->dns, target_host(q), target_port(q), c->next_lookup_id + 1) == -1) {
            free(l);
            finish(q, HX_ERR_NOMEM, 0, false);
            return;
        }
        l->id = ++c->next_lookup_id;
        memcpy(l->key, key, sizeof(key));
        l->next = c->lookups;
        c->lookups = l;
    }
    q->lookup = l;
    q->dns_next = l->waiting;
    l->waiting = q;
}

static void resolved(hx_client *c) {
    hx_dns_result results[16];
    int count;
    while ((count = hx_dns_results(c->dns, results, 16)) > 0) {
        for (int i = 0; i < count; i++) {
            lookup **p = &c->lookups;
            while (*p != NULL && (*p)->id != results[i].id) {
                p = &(*p)->next;
            }
            lookup *l = *p;
            if (l == NULL) {
                if (results[i].addrs != NULL) {
                    freeaddrinfo(results[i].addrs);
                }
                continue;
            }
            *p = l->next;
            cached *e = results[i].error == 0 ? calloc(1, sizeof(cached)) : NULL;
            if (e != NULL) {
                memcpy(e->key, l->key, sizeof(e->key));
                e->addrs = results[i].addrs;
                e->expires = now(c) + (c->cfg.dns_ttl_ns > 0 ? c->cfg.dns_ttl_ns : 60 * (int64_t)1000000000);
                e->next = c->cache;
                c->cache = e;
            } else if (results[i].addrs != NULL) {
                freeaddrinfo(results[i].addrs);
            }
            while (l->waiting != NULL) {
                struct hx_req *q = l->waiting;
                l->waiting = q->dns_next;
                q->lookup = NULL;
                if (e != NULL) {
                    open_for(c, q, e->addrs);
                } else {
                    finish(q, HX_ERR_DNS, results[i].error, false);
                }
            }
            free(l);
        }
    }
}

static bool still_open(conn *k) {
    char b;
    if (k->ssl != NULL) {
        ERR_clear_error();
        int n = SSL_peek(k->ssl, &b, 1);
        bool open = n <= 0 && SSL_get_error(k->ssl, n) == SSL_ERROR_WANT_READ;
        ERR_clear_error();
        return open;
    }
    return recv(k->fd, &b, 1, MSG_PEEK | MSG_DONTWAIT) < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

static conn *find_idle(hx_client *c, const char *key, int64_t max_age, bool check) {
    int64_t t = max_age > 0 ? now(c) : 0;
    for (conn *k = c->idle_head, *after; k != NULL; k = after) {
        after = k->idle_next;
        if (k->state != CONN_IDLE || k->pinned || multiplexed(k) || strcmp(k->key, key) != 0) {
            continue;
        }
        if ((max_age > 0 && t - k->created >= max_age) || (check && !still_open(k))) {
            conn_close(k);
            continue;
        }
        return k;
    }
    return NULL;
}

static bool h2_usable(conn *k);
static void h2_attach(conn *k, struct hx_req *q);
static void h2_start(conn *k);
static void h3_attach(conn *k, struct hx_req *q);

static bool h3_usable(conn *k) {
    return k->h3 && k->quic != NULL && !k->closing && (k->state == CONN_BUSY || k->state == CONN_IDLE) &&
           hx_h3_can_open(k->quic);
}

static bool pending_multiplex(const conn *k) {
    if (k->closing) {
        return false;
    }
    if (k->h3) {
        return k->state == CONN_HANDSHAKE;
    }
    return k->may_h2 && !k->h2 &&
           (k->state == CONN_CONNECTING || k->state == CONN_TUNNEL || k->state == CONN_HANDSHAKE);
}

static conn *find_h2(hx_client *c, const char *key, const conn *avoid, bool *pending) {
    for (conn *k = c->conns; k != NULL; k = k->next) {
        if (k == avoid || strcmp(k->key, key) != 0) {
            continue;
        }
        if (k->h3 ? h3_usable(k) : h2_usable(k)) {
            return k;
        }
        if (pending_multiplex(k)) {
            *pending = true;
            return k;
        }
    }
    return NULL;
}

static void start(hx_client *c, const hx_request *r, struct hx_req *q);
static void place(hx_client *c, struct hx_req *q, const conn *avoid);

hx_req *hx_request_start(hx_client *c, const hx_request *r, void *user) {
    struct hx_req *q = calloc(1, sizeof(struct hx_req));
    if (q == NULL) {
        return NULL;
    }
    q->client = c;
    q->user = user;
    start(c, r, q);
    process_dirty(c);
    return q;
}

static void start(hx_client *c, const hx_request *r, struct hx_req *q) {
    hx_request plain;
    if (r->http != HX_HTTP1 && (r->upgrade || !r->tls)) {
        plain = *r;
        plain.http = HX_HTTP1;
        r = &plain;
    }
    q->h3_offer = r->http == HX_HTTP3 && r->proxy_host == NULL && hx_h3_available();
    q->h2_offer = r->http != HX_HTTP1 && !q->h3_offer;
    q->stream_id = -1;
    q->head = strcmp(r->method, "HEAD") == 0;
    q->decode = r->decode;
    q->max_age = r->max_age_ns;
    q->pool = r->pool;
    q->upload = r->upload || r->upgrade;
    q->upgrade = r->upgrade;
    q->chunked = r->upload && !r->upgrade && r->upload_size < 0;
    q->fresh = r->fresh_connection;
    q->close_after = r->close_after;
    q->port = r->port;
    q->times[HX_T_START] = now(c);
    q->deadline = r->timeout_ns > 0 ? q->times[HX_T_START] + r->timeout_ns : 0;
    hx_text host = hx_text_on(q->host, sizeof(q->host));
    hx_text_str(&host, r->host);
    q->idle_timeout = r->idle_timeout_ns > 0 ? r->idle_timeout_ns : c->cfg.idle_timeout_ns;
    LINK(c->reqs, q);
    schedule(c, q->deadline);
    if (r->tls) {
        hx_error error = HX_OK;
        q->tls = hx_tls_ctx_for(&c->tls, r, &error);
        if (q->tls == NULL) {
            finish(q, error, 0, false);
            return;
        }
        q->tls_cache = r->tls_cache;
        if (r->proxy_host != NULL) {
            snprintf(q->proxy_host, sizeof(q->proxy_host), "%s", r->proxy_host);
            q->proxy_port = r->proxy_port;
            if (r->proxy_auth != NULL && (q->proxy_auth = strdup(r->proxy_auth)) == NULL) {
                finish(q, HX_ERR_NOMEM, 0, false);
                return;
            }
        }
    }
    hx_text key = hx_text_on(q->key, sizeof(q->key));
    hx_text_uint(&key, r->pool, 0);
    hx_text_str(&key, "/");
    hx_text_str(&key, r->host);
    hx_text_str(&key, ":");
    hx_text_uint(&key, r->port, 0);
    if (r->tls) {
        hx_text_str(&key, "/");
        hx_text_uint(&key, q->tls->id, 0);
        hx_text_str(&key, "/");
        hx_text_uint(&key, r->tls_cache, 0);
        hx_text_str(&key, "/");
        hx_text_str(&key, q->proxy_host);
        hx_text_str(&key, ":");
        hx_text_uint(&key, q->proxy_port, 0);
        if (q->h3_offer) {
            hx_text_str(&key, "/h3");
        }
    }
    hx_h1_request h = {
        r->method,
        r->target,
        r->authority,
        r->headers,
        r->header_count,
        r->upload ? NULL : r->body,
        r->body_len,
        !r->upload || r->upgrade ? HX_H1_NO_UPLOAD : r->upload_size < 0 ? HX_H1_CHUNKED : r->upload_size};
    int rc = hx_h1_write_request(&q->request, &h);
    if (rc != 0) {
        finish(q, rc == HX_EINVAL ? HX_ERR_INVALID : HX_ERR_NOMEM, 0, false);
        return;
    }
    q->connect_timeout = r->connect_timeout_ns;
    place(c, q, NULL);
}

static void place(hx_client *c, struct hx_req *q, const conn *avoid) {
    if ((q->h2_offer || q->h3_offer) && !q->fresh) {
        bool pending = false;
        conn *h = find_h2(c, q->key, avoid, &pending);
        if (h != NULL && pending) {
            q->waiting_on = h;
            q->wait_next = h->waiters;
            h->waiters = q;
            return;
        }
        if (h != NULL) {
            q->reused = true;
            q->times[HX_T_DNS] = q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = q->times[HX_T_START];
            if (h->h3) {
                h3_attach(h, q);
            } else {
                h2_attach(h, q);
            }
            return;
        }
    }
    conn *k = q->fresh ? NULL : find_idle(c, q->key, q->max_age, q->upload);
    if (k != NULL) {
        q->reused = true;
        q->times[HX_T_DNS] = q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = q->times[HX_T_START];
        begin(k, q);
        return;
    }
    connect_new(c, q);
}

static void connect_new(hx_client *c, struct hx_req *q) {
    struct addrinfo hints = {.ai_socktype = SOCK_STREAM, .ai_flags = AI_NUMERICHOST | AI_NUMERICSERV}, *ai = NULL;
    char port[8];
    snprintf(port, sizeof(port), "%u", target_port(q));
    if (getaddrinfo(target_host(q), port, &hints, &ai) == 0) {
        open_for(c, q, ai);
        freeaddrinfo(ai);
    } else {
        resolve(c, q);
    }
}

static hx_buf *body_buffer(struct hx_req *q) {
    conn *k = q->conn;
    return k != NULL && k->req == q && k->state == CONN_BUSY ? &k->out : &q->request;
}

static void body_written(struct hx_req *q) {
    q->asked = false;
    conn *k = q->conn;
    if (k != NULL && k->req == q && k->state == CONN_BUSY) {
        send_some(k);
    }
}

static void drop_sent(struct hx_req *q) {
    q->held_dropped = true;
    if (q->held_off == 0) {
        return;
    }
    memmove(q->held.data, q->held.data + q->held_off, q->held.len - q->held_off);
    q->held.len -= q->held_off;
    q->held_off = 0;
}

static void h2_resume(struct hx_req *q) {
    q->asked = false;
    conn *k = q->conn;
    if (k != NULL && k->h2 && q->stream_id > 0 && !q->stream_closed && q->deferred) {
        q->deferred = false;
        nghttp2_session_resume_data(k->ng, q->stream_id);
        mark_dirty(k);
        process_dirty(q->client);
    }
}

static bool h3_attached(const struct hx_req *q) {
    return q->conn != NULL && q->conn->h3 && q->stream_id >= 0 && !q->stream_closed;
}

static void keep_for_retry(struct hx_req *q) {
    if (h3_attached(q) && q->held.len > KEEP_FOR_RETRY) {
        hx_buf_free(&q->held);
        q->held_dropped = true;
    }
}

static int h3_body(struct hx_req *q, const char *data, size_t len, bool end) {
    if (!q->held_dropped && len > 0 && !hx_buf_append(&q->held, data, len)) {
        return -1;
    }
    q->asked = false;
    if (h3_attached(q)) {
        conn *k = q->conn;
        if (hx_h3_send(k->quic, q->stream_id, data, len, end) != 0) {
            return -1;
        }
        q->sent += len;
        mark_dirty(k);
    }
    keep_for_retry(q);
    process_dirty(q->client);
    return 0;
}

int hx_request_send(hx_req *q, const char *data, size_t len) {
    if (q->done || !q->upload || q->body_done) {
        return -1;
    }
    if (len == 0) {
        return 0;
    }
    if (q->h3_offer) {
        return h3_body(q, data, len, false);
    }
    if (q->h2_offer) {
        if (!hx_buf_append(&q->held, data, len)) {
            return -1;
        }
        if (q->held.len > KEEP_FOR_RETRY && q->held_off > 0) {
            drop_sent(q);
        }
        h2_resume(q);
        return 0;
    }
    hx_buf *out = body_buffer(q);
    if (q->chunked ? hx_h1_write_chunk(out, data, len) != 0 : !hx_buf_append(out, data, len)) {
        return -1;
    }
    body_written(q);
    return 0;
}

int hx_request_end_body(hx_req *q) {
    if (q->done || !q->upload || q->body_done) {
        return -1;
    }
    if (q->h3_offer) {
        q->body_done = true;
        return h3_body(q, NULL, 0, true);
    }
    if (q->h2_offer) {
        q->body_done = true;
        h2_resume(q);
        return 0;
    }
    if (q->chunked && !hx_buf_append(body_buffer(q), "0\r\n\r\n", 5)) {
        return -1;
    }
    q->body_done = true;
    body_written(q);
    return 0;
}

static void receive_ready(conn *k);

void hx_request_pause(hx_req *q, bool paused) {
    if (q->done || q->paused == paused) {
        return;
    }
    q->paused = paused;
    conn *k = q->conn;
    if (k != NULL && k->h3) {
        if (!paused && q->unconsumed > 0 && h3_attached(q)) {
            hx_h3_consume(k->quic, q->stream_id, q->unconsumed);
            q->unconsumed = 0;
            mark_dirty(k);
            process_dirty(q->client);
        }
        return;
    }
    if (k != NULL && k->h2) {
        if (!paused && q->unconsumed > 0 && !q->stream_closed) {
            nghttp2_session_consume(k->ng, q->stream_id, q->unconsumed);
            q->unconsumed = 0;
            mark_dirty(k);
            process_dirty(q->client);
        }
        return;
    }
    if (k != NULL && k->req == q && k->state == CONN_BUSY) {
        watch(k, reading(k) | (k->watching & HX_WRITE));
        if (!paused && k->ssl != NULL && SSL_pending(k->ssl) > 0) {
            if (k->pinned) {
                k->want_read = true;
            } else {
                receive_ready(k);
            }
        }
    }
}

void hx_request_cancel(hx_req *q) {
    if (q->done) {
        return;
    }
    finish(q, HX_ERR_CANCELLED, 0, true);
    process_dirty(q->client);
}

int hx_tls_on_session(SSL *ssl, SSL_SESSION *session) {
    conn *k = hx_tls_conn(ssl);
    if (k == NULL || k->state == CONN_CLOSED) {
        return 0;
    }
    hx_tls_save(&k->client->tls, k->session_key, session);
    return 1;
}

static void handshake(conn *k) {
    ERR_clear_error();
    int rc = SSL_connect(k->ssl);
    if (rc == 1) {
        struct hx_req *q = k->req;
        q->times[HX_T_TLS] = now(k->client);
        k->connect_deadline = 0;
        const unsigned char *proto = NULL;
        unsigned int proto_len = 0;
        SSL_get0_alpn_selected(k->ssl, &proto, &proto_len);
        if (proto_len == 2 && memcmp(proto, "h2", 2) == 0) {
            h2_start(k);
            return;
        }
        k->may_h2 = false;
        while (k->waiters != NULL) {
            struct hx_req *w = k->waiters;
            unwait(w);
            connect_new(k->client, w);
        }
        k->req = NULL;
        begin(k, q);
        return;
    }
    int err = SSL_get_error(k->ssl, rc);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        watch(k, err == SSL_ERROR_WANT_READ ? HX_READ : HX_WRITE);
        return;
    }
    long verify = SSL_get_verify_result(k->ssl);
    if (k->tls->verify && verify != X509_V_OK) {
        k->tls_detail = X509_verify_cert_error_string(verify);
        ERR_clear_error();
        fail(k, HX_ERR_TLS_VERIFY, (int)verify);
        return;
    }
    unsigned long e = ERR_peek_last_error();
    k->tls_detail = e != 0 ? ERR_reason_error_string(e) : NULL;
    ERR_clear_error();
    fail(k, HX_ERR_TLS, err == SSL_ERROR_SYSCALL ? errno : 0);
}

static void start_tls(conn *k) {
    k->ssl = hx_tls_new(&k->client->tls, k->tls, k->req->host, k->session_key, k);
    if (k->ssl == NULL || SSL_set_fd(k->ssl, k->fd) != 1) {
        fail(k, HX_ERR_NOMEM, 0);
        return;
    }
    k->state = CONN_HANDSHAKE;
    handshake(k);
}

static bool head_complete(const char *buf, size_t len) {
    for (size_t i = 3; i < len; i++) {
        if (buf[i] == '\n' && buf[i - 1] == '\r' && buf[i - 2] == '\n' && buf[i - 3] == '\r') {
            return true;
        }
    }
    return false;
}

static void tunnel(conn *k) {
    while (k->out_sent < k->out.len) {
        ssize_t n = send(k->fd, k->out.data + k->out_sent, k->out.len - k->out_sent, MSG_NOSIGNAL);
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            watch(k, HX_WRITE);
            return;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            fail(k, HX_ERR_SEND, errno);
            return;
        }
        k->out_sent += (size_t)n;
    }
    watch(k, HX_READ);
    for (;;) {
        if (k->tunnel_len == k->rcap) {
            fail(k, HX_ERR_TUNNEL, 0);
            return;
        }
        ssize_t n = recv(k->fd, k->rbuf + k->tunnel_len, k->rcap - k->tunnel_len, 0);
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            fail(k, n == 0 ? HX_ERR_TUNNEL : HX_ERR_RECEIVE, n == 0 ? 0 : errno);
            return;
        }
        k->tunnel_len += (size_t)n;
        if (!head_complete(k->rbuf, k->tunnel_len)) {
            continue;
        }
        int status = k->tunnel_len > 12 && memcmp(k->rbuf, "HTTP/1.", 7) == 0 ? atoi(k->rbuf + 9) : 0;
        k->out.len = k->out_sent = 0;
        k->tunnel_len = 0;
        if (status < 200 || status > 299) {
            fail(k, HX_ERR_TUNNEL, status);
            return;
        }
        start_tls(k);
        return;
    }
}

static void connected(conn *k) {
    struct pollfd p = {.fd = k->fd, .events = POLLOUT};
    if (poll(&p, 1, 0) == 0) {
        return;
    }
    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(k->fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        hx_client *c = k->client;
        k->connect_errno = err;
        watch(k, 0);
        c->by_fd[k->fd] = NULL;
        close(k->fd);
        k->fd = -1;
        if (connect_next(k) == -1) {
            fail(k, HX_ERR_CONNECT, k->connect_errno);
        }
        return;
    }
    struct hx_req *q = k->req;
    q->times[HX_T_CONNECT] = now(k->client);
    if (k->tls != NULL && q->proxy_host[0]) {
        bool v6 = strchr(q->host, ':') != NULL;
        char line[600];
        int n =
            snprintf(line, sizeof(line),
                     v6 ? "CONNECT [%s]:%u HTTP/1.1\r\nHost: [%s]:%u\r\n" : "CONNECT %s:%u HTTP/1.1\r\nHost: %s:%u\r\n",
                     q->host, q->port, q->host, q->port);
        k->out.len = k->out_sent = 0;
        bool ok = hx_buf_append(&k->out, line, (size_t)n) &&
                  (q->proxy_auth == NULL ||
                   (hx_buf_append_str(&k->out, q->proxy_auth) && hx_buf_append(&k->out, "\r\n", 2))) &&
                  hx_buf_append(&k->out, "\r\n", 2);
        if (!ok) {
            fail(k, HX_ERR_NOMEM, 0);
            return;
        }
        k->state = CONN_TUNNEL;
        k->tunnel_len = 0;
        tunnel(k);
        return;
    }
    if (k->tls != NULL) {
        start_tls(k);
        return;
    }
    q->times[HX_T_TLS] = q->times[HX_T_CONNECT];
    k->connect_deadline = 0;
    k->req = NULL;
    begin(k, q);
}

static const char *find_header(const char *block, size_t len, const char *name, size_t *value_len) {
    size_t n = strlen(name);
    const char *end = block + len;
    const char *line = memchr(block, '\n', len);
    while (line != NULL && ++line < end) {
        const char *eol = memchr(line, '\r', (size_t)(end - line));
        if (eol == NULL || eol == line) {
            return NULL;
        }
        if ((size_t)(eol - line) > n + 1 && line[n] == ':' && strncasecmp(line, name, n) == 0) {
            const char *v = line + n + 1;
            while (v < eol && (*v == ' ' || *v == '\t')) {
                v++;
            }
            *value_len = (size_t)(eol - v);
            return v;
        }
        line = memchr(eol, '\n', (size_t)(end - eol));
    }
    return NULL;
}

static void start_decoding(conn *k, const hx_h1_event *ev) {
    size_t len = 0;
    const char *value = find_header(ev->data, ev->len, "content-encoding", &len);
    if (value == NULL || k->h1.status == 204 || k->h1.status == 304) {
        return;
    }
    int rc = hx_decoder_start(&k->decoder, value, len);
    k->decoding = rc != 0;
    k->undecodable = rc == -1;
}

static int flush_decoded(conn *k, struct hx_req *q) {
    if (k->decoded.len == 0) {
        return 0;
    }
    queued *e = enqueue(k->client, q, HX_EV_BODY);
    if (e == NULL) {
        fail(k, HX_ERR_NOMEM, 0);
        return -1;
    }
    e->ev.data = k->decoded.data;
    e->ev.len = k->decoded.len;
    e->ev.status = k->h1.status;
    e->ev.version = k->h1.version;
    e->pin = k;
    k->queued++;
    k->pinned = true;
    return 0;
}

static struct hx_req *stream_req(nghttp2_session *ng, int32_t id) {
    struct hx_req *q = nghttp2_session_get_stream_user_data(ng, id);
    return q != NULL && !q->done ? q : NULL;
}

static bool is_name(const uint8_t *name, size_t len, const char *want) {
    return len == strlen(want) && memcmp(name, want, len) == 0;
}

static bool stream_header(conn *k, struct hx_req *q, const uint8_t *name, size_t namelen, const uint8_t *value,
                          size_t valuelen) {
    if (is_name(name, namelen, ":status")) {
        if (q->times[HX_T_FIRST_BYTE] == 0) {
            q->times[HX_T_FIRST_BYTE] = now(k->client);
        }
        char line[32];
        q->h2_status = valuelen == 3 ? (value[0] - '0') * 100 + (value[1] - '0') * 10 + (value[2] - '0') : 0;
        int n = snprintf(line, sizeof(line), "HTTP/%d %d \r\n", k->h3 ? 3 : 2, q->h2_status);
        q->h2_head.len = 0;
        return hx_buf_append(&q->h2_head, line, (size_t)n);
    }
    return hx_buf_append(&q->h2_head, (const char *)name, namelen) && hx_buf_append(&q->h2_head, ": ", 2) &&
           hx_buf_append(&q->h2_head, (const char *)value, valuelen) && hx_buf_append(&q->h2_head, "\r\n", 2);
}

static int h2_on_header(nghttp2_session *ng, const nghttp2_frame *frame, const uint8_t *name, size_t namelen,
                        const uint8_t *value, size_t valuelen, uint8_t flags, void *user) {
    if (frame->hd.type != NGHTTP2_HEADERS) {
        return 0;
    }
    struct hx_req *q = stream_req(ng, frame->hd.stream_id);
    if (q == NULL || q->headers_seen) {
        return 0;
    }
    if (!stream_header(user, q, name, namelen, value, valuelen)) {
        q->h2_error = HX_ERR_NOMEM;
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }
    return 0;
}

static void stream_reset(conn *k, struct hx_req *q, hx_error error) {
    q->h2_error = error;
    if (k->h3) {
        hx_h3_reset(k->quic, q->stream_id);
    } else {
        nghttp2_submit_rst_stream(k->ng, NGHTTP2_FLAG_NONE, q->stream_id, NGHTTP2_INTERNAL_ERROR);
    }
}

static void h2_headers_done(conn *k, struct hx_req *q) {
    if (q->upload && !q->held_dropped && k->h3) {
        hx_buf_free(&q->held);
        q->held_dropped = true;
    } else if (q->upload && !q->held_dropped) {
        drop_sent(q);
    }
    if (q->h2_status >= 100 && q->h2_status < 200) {
        q->h2_head.len = 0;
        q->h2_status = 0;
        return;
    }
    if (!hx_buf_append(&q->h2_head, "\r\n", 2)) {
        stream_reset(k, q, HX_ERR_NOMEM);
        return;
    }
    q->headers_seen = true;
    if (q->decode && q->h2_status != 204 && q->h2_status != 304 && !q->head) {
        size_t len = 0;
        const char *value = find_header(q->h2_head.data, q->h2_head.len, "content-encoding", &len);
        if (value != NULL) {
            q->dec = calloc(1, sizeof(hx_decoder));
            int rc = q->dec != NULL ? hx_decoder_start(q->dec, value, len) : -2;
            if (rc == 0) {
                hx_decoder_free(q->dec);
                free(q->dec);
                q->dec = NULL;
            } else if (rc < 0) {
                q->dec_bad = true;
            }
        }
    }
    queued *e = enqueue(k->client, q, HX_EV_HEADERS);
    if (e == NULL) {
        stream_reset(k, q, HX_ERR_NOMEM);
        return;
    }
    e->ev.data = q->h2_head.data;
    e->ev.len = q->h2_head.len;
    e->ev.status = q->h2_status;
    e->ev.version = version_of(k);
}

static int h2_on_frame(nghttp2_session *ng, const nghttp2_frame *frame, void *user) {
    conn *k = user;
    if (frame->hd.type == NGHTTP2_HEADERS && (frame->hd.flags & NGHTTP2_FLAG_END_HEADERS)) {
        struct hx_req *q = stream_req(ng, frame->hd.stream_id);
        if (q != NULL && !q->headers_seen) {
            h2_headers_done(k, q);
        }
    } else if (frame->hd.type == NGHTTP2_DATA && frame->data.padlen > 0) {
        nghttp2_session_consume(ng, frame->hd.stream_id, frame->data.padlen);
    } else if (frame->hd.type == NGHTTP2_GOAWAY) {
        k->closing = true;
    }
    return 0;
}

static void h2_fail_stream(conn *k, struct hx_req *q, hx_error error) {
    if (k->h3) {
        stream_reset(k, q, error);
        return;
    }
    q->h2_error = error;
    nghttp2_submit_rst_stream(k->ng, NGHTTP2_FLAG_NONE, q->stream_id, NGHTTP2_CANCEL);
}

static int h2_on_data(nghttp2_session *ng, uint8_t flags, int32_t id, const uint8_t *data, size_t len, void *user) {
    conn *k = user;
    struct hx_req *q = stream_req(ng, id);
    if (q == NULL || q->h2_error != HX_OK) {
        nghttp2_session_consume(ng, id, len);
        return 0;
    }
    q->received += len;
    if (q->paused) {
        q->unconsumed += len;
    } else {
        nghttp2_session_consume(ng, id, len);
    }
    if (q->dec_bad) {
        h2_fail_stream(k, q, HX_ERR_UNKNOWN_ENCODING);
        return 0;
    }
    if (q->dec != NULL) {
        if (!q->touched) {
            q->touched = true;
            q->dec_out.len = 0;
            q->touched_next = k->touched;
            k->touched = q;
        }
        if (hx_decoder_feed(q->dec, (const char *)data, len, &q->dec_out) == -1) {
            h2_fail_stream(k, q, HX_ERR_DECODE);
        }
        return 0;
    }
    queued *e = enqueue(k->client, q, HX_EV_BODY);
    if (e == NULL) {
        h2_fail_stream(k, q, HX_ERR_NOMEM);
        return 0;
    }
    e->ev.data = (const char *)data;
    e->ev.len = len;
    e->ev.status = q->h2_status;
    e->ev.version = 20;
    e->pin = k;
    k->queued++;
    k->pinned = true;
    return 0;
}

static void h2_flush_decoded(conn *k, struct hx_req *q) {
    q->touched = false;
    if (q->done || q->dec_out.len == 0) {
        return;
    }
    queued *e = enqueue(k->client, q, HX_EV_BODY);
    if (e == NULL) {
        h2_fail_stream(k, q, HX_ERR_NOMEM);
        return;
    }
    e->ev.data = q->dec_out.data;
    e->ev.len = q->dec_out.len;
    e->ev.status = q->h2_status;
    e->ev.version = version_of(k);
    e->pin = k;
    k->queued++;
    k->pinned = true;
}

static void stream_closed(conn *k, struct hx_req *q, bool clean, bool refused) {
    q->stream_closed = true;
    if (q->touched) {
        h2_flush_decoded(k, q);
    }
    hx_error error = q->h2_error != HX_OK       ? q->h2_error
                     : clean && q->headers_seen ? HX_OK
                     : clean                    ? HX_ERR_PROTOCOL
                                                : HX_ERR_RECEIVE;
    if (refused && !q->headers_seen && !q->retried && !(q->upload && q->held_dropped)) {
        h2_detach(k, q);
        q->held_off = 0;
        q->deferred = false;
        q->retried = true;
        q->reused = false;
        q->stream_id = -1;
        place(k->client, q, k);
        return;
    }
    finish(q, error, 0, false);
}

static int h2_on_close(nghttp2_session *ng, int32_t id, uint32_t code, void *user) {
    struct hx_req *q = stream_req(ng, id);
    if (q == NULL) {
        return 0;
    }
    nghttp2_session_set_stream_user_data(ng, id, NULL);
    stream_closed(user, q, code == NGHTTP2_NO_ERROR, code == NGHTTP2_REFUSED_STREAM);
    return 0;
}

static nghttp2_ssize h2_read_body(nghttp2_session *ng, int32_t id, uint8_t *buf, size_t len, uint32_t *flags,
                                  nghttp2_data_source *source, void *user) {
    struct hx_req *q = source->ptr;
    if (q->upload) {
        size_t held = q->held.len - q->held_off;
        size_t m = held < len ? held : len;
        if (m > 0) {
            memcpy(buf, q->held.data + q->held_off, m);
        }
        q->held_off += m;
        q->sent += m;
        if (q->held_off == q->held.len) {
            if (q->held_dropped) {
                q->held.len = q->held_off = 0;
            }
            if (q->body_done) {
                *flags |= NGHTTP2_DATA_FLAG_EOF;
            } else if (!q->asked) {
                q->asked = true;
                enqueue(q->client, q, HX_EV_SEND_READY);
            }
        }
        if (m == 0 && !q->body_done) {
            q->deferred = true;
            return NGHTTP2_ERR_DEFERRED;
        }
        return (nghttp2_ssize)m;
    }
    size_t left = q->request.len - q->body_off;
    size_t n = left < len ? left : len;
    memcpy(buf, q->request.data + q->body_off, n);
    q->body_off += n;
    q->sent += n;
    if (q->body_off == q->request.len) {
        *flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return (nghttp2_ssize)n;
}

static bool hop_header(const char *name, size_t len) {
    static const char *const hops[] = {"connection",        "keep-alive", "proxy-connection",
                                       "transfer-encoding", "upgrade",    "host"};
    for (size_t i = 0; i < sizeof(hops) / sizeof(hops[0]); i++) {
        if (len == strlen(hops[i]) && strncasecmp(name, hops[i], len) == 0) {
            return true;
        }
    }
    return false;
}

static nghttp2_nv *request_nv(struct hx_req *q, size_t *count, char **names_out, hx_error *error) {
    char *text = q->request.data;
    size_t len = q->request.len;
    const char *end = NULL;
    for (size_t i = 3; i < len; i++) {
        if (text[i] == '\n' && text[i - 1] == '\r' && text[i - 2] == '\n' && text[i - 3] == '\r') {
            end = text + i + 1;
            break;
        }
    }
    const char *sp1 = memchr(text, ' ', len), *eol = memchr(text, '\r', len);
    const char *sp2 = sp1 != NULL && eol != NULL ? memchr(sp1 + 1, ' ', (size_t)(eol - sp1 - 1)) : NULL;
    size_t lines = 0;
    for (const char *p = text; end != NULL && p < end; p++) {
        lines += *p == '\n';
    }
    nghttp2_nv *nva = end != NULL && sp2 != NULL ? calloc(lines + 4, sizeof(nghttp2_nv)) : NULL;
    char *names = nva != NULL ? malloc((size_t)(end - text)) : NULL;
    if (names == NULL) {
        free(nva);
        *error = end == NULL || sp2 == NULL ? HX_ERR_INVALID : HX_ERR_NOMEM;
        return NULL;
    }
    size_t n = 0;
    nva[n++] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)text, 7, (size_t)(sp1 - text), 0};
    nva[n++] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)"https", 7, 5, 0};
    size_t authority_at = n++;
    nva[authority_at] = (nghttp2_nv){(uint8_t *)":authority", (uint8_t *)q->host, 10, strlen(q->host), 0};
    nva[n++] = (nghttp2_nv){(uint8_t *)":path", (uint8_t *)sp1 + 1, 5, (size_t)(sp2 - sp1 - 1), 0};
    for (const char *line = eol + 2; line < end - 2;) {
        const char *stop = memchr(line, '\r', (size_t)(end - line));
        const char *colon = memchr(line, ':', (size_t)(stop - line));
        if (colon != NULL) {
            size_t nl = (size_t)(colon - line);
            const char *v = colon + 1;
            while (v < stop && (*v == ' ' || *v == '\t')) {
                v++;
            }
            char *lname = names + (line - text);
            for (size_t i = 0; i < nl; i++) {
                char ch = line[i];
                lname[i] = ch >= 'A' && ch <= 'Z' ? (char)(ch - 'A' + 'a') : ch;
            }
            if (nl == 4 && strncasecmp(line, "host", 4) == 0) {
                nva[authority_at].value = (uint8_t *)v;
                nva[authority_at].valuelen = (size_t)(stop - v);
            } else if (!hop_header(line, nl) && !(nl == 2 && strncasecmp(line, "te", 2) == 0 &&
                                                  !((size_t)(stop - v) == 8 && strncasecmp(v, "trailers", 8) == 0))) {
                nva[n++] = (nghttp2_nv){(uint8_t *)lname, (uint8_t *)v, nl, (size_t)(stop - v), 0};
            }
        }
        line = stop + 2;
    }
    q->body_off = (size_t)(end - text);
    *count = n;
    *names_out = names;
    return nva;
}

static void link_stream(conn *k, struct hx_req *q, int64_t id);

static void h2_attach(conn *k, struct hx_req *q) {
    size_t n = 0;
    char *names = NULL;
    hx_error error = HX_OK;
    nghttp2_nv *nva = request_nv(q, &n, &names, &error);
    if (nva == NULL) {
        q->conn = NULL;
        finish(q, error, 0, false);
        return;
    }
    nghttp2_data_provider2 body = {.source.ptr = q, .read_callback = h2_read_body};
    int32_t id =
        nghttp2_submit_request2(k->ng, NULL, nva, n, q->body_off < q->request.len || q->upload ? &body : NULL, q);
    free(nva);
    free(names);
    if (id < 0) {
        q->conn = NULL;
        finish(q, HX_ERR_PROTOCOL, 0, false);
        return;
    }
    link_stream(k, q, id);
}

static void link_stream(conn *k, struct hx_req *q, int64_t id) {
    q->stream_id = id;
    q->stream_closed = false;
    q->conn = k;
    q->s_prev = NULL;
    q->s_next = k->stream_list;
    if (k->stream_list != NULL) {
        k->stream_list->s_prev = q;
    }
    k->stream_list = q;
    k->streams++;
    if (q->close_after) {
        k->close_after = true;
    }
    if (k->state == CONN_IDLE) {
        k->client->idle--;
        k->state = CONN_BUSY;
    }
    mark_dirty(k);
}

static void h2_start(conn *k) {
    hx_client *c = k->client;
    struct hx_req *q = k->req;
    k->req = NULL;
    if (nghttp2_session_client_new2(&k->ng, c->h2_callbacks, k, c->h2_option) != 0) {
        k->ng = NULL;
        k->req = q;
        fail(k, HX_ERR_NOMEM, 0);
        return;
    }
    nghttp2_settings_entry settings[] = {{NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
                                         {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 1 << 20}};
    nghttp2_submit_settings(k->ng, NGHTTP2_FLAG_NONE, settings, 2);
    nghttp2_session_set_local_window_size(k->ng, NGHTTP2_FLAG_NONE, 0, 1 << 26);
    k->h2 = true;
    k->state = CONN_BUSY;
    watch(k, HX_READ);
    h2_attach(k, q);
    while (k->waiters != NULL) {
        struct hx_req *w = k->waiters;
        unwait(w);
        w->reused = true;
        w->times[HX_T_DNS] = w->times[HX_T_CONNECT] = w->times[HX_T_TLS] = w->times[HX_T_START];
        h2_attach(k, w);
    }
    mark_dirty(k);
}

static bool h2_usable(conn *k) {
    return k->h2 && k->ng != NULL && !k->closing && (k->state == CONN_BUSY || k->state == CONN_IDLE) &&
           nghttp2_session_check_request_allowed(k->ng) &&
           k->streams < nghttp2_session_get_remote_settings(k->ng, NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS);
}

static void h2_receive(conn *k, ssize_t n) {
    if (n <= 0) {
        if (k->streams == 0 && n == 0) {
            conn_close(k);
        } else {
            fail(k, HX_ERR_RECEIVE, n < 0 ? errno : 0);
        }
        return;
    }
    k->in_ng = true;
    nghttp2_ssize rv = nghttp2_session_mem_recv2(k->ng, (const uint8_t *)k->rbuf, (size_t)n);
    k->in_ng = false;
    while (k->touched != NULL) {
        struct hx_req *q = k->touched;
        k->touched = q->touched_next;
        h2_flush_decoded(k, q);
    }
    if (rv < 0) {
        fail(k, HX_ERR_PROTOCOL, 0);
        return;
    }
    mark_dirty(k);
}

static void h2_flush(conn *k) {
    hx_client *c = k->client;
    for (;;) {
        if (k->out_sent == k->out.len) {
            k->out.len = k->out_sent = 0;
            k->in_ng = true;
            while (k->out.len < 65536) {
                const uint8_t *data;
                nghttp2_ssize m = nghttp2_session_mem_send2(k->ng, &data);
                if (m < 0) {
                    k->in_ng = false;
                    fail(k, HX_ERR_PROTOCOL, 0);
                    return;
                }
                if (m == 0) {
                    break;
                }
                if (!hx_buf_append(&k->out, (const char *)data, (size_t)m)) {
                    k->in_ng = false;
                    fail(k, HX_ERR_NOMEM, 0);
                    return;
                }
            }
            k->in_ng = false;
            if (k->out.len == 0) {
                break;
            }
        }
        ssize_t n = conn_write(k, k->out.data + k->out_sent, k->out.len - k->out_sent);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            watch(k, HX_READ | HX_WRITE);
            return;
        }
        if (n < 0) {
            fail(k, HX_ERR_SEND, errno);
            return;
        }
        k->out_sent += (size_t)n;
    }
    watch(k, HX_READ);
    int64_t t = now(c);
    for (struct hx_req *q = k->stream_list; q != NULL; q = q->s_next) {
        if (q->times[HX_T_FIRST_SENT] == 0) {
            q->times[HX_T_FIRST_SENT] = t;
        }
        if (q->times[HX_T_LAST_SENT] == 0 &&
            (q->upload ? q->body_done && q->held.len == 0 : q->body_off >= q->request.len)) {
            q->times[HX_T_LAST_SENT] = t;
        }
    }
}

static void h2_settle(conn *k) {
    hx_client *c = k->client;
    if (k->streams > 0 || k->state != CONN_BUSY) {
        return;
    }
    bool refused = k->h3 ? hx_h3_goaway(k->quic) : !nghttp2_session_check_request_allowed(k->ng);
    if (k->closing || k->close_after || refused || (c->cfg.max_idle > 0 && c->idle >= c->cfg.max_idle)) {
        conn_close(k);
        return;
    }
    k->state = CONN_IDLE;
    c->idle++;
    k->idle_since = now(c);
    schedule(c, k->idle_timeout > 0 ? k->idle_since + k->idle_timeout : 0);
}

static void h3_on_header(void *user, void *stream, const uint8_t *name, size_t namelen, const uint8_t *value,
                         size_t valuelen) {
    struct hx_req *q = stream;
    if (q->done || q->headers_seen || q->h2_error != HX_OK) {
        return;
    }
    if (!stream_header(user, q, name, namelen, value, valuelen)) {
        stream_reset(user, q, HX_ERR_NOMEM);
    }
}

static void h3_on_headers_done(void *user, void *stream) {
    struct hx_req *q = stream;
    if (!q->done && !q->headers_seen && q->h2_error == HX_OK) {
        h2_headers_done(user, q);
    }
}

static void h3_on_data(void *user, void *stream, const uint8_t *data, size_t len) {
    conn *k = user;
    struct hx_req *q = stream;
    if (q->done || q->h2_error != HX_OK) {
        hx_h3_consume(k->quic, q->stream_id, len);
        return;
    }
    q->received += len;
    if (q->paused) {
        q->unconsumed += len;
    } else {
        hx_h3_consume(k->quic, q->stream_id, len);
    }
    if (q->dec_bad) {
        h2_fail_stream(k, q, HX_ERR_UNKNOWN_ENCODING);
        return;
    }
    if (!q->touched) {
        q->touched = true;
        q->dec_out.len = 0;
        q->touched_next = k->touched;
        k->touched = q;
    }
    if (q->dec != NULL) {
        if (hx_decoder_feed(q->dec, (const char *)data, len, &q->dec_out) == -1) {
            h2_fail_stream(k, q, HX_ERR_DECODE);
        }
    } else if (!hx_buf_append(&q->dec_out, (const char *)data, len)) {
        h2_fail_stream(k, q, HX_ERR_NOMEM);
    }
}

static void h3_on_want_body(void *user, void *stream) {
    struct hx_req *q = stream;
    if (!q->done && q->upload && !q->body_done && !q->asked) {
        q->asked = true;
        enqueue(q->client, q, HX_EV_SEND_READY);
    }
}

static void h3_on_closed(void *user, void *stream, uint64_t code) {
    stream_closed(user, stream, code == HX_H3_NO_ERROR, code == HX_H3_REJECTED);
}

static const hx_h3_handler h3_handler = {h3_on_header, h3_on_headers_done, h3_on_data, h3_on_want_body, h3_on_closed};

static void h3_begin(conn *k) {
    struct hx_req *q = k->req;
    SSL *ssl = hx_tls_new_quic(&k->client->tls, k->tls, q->host, k->session_key, k);
    k->quic = ssl != NULL ? hx_h3_new(k->fd, ssl, &h3_handler, k, now(k->client), q->connect_timeout) : NULL;
    if (k->quic == NULL) {
        SSL_free(ssl);
        fail(k, HX_ERR_NOMEM, 0);
        return;
    }
    k->state = CONN_HANDSHAKE;
    mark_dirty(k);
}

static void h3_attach(conn *k, struct hx_req *q) {
    if (!hx_h3_can_open(k->quic)) {
        q->conn = NULL;
        place(k->client, q, k);
        return;
    }
    size_t n = 0;
    char *names = NULL;
    hx_error error = HX_OK;
    nghttp2_nv *nva = request_nv(q, &n, &names, &error);
    if (nva == NULL) {
        q->conn = NULL;
        finish(q, error, 0, false);
        return;
    }
    bool body = q->upload || q->body_off < q->request.len;
    int64_t id = hx_h3_open(k->quic, nva, n, body, q);
    free(nva);
    free(names);
    if (id < 0) {
        q->conn = NULL;
        finish(q, HX_ERR_PROTOCOL, 0, false);
        return;
    }
    link_stream(k, q, id);
    if (!body) {
        return;
    }
    const char *data = q->upload ? q->held.data : q->request.data + q->body_off;
    size_t len = q->upload ? q->held.len : q->request.len - q->body_off;
    if (hx_h3_send(k->quic, id, data, len, !q->upload || q->body_done) != 0) {
        h2_fail_stream(k, q, HX_ERR_NOMEM);
        return;
    }
    q->sent += len;
    keep_for_retry(q);
}

static void h3_start(conn *k) {
    hx_client *c = k->client;
    struct hx_req *q = k->req;
    k->req = NULL;
    k->state = CONN_BUSY;
    k->connect_deadline = 0;
    if (q != NULL) {
        q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = now(c);
        h3_attach(k, q);
    }
    while (k->waiters != NULL) {
        struct hx_req *w = k->waiters;
        unwait(w);
        w->reused = true;
        w->times[HX_T_DNS] = w->times[HX_T_CONNECT] = w->times[HX_T_TLS] = w->times[HX_T_START];
        h3_attach(k, w);
    }
    mark_dirty(k);
}

static void h3_failed(conn *k, int rc, int sys_errno) {
    bool handshaking = k->state == CONN_HANDSHAKE;
    if (rc == HX_H3_CLOSED && !handshaking && k->streams == 0) {
        conn_close(k);
        return;
    }
    if (rc == HX_H3_TLS) {
        long verify = SSL_get_verify_result(hx_h3_ssl(k->quic));
        if (k->tls->verify && verify != X509_V_OK) {
            k->tls_detail = X509_verify_cert_error_string(verify);
            ERR_clear_error();
            fail(k, HX_ERR_TLS_VERIFY, (int)verify);
            return;
        }
        unsigned long e = ERR_peek_last_error();
        k->tls_detail = e != 0 ? ERR_reason_error_string(e) : NULL;
        ERR_clear_error();
    }
    hx_error error = rc == HX_H3_NOMEM                          ? HX_ERR_NOMEM
                     : rc == HX_H3_TIMEOUT                      ? HX_ERR_TIMEOUT
                     : rc == HX_H3_TLS                          ? HX_ERR_TLS
                     : handshaking                              ? HX_ERR_CONNECT
                     : rc == HX_H3_FAILED || rc == HX_H3_CLOSED ? HX_ERR_RECEIVE
                                                                : HX_ERR_PROTOCOL;
    fail(k, error, sys_errno);
}

static void h3_receive(conn *k) {
    hx_client *c = k->client;
    int sys_errno = 0;
    conn *outer = c->receiving;
    c->receiving = k;
    int rc = hx_h3_read(k->quic, now(c), &sys_errno);
    while (k->touched != NULL) {
        struct hx_req *q = k->touched;
        k->touched = q->touched_next;
        h2_flush_decoded(k, q);
    }
    c->receiving = outer;
    mark_dirty(k);
    if (rc != HX_H3_OK) {
        h3_failed(k, rc, sys_errno);
        return;
    }
    if (hx_h3_goaway(k->quic)) {
        k->closing = true;
    }
    if (k->state == CONN_HANDSHAKE && hx_h3_ready(k->quic)) {
        h3_start(k);
    }
}

static void h3_flush(conn *k) {
    hx_client *c = k->client;
    int sys_errno = 0;
    int rc = hx_h3_write(k->quic, now(c), &sys_errno);
    if (rc < 0) {
        h3_failed(k, rc, sys_errno);
        return;
    }
    watch(k, HX_READ | (rc == HX_H3_BLOCKED ? HX_WRITE : 0));
    schedule(c, hx_h3_expiry(k->quic));
    int64_t t = now(c);
    for (struct hx_req *q = k->stream_list; q != NULL; q = q->s_next) {
        if (q->times[HX_T_FIRST_SENT] == 0) {
            q->times[HX_T_FIRST_SENT] = t;
        }
        if (q->times[HX_T_LAST_SENT] == 0 && (!q->upload || q->body_done)) {
            q->times[HX_T_LAST_SENT] = t;
        }
    }
}

static void process_dirty(hx_client *c) {
    while (c->dirty != NULL) {
        conn *k = c->dirty;
        c->dirty = k->dirty_next;
        k->dirty = false;
        conn *outer = c->receiving;
        c->receiving = k;
        if (k->state != CONN_CLOSED && k->h2) {
            h2_flush(k);
        }
        if (k->state != CONN_CLOSED && k->h3) {
            h3_flush(k);
        }
        if (k->state != CONN_CLOSED && multiplexed(k)) {
            h2_settle(k);
        }
        c->receiving = outer;
        if (k->state == CONN_CLOSED && !k->pinned && k->queued == 0 && !k->dirty && outer != k) {
            conn_free(k);
        }
    }
}

static int h2_init(hx_client *c) {
    if (nghttp2_session_callbacks_new(&c->h2_callbacks) != 0 || nghttp2_option_new(&c->h2_option) != 0) {
        return -1;
    }
    nghttp2_session_callbacks_set_on_header_callback(c->h2_callbacks, h2_on_header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(c->h2_callbacks, h2_on_frame);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(c->h2_callbacks, h2_on_data);
    nghttp2_session_callbacks_set_on_stream_close_callback(c->h2_callbacks, h2_on_close);
    nghttp2_option_set_no_auto_window_update(c->h2_option, 1);
    return 0;
}

static void raw_body(conn *k, struct hx_req *q, const char *data, size_t len) {
    q->received += len;
    queued *e = enqueue(k->client, q, HX_EV_BODY);
    if (e == NULL) {
        fail(k, HX_ERR_NOMEM, 0);
        return;
    }
    e->ev.data = data;
    e->ev.len = len;
    e->ev.status = k->h1.status;
    e->ev.version = k->h1.version;
    e->pin = k;
    k->queued++;
    k->pinned = true;
}

static void receive_raw(conn *k, struct hx_req *q, ssize_t n) {
    if (n == 0) {
        finish(q, HX_OK, 0, true);
    } else if (n < 0) {
        fail(k, HX_ERR_RECEIVE, errno);
    } else {
        raw_body(k, q, k->rbuf, (size_t)n);
    }
}

static void receive(conn *k) {
    if (k->h3) {
        h3_receive(k);
        return;
    }
    hx_client *c = k->client;
    ssize_t n;
    do {
        n = conn_read(k);
    } while (n < 0 && errno == EINTR);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    }
    if (k->h2) {
        h2_receive(k, n);
        return;
    }
    struct hx_req *q = k->req;
    if (q == NULL) {
        conn_close(k);
        return;
    }
    if (k->raw) {
        receive_raw(k, q, n);
        return;
    }
    if (n <= 0) {
        hx_h1_event events[8];
        int count = n == 0 ? hx_h1_finish(&k->h1, events, 8) : -1;
        if (count > 0 && events[count - 1].type == HX_H1_DONE) {
            finish(q, HX_OK, 0, true);
        } else {
            fail(k, q->send_errno != 0 ? HX_ERR_SEND : HX_ERR_RECEIVE,
                 q->send_errno != 0 ? q->send_errno
                 : n < 0            ? errno
                                    : 0);
        }
        return;
    }
    if (q->times[HX_T_FIRST_BYTE] == 0) {
        q->times[HX_T_FIRST_BYTE] = now(c);
    }
    k->decoded.len = 0;
    size_t at = 0;
    while (at < (size_t)n && !q->done) {
        hx_h1_event events[16];
        size_t used = 0;
        int count = hx_h1_feed(&k->h1, k->rbuf + at, (size_t)n - at, &used, events, 16);
        if (count < 0) {
            fail(k, HX_ERR_PROTOCOL, 0);
            return;
        }
        at += used;
        q->received += used;
        for (int i = 0; i < count; i++) {
            if (events[i].type == HX_H1_DONE && q->upgrade && k->h1.status == 101) {
                k->raw = true;
                if (at < (size_t)n) {
                    raw_body(k, q, k->rbuf + at, (size_t)n - at);
                }
                return;
            }
            if (events[i].type == HX_H1_DONE) {
                if (k->decoding) {
                    if (flush_decoded(k, q) == -1) {
                        return;
                    }
                    end_decoding(k);
                }
                bool reuse = k->h1.keep_alive && !q->close_after && at == (size_t)n;
                finish(q, HX_OK, 0, !reuse);
                break;
            }
            if (events[i].type == HX_H1_HEADERS && q->decode) {
                start_decoding(k, &events[i]);
            }
            if (events[i].type == HX_H1_BODY && k->decoding) {
                if (k->undecodable) {
                    fail(k, HX_ERR_UNKNOWN_ENCODING, 0);
                    return;
                }
                if (hx_decoder_feed(&k->decoder, events[i].data, events[i].len, &k->decoded) == -1) {
                    fail(k, HX_ERR_DECODE, 0);
                    return;
                }
                continue;
            }
            queued *e = enqueue(c, q, events[i].type == HX_H1_HEADERS ? HX_EV_HEADERS : HX_EV_BODY);
            if (e == NULL) {
                fail(k, HX_ERR_NOMEM, 0);
                return;
            }
            e->ev.data = events[i].data;
            e->ev.len = events[i].len;
            e->ev.status = k->h1.status;
            e->ev.version = k->h1.version;
            e->pin = k;
            k->queued++;
            k->pinned = true;
        }
        if (used == 0 && count == 0) {
            fail(k, HX_ERR_PROTOCOL, 0);
            return;
        }
    }
    if (k->decoding && !q->done) {
        flush_decoded(k, q);
    }
}

static void receive_ready(conn *k) {
    if (k->ssl == NULL) {
        receive(k);
        return;
    }
    hx_client *c = k->client;
    conn *outer = c->receiving;
    c->receiving = k;
    receive(k);
    while ((k->state == CONN_BUSY || (k->h2 && k->state == CONN_IDLE)) && reading(k) && SSL_pending(k->ssl) > 0) {
        if (k->pinned) {
            k->want_read = true;
            break;
        }
        receive(k);
    }
    c->receiving = outer;
    if (k->state == CONN_CLOSED && !k->pinned && k->queued == 0 && !k->dirty && outer != k) {
        conn_free(k);
    }
}

static void on_socket(hx_client *c, hx_socket s, int events);

void hx_client_on_socket(hx_client *c, hx_socket s, int events) {
    on_socket(c, s, events);
    process_dirty(c);
}

static void on_socket(hx_client *c, hx_socket s, int events) {
    if (c->dns != NULL && s == hx_dns_fd(c->dns)) {
        resolved(c);
        return;
    }
    conn *k = s >= 0 && (size_t)s < c->by_fd_len ? c->by_fd[s] : NULL;
    if (k == NULL) {
        return;
    }
    if (k->h3) {
        if ((events & HX_READ) && k->state != CONN_CLOSED) {
            if (k->pinned) {
                k->want_read = true;
            } else {
                h3_receive(k);
            }
        }
        if ((events & HX_WRITE) && k->state != CONN_CLOSED) {
            mark_dirty(k);
        }
        return;
    }
    if (k->state == CONN_CONNECTING) {
        if (events & (HX_WRITE | HX_READ)) {
            connected(k);
        }
        return;
    }
    if (k->state == CONN_HANDSHAKE) {
        handshake(k);
        return;
    }
    if (k->state == CONN_TUNNEL) {
        tunnel(k);
        return;
    }
    if ((events & HX_WRITE) && k->h2 && k->state != CONN_CLOSED) {
        h2_flush(k);
    } else if ((events & HX_WRITE) && k->state == CONN_BUSY && k->req != NULL) {
        send_some(k);
    }
    if ((events & HX_READ) && k->state != CONN_CLOSED) {
        if (k->pinned) {
            k->want_read = true;
        } else {
            receive_ready(k);
        }
    }
}

void hx_client_on_timer(hx_client *c) {
    int64_t t = now(c);
    c->timer = 0;
    int64_t next = 0;
    for (struct hx_req *q = c->reqs, *after; q != NULL; q = after) {
        after = q->next;
        if (q->deadline > 0 && q->deadline <= t) {
            finish(q, HX_ERR_TIMEOUT, 0, true);
        } else if (q->deadline > 0 && (next == 0 || q->deadline < next)) {
            next = q->deadline;
        }
    }
    for (conn *k = c->conns, *after; k != NULL; k = after) {
        after = k->next;
        if (k->quic != NULL && k->state != CONN_CLOSED) {
            int64_t expiry = hx_h3_expiry(k->quic);
            if (expiry > 0 && expiry <= t) {
                mark_dirty(k);
                int rc = hx_h3_expire(k->quic, now(c));
                if (rc != HX_H3_OK) {
                    h3_failed(k, rc, 0);
                    continue;
                }
                expiry = hx_h3_expiry(k->quic);
            }
            if (expiry > 0 && (next == 0 || expiry < next)) {
                next = expiry;
            }
        }
        int64_t deadline = k->state == CONN_IDLE && k->idle_timeout > 0 ? k->idle_since + k->idle_timeout
                           : k->state == CONN_CONNECTING || k->state == CONN_TUNNEL || k->state == CONN_HANDSHAKE
                               ? k->connect_deadline
                               : 0;
        if (deadline > 0 && deadline <= t) {
            if (k->state == CONN_CONNECTING || k->state == CONN_TUNNEL || k->state == CONN_HANDSHAKE) {
                fail(k, HX_ERR_TIMEOUT, 0);
            } else if (!k->pinned) {
                conn_close(k);
            }
        } else if (deadline > 0 && (next == 0 || deadline < next)) {
            next = deadline;
        }
    }
    schedule(c, next);
    process_dirty(c);
}

static bool release_later(hx_client *c, conn *k) {
    if (c->released_len == c->released_cap) {
        size_t cap = c->released_cap ? c->released_cap * 2 : 16;
        conn **grown = realloc(c->released, cap * sizeof(conn *));
        if (grown == NULL) {
            return false;
        }
        c->released = grown;
        c->released_cap = cap;
    }
    c->released[c->released_len++] = k;
    return true;
}

static void free_retired(hx_client *c) {
    while (c->retired != NULL) {
        struct hx_req *q = c->retired;
        c->retired = q->retired_next;
        req_free(q);
    }
}

int hx_client_events(hx_client *c, hx_event *out, int max) {
    free_retired(c);
    size_t released = c->released_len;
    for (size_t i = 0; i < released; i++) {
        conn *k = c->released[i];
        k->pinned = false;
        if (k->state == CONN_CLOSED && !k->dirty) {
            conn_free(k);
        } else if (k->want_read && reading(k)) {
            k->want_read = false;
            receive_ready(k);
        }
    }
    if (c->released_len > released) {
        memmove(c->released, c->released + released, (c->released_len - released) * sizeof(conn *));
    }
    c->released_len -= released;
    process_dirty(c);
    int count = 0;
    while (count < max && c->queue_pos < c->queue_len) {
        queued *e = &c->queue[c->queue_pos++];
        out[count++] = e->ev;
        if (e->pin != NULL && --e->pin->queued == 0) {
            release_later(c, e->pin);
        }
        if (e->done != NULL) {
            e->done->retired_next = c->retired;
            c->retired = e->done;
        }
    }
    if (c->queue_pos == c->queue_len) {
        c->queue_pos = c->queue_len = 0;
    }
    return count;
}

size_t hx_client_open_connections(const hx_client *c) { return c->open; }

bool hx_http3_available(void) { return hx_h3_available(); }

void hx_client_close_pool(hx_client *c, uint64_t pool) {
    for (conn *k = c->conns, *after; k != NULL; k = after) {
        after = k->next;
        if (k->pool != pool) {
            continue;
        }
        if (k->state == CONN_IDLE) {
            conn_close(k);
        } else {
            k->closing = true;
        }
    }
}

hx_client *hx_client_new(const hx_config *cfg, hx_io io) {
    hx_client *c = calloc(1, sizeof(hx_client));
    if (c == NULL) {
        return NULL;
    }
    c->cfg = *cfg;
    if (c->cfg.read_size == 0) {
        c->cfg.read_size = 16384;
    }
    c->io = io;
    if (h2_init(c) != 0) {
        nghttp2_session_callbacks_del(c->h2_callbacks);
        nghttp2_option_del(c->h2_option);
        free(c);
        return NULL;
    }
    return c;
}

void hx_client_free(hx_client *c) {
    if (c->dns != NULL) {
        c->io.watch(c->io.ctx, hx_dns_fd(c->dns), 0);
        hx_dns_free(c->dns);
    }
    while (c->lookups != NULL) {
        lookup *l = c->lookups;
        c->lookups = l->next;
        free(l);
    }
    while (c->cache != NULL) {
        cached *e = c->cache;
        c->cache = e->next;
        freeaddrinfo(e->addrs);
        free(e);
    }
    while (c->reqs != NULL) {
        struct hx_req *q = c->reqs;
        UNLINK(c->reqs, q);
        req_free(q);
    }
    free_retired(c);
    for (size_t i = 0; i < c->released_len; i++) {
        if (c->released[i]->state == CONN_CLOSED) {
            conn_free(c->released[i]);
        }
    }
    c->released_len = 0;
    while (c->conns != NULL) {
        conn *k = c->conns;
        k->pinned = false;
        k->queued = 0;
        k->req = NULL;
        k->waiters = NULL;
        conn_close(k);
    }
    for (size_t i = c->queue_pos; i < c->queue_len; i++) {
        if (c->queue[i].done != NULL) {
            req_free(c->queue[i].done);
        }
    }
    hx_tls_free(&c->tls);
    nghttp2_session_callbacks_del(c->h2_callbacks);
    nghttp2_option_del(c->h2_option);
    free(c->queue);
    free(c->released);
    free(c->by_fd);
    free(c);
}
