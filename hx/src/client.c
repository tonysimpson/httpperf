#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../include/hx.h"
#include "decode.h"
#include "dns.h"
#include "h1.h"

#define MAX_ADDRS 8

typedef enum { CONN_CONNECTING, CONN_BUSY, CONN_IDLE, CONN_CLOSED } conn_state;

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
    struct hx_req *req;
    struct conn *prev, *next;
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
    int64_t connect_timeout;
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
    conn **by_fd;
    size_t by_fd_len;
    struct hx_req *reqs;
    queued *queue;
    size_t queue_len, queue_cap, queue_pos;
    conn **released;
    size_t released_len, released_cap;
    int64_t timer;
    hx_dns *dns;
    uint64_t next_lookup_id;
    lookup *lookups;
    cached *cache;
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
    hx_decoder_free(&k->decoder);
    hx_buf_free(&k->decoded);
    hx_h1_free(&k->h1);
    hx_buf_free(&k->out);
    free(k->rbuf);
    free(k);
}

static void conn_close(conn *k) {
    if (k->state == CONN_CLOSED) {
        return;
    }
    hx_client *c = k->client;
    if (k->state == CONN_IDLE) {
        c->idle--;
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
    if (!k->pinned && k->queued == 0) {
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
    conn *k = q->conn;
    if (!closed && k != NULL && q->upload && !q->body_done) {
        closed = true;
    }
    if (!closed && k != NULL && (k->closing || (c->cfg.max_idle > 0 && c->idle >= c->cfg.max_idle))) {
        closed = true;
    }
    queued *e = enqueue(c, q, HX_EV_DONE);
    if (e != NULL) {
        e->ev.error = error;
        e->ev.sys_errno = sys_errno;
        e->ev.status = k != NULL ? k->h1.status : 0;
        e->ev.version = k != NULL ? k->h1.version : 0;
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
    if (k != NULL) {
        k->req = NULL;
        q->conn = NULL;
        if (closed) {
            conn_close(k);
        } else {
            k->state = CONN_IDLE;
            c->idle++;
            k->idle_since = now(c);
            k->idle_timeout = q->idle_timeout;
            schedule(c, k->idle_timeout > 0 ? k->idle_since + k->idle_timeout : 0);
            watch(k, HX_READ);
        }
    }
    if (e == NULL) {
        hx_buf_free(&q->request);
        free(q);
    }
}

static void connect_new(hx_client *c, struct hx_req *q);

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
    if (k->req != NULL) {
        finish(k->req, error, sys_errno, true);
    } else {
        conn_close(k);
    }
}

static int reading(const conn *k) { return k->req != NULL && k->req->paused ? 0 : HX_READ; }

static void send_some(conn *k) {
    struct hx_req *q = k->req;
    if (q->send_errno != 0) {
        k->out.len = k->out_sent = 0;
        return;
    }
    while (k->out_sent < k->out.len) {
        ssize_t n = send(k->fd, k->out.data + k->out_sent, k->out.len - k->out_sent, MSG_NOSIGNAL);
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
            q->times[HX_T_FIRST_SENT] = now(k->client);
        }
        k->out_sent += (size_t)n;
        q->sent += (uint64_t)n;
    }
    if (q->upload) {
        k->out.len = k->out_sent = 0;
    }
    watch(k, reading(k));
    if (!q->upload || q->body_done) {
        q->times[HX_T_LAST_SENT] = now(k->client);
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

static void begin(conn *k, struct hx_req *q) {
    if (k->state == CONN_IDLE) {
        k->client->idle--;
    }
    k->req = q;
    q->conn = k;
    k->state = CONN_BUSY;
    k->out.len = 0;
    k->out_sent = 0;
    hx_buf tmp = k->out;
    k->out = q->request;
    q->request = tmp;
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
        int fd = socket(sa->sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            k->connect_errno = errno;
            continue;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
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
        watch(k, HX_WRITE);
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

static void open_for(hx_client *c, struct hx_req *q, const struct addrinfo *ai) {
    q->times[HX_T_DNS] = now(c);
    hx_error error = HX_OK;
    int sys_errno = 0;
    if (conn_open(c, q, ai, &error, &sys_errno) == NULL) {
        finish(q, error, sys_errno, false);
    }
}

static void resolve(hx_client *c, struct hx_req *q) {
    char key[264];
    snprintf(key, sizeof(key), "%s:%u", q->host, q->port);
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
        if (l == NULL || hx_dns_lookup(c->dns, q->host, q->port, c->next_lookup_id + 1) == -1) {
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

static bool still_open(const conn *k) {
    char b;
    return recv(k->fd, &b, 1, MSG_PEEK | MSG_DONTWAIT) < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

static conn *find_idle(hx_client *c, const char *key, int64_t max_age, bool check) {
    int64_t t = max_age > 0 ? now(c) : 0;
    for (conn *k = c->conns, *after; k != NULL; k = after) {
        after = k->next;
        if (k->state != CONN_IDLE || k->pinned || strcmp(k->key, key) != 0) {
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

hx_req *hx_request_start(hx_client *c, const hx_request *r, void *user) {
    struct hx_req *q = calloc(1, sizeof(struct hx_req));
    if (q == NULL) {
        return NULL;
    }
    q->client = c;
    q->user = user;
    q->head = strcmp(r->method, "HEAD") == 0;
    q->decode = r->decode;
    q->max_age = r->max_age_ns;
    q->pool = r->pool;
    q->upload = r->upload;
    q->chunked = r->upload && r->upload_size < 0;
    q->fresh = r->fresh_connection;
    q->close_after = r->close_after;
    q->port = r->port;
    q->times[HX_T_START] = now(c);
    q->deadline = r->timeout_ns > 0 ? q->times[HX_T_START] + r->timeout_ns : 0;
    snprintf(q->host, sizeof(q->host), "%s", r->host);
    q->idle_timeout = r->idle_timeout_ns > 0 ? r->idle_timeout_ns : c->cfg.idle_timeout_ns;
    snprintf(q->key, sizeof(q->key), "%llu/%s:%u", (unsigned long long)r->pool, r->host, r->port);
    LINK(c->reqs, q);
    schedule(c, q->deadline);
    hx_h1_request h = {
        r->method,       r->target,
        r->authority,    r->headers,
        r->header_count, r->upload ? NULL : r->body,
        r->body_len,     !r->upload ? HX_H1_NO_UPLOAD : r->upload_size < 0 ? HX_H1_CHUNKED : r->upload_size};
    int rc = hx_h1_write_request(&q->request, &h);
    if (rc != 0) {
        finish(q, rc == HX_EINVAL ? HX_ERR_INVALID : HX_ERR_NOMEM, 0, false);
        return q;
    }
    conn *k = q->fresh ? NULL : find_idle(c, q->key, q->max_age, q->upload);
    if (k != NULL) {
        q->reused = true;
        q->times[HX_T_DNS] = q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = q->times[HX_T_START];
        begin(k, q);
        return q;
    }
    q->connect_timeout = r->connect_timeout_ns;
    connect_new(c, q);
    return q;
}

static void connect_new(hx_client *c, struct hx_req *q) {
    struct addrinfo hints = {.ai_socktype = SOCK_STREAM, .ai_flags = AI_NUMERICHOST | AI_NUMERICSERV}, *ai = NULL;
    char port[8];
    snprintf(port, sizeof(port), "%u", q->port);
    if (getaddrinfo(q->host, port, &hints, &ai) == 0) {
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

int hx_request_send(hx_req *q, const char *data, size_t len) {
    if (q->done || !q->upload || q->body_done) {
        return -1;
    }
    if (len == 0) {
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
    if (q->chunked && !hx_buf_append(body_buffer(q), "0\r\n\r\n", 5)) {
        return -1;
    }
    q->body_done = true;
    body_written(q);
    return 0;
}

void hx_request_pause(hx_req *q, bool paused) {
    if (q->done || q->paused == paused) {
        return;
    }
    q->paused = paused;
    conn *k = q->conn;
    if (k != NULL && k->req == q && k->state == CONN_BUSY) {
        watch(k, reading(k) | (k->watching & HX_WRITE));
    }
}

void hx_request_cancel(hx_req *q) {
    if (q->done) {
        return;
    }
    finish(q, HX_ERR_CANCELLED, 0, true);
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
    q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = now(k->client);
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

static void receive(conn *k) {
    hx_client *c = k->client;
    ssize_t n;
    do {
        n = recv(k->fd, k->rbuf, k->rcap, 0);
    } while (n < 0 && errno == EINTR);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    }
    struct hx_req *q = k->req;
    if (q == NULL) {
        conn_close(k);
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

void hx_client_on_socket(hx_client *c, hx_socket s, int events) {
    if (c->dns != NULL && s == hx_dns_fd(c->dns)) {
        resolved(c);
        return;
    }
    conn *k = s >= 0 && (size_t)s < c->by_fd_len ? c->by_fd[s] : NULL;
    if (k == NULL) {
        return;
    }
    if (k->state == CONN_CONNECTING) {
        if (events & (HX_WRITE | HX_READ)) {
            connected(k);
        }
        return;
    }
    if ((events & HX_WRITE) && k->state == CONN_BUSY && k->req != NULL) {
        send_some(k);
    }
    if ((events & HX_READ) && k->state != CONN_CLOSED) {
        if (k->pinned) {
            k->want_read = true;
        } else {
            receive(k);
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
        int64_t deadline = k->state == CONN_IDLE && k->idle_timeout > 0 ? k->idle_since + k->idle_timeout
                           : k->state == CONN_CONNECTING                ? k->connect_deadline
                                                                        : 0;
        if (deadline > 0 && deadline <= t) {
            if (k->state == CONN_CONNECTING) {
                fail(k, HX_ERR_TIMEOUT, 0);
            } else if (!k->pinned) {
                conn_close(k);
            }
        } else if (deadline > 0 && (next == 0 || deadline < next)) {
            next = deadline;
        }
    }
    schedule(c, next);
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

int hx_client_events(hx_client *c, hx_event *out, int max) {
    size_t released = c->released_len;
    for (size_t i = 0; i < released; i++) {
        conn *k = c->released[i];
        k->pinned = false;
        if (k->state == CONN_CLOSED) {
            conn_free(k);
        } else if (k->want_read && reading(k)) {
            k->want_read = false;
            receive(k);
        }
    }
    if (c->released_len > released) {
        memmove(c->released, c->released + released, (c->released_len - released) * sizeof(conn *));
    }
    c->released_len -= released;
    int count = 0;
    while (count < max && c->queue_pos < c->queue_len) {
        queued *e = &c->queue[c->queue_pos++];
        out[count++] = e->ev;
        if (e->pin != NULL && --e->pin->queued == 0) {
            release_later(c, e->pin);
        }
        if (e->done != NULL) {
            hx_buf_free(&e->done->request);
            free(e->done);
        }
    }
    if (c->queue_pos == c->queue_len) {
        c->queue_pos = c->queue_len = 0;
    }
    return count;
}

size_t hx_client_open_connections(const hx_client *c) { return c->open; }

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
        hx_buf_free(&q->request);
        free(q);
    }
    while (c->conns != NULL) {
        conn *k = c->conns;
        k->pinned = false;
        k->queued = 0;
        k->req = NULL;
        conn_close(k);
    }
    for (size_t i = c->queue_pos; i < c->queue_len; i++) {
        if (c->queue[i].done != NULL) {
            hx_buf_free(&c->queue[i].done->request);
            free(c->queue[i].done);
        }
    }
    for (size_t i = 0; i < c->released_len; i++) {
        if (c->released[i]->state == CONN_CLOSED) {
            conn_free(c->released[i]);
        }
    }
    free(c->queue);
    free(c->released);
    free(c->by_fd);
    free(c);
}
