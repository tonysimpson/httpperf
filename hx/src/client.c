#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../include/hx.h"
#include "h1.h"

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
    int64_t connect_deadline;
    struct hx_req *req;
    struct conn *prev, *next;
} conn;

struct hx_req {
    hx_client *client;
    void *user;
    conn *conn;
    hx_buf request;
    bool head;
    bool fresh;
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
    conn *conns;
    conn **by_fd;
    size_t by_fd_len;
    struct hx_req *reqs;
    queued *queue;
    size_t queue_len, queue_cap, queue_pos;
    conn **released;
    size_t released_len, released_cap;
    int64_t timer;
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
    watch(k, 0);
    close(k->fd);
    k->state = CONN_CLOSED;
    c->open--;
    UNLINK(c->conns, k);
    c->by_fd[k->fd] = NULL;
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
    conn *k = q->conn;
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

static void fail(conn *k, hx_error error, int sys_errno) {
    if (k->req != NULL) {
        finish(k->req, error, sys_errno, true);
    } else {
        conn_close(k);
    }
}

static void send_some(conn *k) {
    struct hx_req *q = k->req;
    while (k->out_sent < k->out.len) {
        ssize_t n = send(k->fd, k->out.data + k->out_sent, k->out.len - k->out_sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                watch(k, HX_READ | HX_WRITE);
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            fail(k, HX_ERR_SEND, errno);
            return;
        }
        if (k->out_sent == 0) {
            q->times[HX_T_FIRST_SENT] = now(k->client);
        }
        k->out_sent += (size_t)n;
        q->sent += (uint64_t)n;
    }
    q->times[HX_T_LAST_SENT] = now(k->client);
    watch(k, HX_READ);
}

static void begin(conn *k, struct hx_req *q) {
    k->req = q;
    q->conn = k;
    k->state = CONN_BUSY;
    k->out.len = 0;
    k->out_sent = 0;
    hx_buf tmp = k->out;
    k->out = q->request;
    q->request = tmp;
    hx_h1_next(&k->h1, q->head);
    send_some(k);
}

static conn *conn_open(hx_client *c, struct hx_req *q, hx_error *error, int *sys_errno) {
    struct addrinfo hints = {0}, *ai = NULL;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    char port[8];
    snprintf(port, sizeof(port), "%u", q->port);
    const char *host = strcmp(q->host, "localhost") == 0 ? "127.0.0.1" : q->host;
    if (getaddrinfo(host, port, &hints, &ai) != 0) {
        *error = HX_ERR_DNS;
        return NULL;
    }
    q->times[HX_T_DNS] = now(c);
    conn *k = calloc(1, sizeof(conn));
    char *rbuf = malloc(c->cfg.read_size);
    int fd = socket(ai->ai_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (k == NULL || rbuf == NULL || fd < 0) {
        *error = fd < 0 ? HX_ERR_CONNECT : HX_ERR_NOMEM;
        *sys_errno = fd < 0 ? errno : 0;
        if (fd >= 0) {
            close(fd);
        }
        free(k);
        free(rbuf);
        freeaddrinfo(ai);
        return NULL;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    void *addr = ai->ai_family == AF_INET6 ? (void *)&((struct sockaddr_in6 *)ai->ai_addr)->sin6_addr
                                           : (void *)&((struct sockaddr_in *)ai->ai_addr)->sin_addr;
    inet_ntop(ai->ai_family, addr, k->ip, sizeof(k->ip));
    int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
    int connect_errno = errno;
    freeaddrinfo(ai);
    k->client = c;
    k->fd = fd;
    k->id = ++c->next_conn_id;
    k->rbuf = rbuf;
    k->rcap = c->cfg.read_size;
    memcpy(k->key, q->key, sizeof(k->key));
    hx_h1_init(&k->h1, false);
    if ((size_t)fd >= c->by_fd_len) {
        size_t len = (size_t)fd * 2 + 64;
        conn **grown = realloc(c->by_fd, len * sizeof(conn *));
        if (grown == NULL) {
            *error = HX_ERR_NOMEM;
            close(fd);
            hx_h1_free(&k->h1);
            free(k);
            free(rbuf);
            return NULL;
        }
        memset(grown + c->by_fd_len, 0, (len - c->by_fd_len) * sizeof(conn *));
        c->by_fd = grown;
        c->by_fd_len = len;
    }
    c->by_fd[fd] = k;
    LINK(c->conns, k);
    c->open++;
    if (rc < 0 && connect_errno != EINPROGRESS) {
        *error = HX_ERR_CONNECT;
        *sys_errno = connect_errno;
        watch(k, 0);
        conn_close(k);
        return NULL;
    }
    k->state = CONN_CONNECTING;
    k->req = q;
    q->conn = k;
    watch(k, HX_WRITE);
    return k;
}

static conn *find_idle(hx_client *c, const char *key) {
    for (conn *k = c->conns; k != NULL; k = k->next) {
        if (k->state == CONN_IDLE && !k->pinned && strcmp(k->key, key) == 0) {
            return k;
        }
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
    hx_h1_request h = {r->method,       r->target, r->authority, r->headers,
                       r->header_count, r->body,   r->body_len,  HX_H1_NO_UPLOAD};
    int rc = hx_h1_write_request(&q->request, &h);
    if (rc != 0) {
        finish(q, rc == HX_EINVAL ? HX_ERR_INVALID : HX_ERR_NOMEM, 0, false);
        return q;
    }
    conn *k = q->fresh ? NULL : find_idle(c, q->key);
    if (k != NULL) {
        q->reused = true;
        q->times[HX_T_DNS] = q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = q->times[HX_T_START];
        begin(k, q);
        return q;
    }
    hx_error error = HX_OK;
    int sys_errno = 0;
    k = conn_open(c, q, &error, &sys_errno);
    if (k == NULL) {
        finish(q, error, sys_errno, false);
        return q;
    }
    if (r->connect_timeout_ns > 0) {
        k->connect_deadline = q->times[HX_T_START] + r->connect_timeout_ns;
        schedule(c, k->connect_deadline);
    }
    return q;
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
        fail(k, HX_ERR_CONNECT, err);
        return;
    }
    struct hx_req *q = k->req;
    q->times[HX_T_CONNECT] = q->times[HX_T_TLS] = now(k->client);
    k->connect_deadline = 0;
    k->req = NULL;
    begin(k, q);
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
            fail(k, HX_ERR_RECEIVE, n < 0 ? errno : 0);
        }
        return;
    }
    if (q->times[HX_T_FIRST_BYTE] == 0) {
        q->times[HX_T_FIRST_BYTE] = now(c);
    }
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
                bool reuse = k->h1.keep_alive && !q->close_after && at == (size_t)n;
                finish(q, HX_OK, 0, !reuse);
                break;
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
}

void hx_client_on_socket(hx_client *c, hx_socket s, int events) {
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
        } else if (k->want_read) {
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
