#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "../../hx/include/hx.h"
#include "../../hx/src/buf.h"
#include "../../hx/src/cookie.h"
#include "../ae/ae.h"
#include "../parse_headers.h"

#ifndef Py_BEGIN_CRITICAL_SECTION
#define Py_BEGIN_CRITICAL_SECTION(op) {
#define Py_END_CRITICAL_SECTION() }
#endif

enum {
    E_UNSUPPORTED_PROTOCOL = 1,
    E_URL_MALFORMAT = 3,
    E_COULDNT_RESOLVE_HOST = 6,
    E_COULDNT_CONNECT = 7,
    E_WEIRD_SERVER_REPLY = 8,
    E_OUT_OF_MEMORY = 27,
    E_OPERATION_TIMEDOUT = 28,
    E_ABORTED_BY_CALLBACK = 42,
    E_SEND_ERROR = 55,
    E_RECV_ERROR = 56,
    E_BAD_CONTENT_ENCODING = 61,
};

typedef struct {
    atomic_long requests, errors, cancelled, connections, bytes_sent, bytes_received, running, queued;
    atomic_long http_versions[4];
} Stats;

typedef struct EventLoop EventLoop;
typedef struct Session Session;
typedef struct Request Request;

typedef struct Slot {
    char key[300];
    long active;
    long waiting;
    Request *head, *tail;
    struct Slot *next;
    struct Slot *ready_prev, *ready_next;
    bool ready;
} Slot;

typedef struct Pool {
    atomic_int refs;
    atomic_long open;
    struct Pool *next;
} Pool;

static void pool_decref(Pool *p) {
    if (p != NULL && atomic_fetch_sub(&p->refs, 1) == 1) {
        free(p);
    }
}

typedef struct Limiter {
    atomic_int refs;
    pthread_mutex_t lock;
    long max_host, max_total;
    long connections;
    Slot *slots;
    Slot *ready_head, *ready_tail;
    bool dispatching;
    bool pending;
    struct Limiter *pending_next;
} Limiter;

#define STREAM_BUFFER (1024 * 1024)

typedef struct Stream {
    pthread_mutex_t lock;
    atomic_int refs;
    hx_buf buf, spare;
    bool paused, want_wake, complete;
    PyObject *waiter;
    Request *req;
    int64_t paused_since, paused_ns;
    hx_event done;
    long long size_download, size_upload;
    int error_code;
    char error[512];
} Stream;

typedef struct Upload {
    pthread_mutex_t lock;
    atomic_int refs;
    char *ring;
    size_t head, len;
    bool eof, ended, done, hungry, want_wake;
    PyObject *waiter;
    Request *req;
    EventLoop *loop;
    long long total;
    int64_t paused_since, paused_ns;
} Upload;

enum { NOTE_HEADERS, NOTE_STREAM_WAKE, NOTE_UPLOAD_WAKE, NOTE_STREAM_RESUME, NOTE_UPLOAD_RESUME };

typedef struct Note {
    int kind;
    void *ptr;
    struct Note *next;
} Note;

struct Request {
    atomic_int refs;
    atomic_bool cancel_sent;
    struct Request *next;
    struct Request *cancel_next;
    struct Request *live_prev, *live_next;
    EventLoop *loop;
    PyObject *future;
    Session *session;
    Stats *session_stats;
    char *url;
    char *host;
    char *authority;
    char *target;
    char *method;
    uint16_t port;
    char **headers;
    size_t header_count;
    char *body;
    size_t body_len;
    bool discard_body;
    bool fresh;
    bool close_after;
    uint64_t pool;
    int64_t idle_timeout_ns;
    int64_t timeout_ns;
    int64_t connect_timeout_ns;
    hx_req *hx;
    bool started;
    bool finished;
    bool cancel;
    hx_buf header;
    hx_buf received;
    uint64_t body_bytes;
    hx_event done;
    int64_t max_age_ns;
    Limiter *limiter;
    Slot *slot;
    Request *wait_prev, *wait_next;
    bool waiting;
    int64_t queued_at;
    int64_t queue_ns;
    Stream *stream;
    Upload *upload;
    int64_t upload_size;
    bool delivered;
    int head_status, head_version;
    bool send_cookies;
    char *path;
};

struct EventLoop {
    PyObject_HEAD aeEventLoop *ae;
    hx_client *client;
    long long timer_id;
    int wake[2];
    int out[2];
    int stop_pipe[2];
    bool stop;
    bool running;
    atomic_bool closed;
    Request *abandoned;
    pthread_mutex_t lock;
    Request *incoming, *incoming_tail;
    Pool *closing;
    Note *resumes, *resumes_tail;
    Note *notes, *notes_tail;
    Request *cancels, *cancels_tail;
    Request *completed, *completed_tail;
    Request *live;
    Stats stats;
    atomic_long open;
    size_t buffer_size;
    Limiter *limiter;
    Limiter *pending;
};

struct Session {
    PyObject_HEAD EventLoop *loop;
    bool closed;
    bool own_pool;
    bool fresh;
    long idle_timeout;
    long max_connection_age;
    Limiter *limiter;
    Pool *pool;
    hx_jar *jar;
    pthread_mutex_t jar_lock;
    Stats stats;
};

typedef struct {
    PyObject_HEAD int status;
    int version;
    hx_buf header;
    hx_buf body;
    PyObject *body_bytes;
    bool body_discarded;
    double times[HX_T_COUNT];
    double queue_time;
    Stream *stream;
    Upload *upload;
    EventLoop *loop;
    bool synced;
    long long size_upload;
    long long size_download;
    char primary_ip[46];
    char *url;
} Response;

static PyTypeObject EventLoopType, SessionType, ResponseType;

static int64_t clock_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static int make_pipe(int fds[2]) {
    if (pipe(fds) == -1) {
        return -1;
    }
    for (int i = 0; i < 2; i++) {
        if (fcntl(fds[i], F_SETFL, fcntl(fds[i], F_GETFL) | O_NONBLOCK) == -1 ||
            fcntl(fds[i], F_SETFD, FD_CLOEXEC) == -1) {
            close(fds[0]);
            close(fds[1]);
            fds[0] = fds[1] = -1;
            return -1;
        }
    }
    return 0;
}

static void poke(int fd) {
    char byte = 0;
    while (write(fd, &byte, 1) == -1 && errno == EINTR) {
    }
}

static void drain_pipe(int fd) {
    char buf[256];
    while (read(fd, buf, sizeof(buf)) > 0) {
    }
}

static void free_strings(char **strings, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(strings[i]);
    }
    free(strings);
}

static void limiter_decref(struct Limiter *l);
static void stream_decref(Stream *s);
static void upload_decref(Upload *u);

static void request_free(Request *r) {
    free(r->url);
    free(r->host);
    free(r->authority);
    free(r->target);
    free(r->path);
    free(r->method);
    free_strings(r->headers, r->header_count);
    free(r->body);
    hx_buf_free(&r->header);
    hx_buf_free(&r->received);
    stream_decref(r->stream);
    upload_decref(r->upload);
    limiter_decref(r->limiter);
    free(r);
}

static void request_decref(Request *r) {
    if (atomic_fetch_sub(&r->refs, 1) == 1) {
        request_free(r);
    }
}

static void push(Request **head, Request **tail, Request *r) {
    r->next = NULL;
    if (*tail != NULL) {
        (*tail)->next = r;
    } else {
        *head = r;
    }
    *tail = r;
}

static void note_push(Note **head, Note **tail, Note *n) {
    n->next = NULL;
    if (*tail != NULL) {
        (*tail)->next = n;
    } else {
        *head = n;
    }
    *tail = n;
}

static bool inbox_empty(EventLoop *loop) {
    return loop->incoming == NULL && loop->cancels == NULL && loop->closing == NULL && loop->resumes == NULL;
}

static Stream *stream_new(void) {
    Stream *s = calloc(1, sizeof(Stream));
    if (s != NULL) {
        pthread_mutex_init(&s->lock, NULL);
        atomic_init(&s->refs, 1);
    }
    return s;
}

static void stream_decref(Stream *s) {
    if (s == NULL || atomic_fetch_sub(&s->refs, 1) != 1) {
        return;
    }
    hx_buf_free(&s->buf);
    hx_buf_free(&s->spare);
    pthread_mutex_destroy(&s->lock);
    free(s);
}

static Upload *upload_new(void) {
    Upload *u = calloc(1, sizeof(Upload));
    if (u == NULL || (u->ring = malloc(STREAM_BUFFER)) == NULL) {
        free(u);
        return NULL;
    }
    pthread_mutex_init(&u->lock, NULL);
    atomic_init(&u->refs, 1);
    return u;
}

static void upload_decref(Upload *u) {
    if (u == NULL || atomic_fetch_sub(&u->refs, 1) != 1) {
        return;
    }
    free(u->ring);
    pthread_mutex_destroy(&u->lock);
    free(u);
}

static void send_resume(EventLoop *loop, int kind, void *ptr) {
    Note *n = malloc(sizeof(Note));
    if (n == NULL) {
        return;
    }
    n->kind = kind;
    n->ptr = ptr;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = inbox_empty(loop);
    note_push(&loop->resumes, &loop->resumes_tail, n);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->wake[1]);
    }
}

static void watch(void *ctx, hx_socket s, int events);
static void set_timer(void *ctx, int64_t deadline);
static int64_t io_now(void *ctx) { return clock_ns(); }

static void stats_add(EventLoop *loop, Request *r, atomic_long *(*field)(Stats *), long n) {
    atomic_fetch_add_explicit(field(&loop->stats), n, memory_order_relaxed);
    atomic_fetch_add_explicit(field(r->session_stats), n, memory_order_relaxed);
}

#define STAT(name) \
    static atomic_long *stat_##name(Stats *s) { return &s->name; }
STAT(requests)
STAT(errors)
STAT(cancelled)
STAT(connections)
STAT(bytes_sent)
STAT(bytes_received)
STAT(running)
STAT(queued)

static void slot_release(EventLoop *loop, Request *r);

static void notify(EventLoop *loop, int kind, void *ptr) {
    Note *n = malloc(sizeof(Note));
    if (n == NULL) {
        return;
    }
    n->kind = kind;
    n->ptr = ptr;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = loop->completed == NULL && loop->notes == NULL;
    note_push(&loop->notes, &loop->notes_tail, n);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->out[1]);
    }
}

static PyObject *take_waiter(pthread_mutex_t *lock, PyObject **waiter) {
    pthread_mutex_lock(lock);
    PyObject *w = *waiter;
    *waiter = NULL;
    pthread_mutex_unlock(lock);
    return w;
}

static void complete(EventLoop *loop, Request *r) {
    if (r->live_prev != NULL) {
        r->live_prev->live_next = r->live_next;
    } else if (loop->live == r) {
        loop->live = r->live_next;
    }
    if (r->live_next != NULL) {
        r->live_next->live_prev = r->live_prev;
    }
    r->live_prev = r->live_next = NULL;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = loop->completed == NULL && loop->notes == NULL;
    push(&loop->completed, &loop->completed_tail, r);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->out[1]);
    }
}

static bool dispatch_pending(EventLoop *loop);
static int error_code(hx_error e);
static void error_text(const Request *r, char *message, size_t size);

static void stream_append(EventLoop *loop, Request *r, const char *data, size_t len) {
    Stream *s = r->stream;
    pthread_mutex_lock(&s->lock);
    bool ok = hx_buf_append(&s->buf, data, len);
    bool pause = ok && s->buf.len >= STREAM_BUFFER && !s->paused;
    if (pause) {
        s->paused = true;
        s->paused_since = clock_ns();
    }
    bool wake = s->want_wake;
    s->want_wake = false;
    pthread_mutex_unlock(&s->lock);
    if (!ok) {
        hx_request_cancel(r->hx);
        return;
    }
    if (pause) {
        hx_request_pause(r->hx, true);
    }
    if (wake) {
        atomic_fetch_add(&s->refs, 1);
        notify(loop, NOTE_STREAM_WAKE, s);
    }
}

static void stream_finish(EventLoop *loop, Request *r) {
    Stream *s = r->stream;
    char message[512] = "";
    if (r->done.error != HX_OK) {
        error_text(r, message, sizeof(message));
    }
    pthread_mutex_lock(&s->lock);
    s->done = r->done;
    s->size_download = (long long)r->body_bytes;
    s->size_upload = r->upload != NULL ? r->upload->total : (long long)r->body_len;
    s->error_code = r->done.error != HX_OK ? error_code(r->done.error) : 0;
    memcpy(s->error, message, sizeof(message));
    if (s->paused) {
        s->paused_ns += clock_ns() - s->paused_since;
        s->paused = false;
    }
    s->complete = true;
    Request *held = s->req;
    s->req = NULL;
    s->want_wake = false;
    bool wake = s->waiter != NULL;
    pthread_mutex_unlock(&s->lock);
    if (wake) {
        atomic_fetch_add(&s->refs, 1);
        notify(loop, NOTE_STREAM_WAKE, s);
    }
    request_decref(held);
}

static void upload_pump(EventLoop *loop, Request *r, bool asked) {
    Upload *u = r->upload;
    if (r->hx == NULL || r->finished) {
        return;
    }
    pthread_mutex_lock(&u->lock);
    if (!asked && !u->hungry) {
        pthread_mutex_unlock(&u->lock);
        return;
    }
    if (u->hungry) {
        u->paused_ns += clock_ns() - u->paused_since;
        u->hungry = false;
    }
    bool sent = u->len > 0;
    while (u->len > 0) {
        size_t n = STREAM_BUFFER - u->head < u->len ? STREAM_BUFFER - u->head : u->len;
        hx_request_send(r->hx, u->ring + u->head, n);
        u->head = (u->head + n) % STREAM_BUFFER;
        u->len -= n;
        u->total += (long long)n;
    }
    bool end = u->eof && !u->ended;
    if (end) {
        u->ended = true;
    } else if (!sent && !u->eof) {
        u->hungry = true;
        u->paused_since = clock_ns();
    }
    bool wake = u->want_wake;
    u->want_wake = false;
    pthread_mutex_unlock(&u->lock);
    if (end) {
        hx_request_end_body(r->hx);
    }
    if (wake) {
        atomic_fetch_add(&u->refs, 1);
        notify(loop, NOTE_UPLOAD_WAKE, u);
    }
}

static void upload_done(EventLoop *loop, Request *r) {
    Upload *u = r->upload;
    pthread_mutex_lock(&u->lock);
    u->done = true;
    u->req = NULL;
    if (u->hungry) {
        u->paused_ns += clock_ns() - u->paused_since;
        u->hungry = false;
    }
    bool wake = u->waiter != NULL;
    pthread_mutex_unlock(&u->lock);
    if (wake) {
        atomic_fetch_add(&u->refs, 1);
        notify(loop, NOTE_UPLOAD_WAKE, u);
    }
}

static void store_cookies(Request *r, const char *block, size_t len) {
    Session *session = r->session;
    const char *end = block + len;
    bool locked = false;
    int64_t now = (int64_t)time(NULL);
    for (const char *line = memchr(block, '\n', len); line != NULL && ++line < end;) {
        const char *eol = memchr(line, '\r', (size_t)(end - line));
        if (eol == NULL || eol == line) {
            break;
        }
        if (eol - line > 11 && strncasecmp(line, "set-cookie:", 11) == 0) {
            if (!locked) {
                pthread_mutex_lock(&session->jar_lock);
                locked = true;
            }
            const char *v = line + 11;
            hx_jar_set_cookie(session->jar, v, (size_t)(eol - v), r->host, r->path, false, now);
        }
        line = memchr(eol, '\n', (size_t)(end - eol));
    }
    if (locked) {
        pthread_mutex_unlock(&session->jar_lock);
    }
}

static void handle_events(EventLoop *loop) {
    hx_event events[64];
    int count;
again:
    while ((count = hx_client_events(loop->client, events, 64)) > 0) {
        for (int i = 0; i < count; i++) {
            hx_event *e = &events[i];
            Request *r = e->user;
            if (r->finished) {
                continue;
            }
            if (e->type == HX_EV_HEADERS) {
                r->header.len = 0;
                hx_buf_append(&r->header, e->data, e->len);
                if (r->session != NULL) {
                    store_cookies(r, e->data, e->len);
                }
                if (r->stream != NULL) {
                    r->head_status = e->status;
                    r->head_version = e->version;
                    r->delivered = true;
                    atomic_fetch_add(&r->refs, 1);
                    notify(loop, NOTE_HEADERS, r);
                }
            } else if (e->type == HX_EV_BODY) {
                r->body_bytes += e->len;
                if (r->stream != NULL) {
                    stream_append(loop, r, e->data, e->len);
                } else if (!r->discard_body) {
                    hx_buf_append(&r->received, e->data, e->len);
                }
            } else if (e->type == HX_EV_SEND_READY) {
                if (r->upload != NULL) {
                    upload_pump(loop, r, true);
                }
            } else {
                r->done = *e;
                r->done.data = NULL;
                r->finished = true;
                stats_add(loop, r, stat_running, -1);
                stats_add(loop, r, stat_bytes_sent, (long)e->bytes_sent);
                stats_add(loop, r, stat_bytes_received, (long)e->bytes_received);
                if (!e->reused && e->connection_id != 0) {
                    stats_add(loop, r, stat_connections, 1);
                }
                if (e->error == HX_ERR_CANCELLED) {
                    stats_add(loop, r, stat_cancelled, 1);
                } else if (e->error != HX_OK) {
                    stats_add(loop, r, stat_errors, 1);
                } else if (e->version == 10 || e->version == 11) {
                    atomic_fetch_add_explicit(&loop->stats.http_versions[e->version - 10], 1, memory_order_relaxed);
                    atomic_fetch_add_explicit(&r->session_stats->http_versions[e->version - 10], 1,
                                              memory_order_relaxed);
                }
                slot_release(loop, r);
                if (r->upload != NULL) {
                    upload_done(loop, r);
                }
                if (r->delivered) {
                    stream_finish(loop, r);
                }
                complete(loop, r);
            }
        }
    }
    if (dispatch_pending(loop)) {
        goto again;
    }
    atomic_store_explicit(&loop->open, (long)hx_client_open_connections(loop->client), memory_order_relaxed);
}

static void on_fd(aeEventLoop *ae, int fd, void *data, int mask) {
    EventLoop *loop = data;
    hx_client_on_socket(loop->client, fd, (mask & AE_READABLE ? HX_READ : 0) | (mask & AE_WRITABLE ? HX_WRITE : 0));
    handle_events(loop);
}

static void on_connection(void *ctx, uint64_t pool, int delta) {
    Pool *p = (Pool *)(uintptr_t)pool;
    if (delta > 0) {
        atomic_fetch_add(&p->refs, 1);
        atomic_fetch_add_explicit(&p->open, 1, memory_order_relaxed);
    } else {
        atomic_fetch_sub_explicit(&p->open, 1, memory_order_relaxed);
        pool_decref(p);
    }
}

static void watch(void *ctx, hx_socket s, int events) {
    EventLoop *loop = ctx;
    int want = (events & HX_READ ? AE_READABLE : 0) | (events & HX_WRITE ? AE_WRITABLE : 0);
    int have = aeGetFileEvents(loop->ae, s) & (AE_READABLE | AE_WRITABLE);
    if (have & ~want) {
        aeDeleteFileEvent(loop->ae, s, have & ~want);
    }
    if (want & ~have) {
        aeCreateFileEvent(loop->ae, s, want & ~have, on_fd, loop);
    }
}

static int on_timer(aeEventLoop *ae, long long id, void *data) {
    EventLoop *loop = data;
    loop->timer_id = -1;
    hx_client_on_timer(loop->client);
    handle_events(loop);
    return AE_NOMORE;
}

static void set_timer(void *ctx, int64_t deadline) {
    EventLoop *loop = ctx;
    if (loop->timer_id != -1) {
        aeDeleteTimeEvent(loop->ae, loop->timer_id);
        loop->timer_id = -1;
    }
    int64_t ms = (deadline - clock_ns() + 999999) / 1000000;
    loop->timer_id = aeCreateTimeEvent(loop->ae, ms < 0 ? 0 : ms, on_timer, loop, NULL);
}

static Limiter *limiter_new(long max_host, long max_total) {
    Limiter *l = calloc(1, sizeof(Limiter));
    if (l != NULL) {
        atomic_init(&l->refs, 1);
        pthread_mutex_init(&l->lock, NULL);
        l->max_host = max_host;
        l->max_total = max_total;
    }
    return l;
}

static void limiter_decref(Limiter *l) {
    if (l == NULL || atomic_fetch_sub(&l->refs, 1) != 1) {
        return;
    }
    while (l->slots != NULL) {
        Slot *h = l->slots;
        l->slots = h->next;
        free(h);
    }
    pthread_mutex_destroy(&l->lock);
    free(l);
}

static Slot *slot_for(Limiter *l, const Request *r) {
    char key[300];
    snprintf(key, sizeof(key), "http://%s:%u", r->host, r->port);
    for (Slot *h = l->slots; h != NULL; h = h->next) {
        if (strcmp(h->key, key) == 0) {
            return h;
        }
    }
    Slot *h = calloc(1, sizeof(Slot));
    if (h != NULL) {
        memcpy(h->key, key, sizeof(key));
        pthread_mutex_lock(&l->lock);
        h->next = l->slots;
        l->slots = h;
        pthread_mutex_unlock(&l->lock);
    }
    return h;
}

static bool slot_free(const Limiter *l, const Slot *h) {
    if (l->max_host && h->active >= l->max_host) {
        return false;
    }
    return !l->max_total || l->connections < l->max_total;
}

static void slot_use(Limiter *l, Slot *h, long delta) {
    pthread_mutex_lock(&l->lock);
    h->active += delta;
    l->connections += delta;
    pthread_mutex_unlock(&l->lock);
}

static void ready_remove(Limiter *l, Slot *h) {
    if (h->ready_prev != NULL) {
        h->ready_prev->ready_next = h->ready_next;
    } else {
        l->ready_head = h->ready_next;
    }
    if (h->ready_next != NULL) {
        h->ready_next->ready_prev = h->ready_prev;
    } else {
        l->ready_tail = h->ready_prev;
    }
    h->ready_prev = h->ready_next = NULL;
    h->ready = false;
}

static void ready_append(Limiter *l, Slot *h) {
    h->ready_prev = l->ready_tail;
    if (l->ready_tail != NULL) {
        l->ready_tail->ready_next = h;
    } else {
        l->ready_head = h;
    }
    l->ready_tail = h;
    h->ready = true;
}

static void wait_push(EventLoop *loop, Request *r) {
    Limiter *l = r->limiter;
    Slot *h = r->slot;
    r->wait_prev = h->tail;
    r->wait_next = NULL;
    if (h->tail != NULL) {
        h->tail->wait_next = r;
    } else {
        h->head = r;
    }
    h->tail = r;
    pthread_mutex_lock(&l->lock);
    h->waiting++;
    pthread_mutex_unlock(&l->lock);
    r->waiting = true;
    r->queued_at = clock_ns();
    stats_add(loop, r, stat_queued, 1);
    if (!h->ready) {
        ready_append(l, h);
    }
}

static void wait_remove(EventLoop *loop, Request *r) {
    Limiter *l = r->limiter;
    Slot *h = r->slot;
    if (r->wait_prev != NULL) {
        r->wait_prev->wait_next = r->wait_next;
    } else {
        h->head = r->wait_next;
    }
    if (r->wait_next != NULL) {
        r->wait_next->wait_prev = r->wait_prev;
    } else {
        h->tail = r->wait_prev;
    }
    r->wait_prev = r->wait_next = NULL;
    pthread_mutex_lock(&l->lock);
    h->waiting--;
    pthread_mutex_unlock(&l->lock);
    r->waiting = false;
    stats_add(loop, r, stat_queued, -1);
    if (h->head == NULL) {
        ready_remove(l, h);
    }
}

static void begin_request(EventLoop *loop, Request *r);

static void dispatch(EventLoop *loop, Limiter *l) {
    if (l->dispatching) {
        return;
    }
    l->dispatching = true;
    Slot *h = l->ready_head;
    while (h != NULL) {
        if (!slot_free(l, h)) {
            h = h->ready_next;
            continue;
        }
        Request *r = h->head;
        wait_remove(loop, r);
        if (h->ready) {
            ready_remove(l, h);
            ready_append(l, h);
        }
        r->queue_ns = clock_ns() - r->queued_at;
        slot_use(l, h, 1);
        begin_request(loop, r);
        h = l->ready_head;
    }
    l->dispatching = false;
}

static void slot_release(EventLoop *loop, Request *r) {
    if (r->slot == NULL) {
        return;
    }
    Limiter *l = r->limiter;
    slot_use(l, r->slot, -1);
    r->slot = NULL;
    if (!l->pending && l->ready_head != NULL) {
        atomic_fetch_add(&l->refs, 1);
        l->pending = true;
        l->pending_next = loop->pending;
        loop->pending = l;
    }
}

static bool dispatch_pending(EventLoop *loop) {
    Limiter *l = loop->pending;
    loop->pending = NULL;
    bool any = l != NULL;
    while (l != NULL) {
        Limiter *next = l->pending_next;
        l->pending = false;
        dispatch(loop, l);
        limiter_decref(l);
        l = next;
    }
    return any;
}

static PyObject *limiter_state(Limiter *l) {
    if (l == NULL) {
        Py_RETURN_NONE;
    }
    PyObject *hosts = PyDict_New();
    if (hosts == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&l->lock);
    long connections = l->connections;
    for (Slot *h = l->slots; h != NULL; h = h->next) {
        PyObject *state =
            Py_BuildValue("{s:l,s:l,s:O}", "active", h->active, "waiting", h->waiting, "multiplexed", Py_False);
        if (state == NULL || PyDict_SetItemString(hosts, h->key, state) == -1) {
            pthread_mutex_unlock(&l->lock);
            Py_XDECREF(state);
            Py_DECREF(hosts);
            return NULL;
        }
        Py_DECREF(state);
    }
    pthread_mutex_unlock(&l->lock);
    return Py_BuildValue("{s:l,s:N}", "connections", connections, "hosts", hosts);
}

static void start(EventLoop *loop, Request *r) {
    r->live_next = loop->live;
    if (loop->live != NULL) {
        loop->live->live_prev = r;
    }
    loop->live = r;
    r->started = true;
    stats_add(loop, r, stat_requests, 1);
    if (r->limiter != NULL) {
        r->slot = slot_for(r->limiter, r);
        if (r->slot != NULL && !slot_free(r->limiter, r->slot)) {
            wait_push(loop, r);
            return;
        }
        if (r->slot != NULL) {
            slot_use(r->limiter, r->slot, 1);
        }
    }
    begin_request(loop, r);
}

static void add_cookie_header(Request *r) {
    Session *session = r->session;
    if (!r->send_cookies || session == NULL) {
        return;
    }
    hx_buf line = {0};
    pthread_mutex_lock(&session->jar_lock);
    bool any = hx_jar_count(session->jar) > 0 && hx_buf_append_str(&line, "Cookie: ") &&
               hx_jar_header(session->jar, r->host, r->path, false, (int64_t)time(NULL), &line);
    pthread_mutex_unlock(&session->jar_lock);
    char **grown =
        any && hx_buf_append(&line, "", 1) ? realloc(r->headers, (r->header_count + 1) * sizeof(char *)) : NULL;
    if (grown == NULL) {
        hx_buf_free(&line);
        return;
    }
    r->headers = grown;
    r->headers[r->header_count++] = line.data;
}

static void begin_request(EventLoop *loop, Request *r) {
    add_cookie_header(r);
    hx_request q = {
        .method = r->method,
        .host = r->host,
        .port = r->port,
        .authority = r->authority,
        .target = r->target,
        .headers = (const char *const *)r->headers,
        .header_count = r->header_count,
        .body = r->body,
        .body_len = r->body_len,
        .fresh_connection = r->fresh,
        .close_after = r->close_after,
        .pool = r->pool,
        .idle_timeout_ns = r->idle_timeout_ns,
        .timeout_ns = r->timeout_ns,
        .connect_timeout_ns = r->connect_timeout_ns,
        .decode = true,
        .max_age_ns = r->max_age_ns,
        .upload = r->upload != NULL,
        .upload_size = r->upload_size,
    };
    stats_add(loop, r, stat_running, 1);
    r->hx = hx_request_start(loop->client, &q, r);
    if (r->hx == NULL) {
        r->done.error = HX_ERR_NOMEM;
        r->finished = true;
        stats_add(loop, r, stat_running, -1);
        stats_add(loop, r, stat_errors, 1);
        slot_release(loop, r);
        complete(loop, r);
    }
}

static void on_wake(aeEventLoop *ae, int fd, void *data, int mask) {
    EventLoop *loop = data;
    drain_pipe(fd);
    pthread_mutex_lock(&loop->lock);
    Request *incoming = loop->incoming, *cancels = loop->cancels;
    Pool *closing = loop->closing;
    Note *resumes = loop->resumes;
    loop->incoming = loop->incoming_tail = loop->cancels = loop->cancels_tail = NULL;
    loop->closing = NULL;
    loop->resumes = loop->resumes_tail = NULL;
    pthread_mutex_unlock(&loop->lock);
    while (resumes != NULL) {
        Note *n = resumes;
        resumes = n->next;
        if (n->kind == NOTE_STREAM_RESUME) {
            Stream *st = n->ptr;
            pthread_mutex_lock(&st->lock);
            Request *r = st->req;
            pthread_mutex_unlock(&st->lock);
            if (r != NULL && !r->finished && r->hx != NULL) {
                hx_request_pause(r->hx, false);
            }
            stream_decref(st);
        } else {
            Upload *u = n->ptr;
            pthread_mutex_lock(&u->lock);
            Request *r = u->req;
            pthread_mutex_unlock(&u->lock);
            if (r != NULL) {
                upload_pump(loop, r, false);
            }
            upload_decref(u);
        }
        free(n);
    }
    while (closing != NULL) {
        Pool *p = closing;
        closing = p->next;
        hx_client_close_pool(loop->client, (uint64_t)(uintptr_t)p);
        pool_decref(p);
    }
    while (incoming != NULL) {
        Request *r = incoming;
        incoming = r->next;
        start(loop, r);
    }
    handle_events(loop);
    while (cancels != NULL) {
        Request *r = cancels;
        cancels = r->cancel_next;
        if (r->waiting) {
            wait_remove(loop, r);
            r->slot = NULL;
            r->done.error = HX_ERR_CANCELLED;
            r->finished = true;
            stats_add(loop, r, stat_cancelled, 1);
            complete(loop, r);
        } else if (r->started && !r->finished && r->hx != NULL) {
            hx_request_cancel(r->hx);
        }
        request_decref(r);
    }
    handle_events(loop);
}

static void on_stop(aeEventLoop *ae, int fd, void *data, int mask) {
    EventLoop *loop = data;
    drain_pipe(fd);
    loop->stop = true;
}

static PyObject *stats_dict(Stats *s, PyObject *open) {
#define LOAD(field) atomic_load_explicit(&s->field, memory_order_relaxed)
    return Py_BuildValue("{s:l,s:l,s:l,s:l,s:N,s:i,s:{s:l,s:l,s:i,s:i},s:l,s:l,s:l,s:i}", "requests", LOAD(requests),
                         "errors", LOAD(errors), "cancelled", LOAD(cancelled), "connections", LOAD(connections),
                         "connections_open", open, "tls_handshakes", 0, "http_versions", "HTTP/1.0",
                         LOAD(http_versions[0]), "HTTP/1.1", LOAD(http_versions[1]), "HTTP/2", 0, "HTTP/3", 0,
                         "bytes_sent", LOAD(bytes_sent), "bytes_received", LOAD(bytes_received), "running",
                         LOAD(running), "queued", LOAD(queued));
#undef LOAD
}

static PyObject *EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    int max_host_connections, max_total_connections, max_idle_connections, buffer_size, host_stats;
    if (!PyArg_ParseTuple(args, "iiiip", &max_host_connections, &max_total_connections, &max_idle_connections,
                          &buffer_size, &host_stats)) {
        return NULL;
    }
    if (buffer_size < 1024 || buffer_size > 10 * 1024 * 1024) {
        PyErr_Format(PyExc_ValueError, "buffer_size must be from 1024 to %d", 10 * 1024 * 1024);
        return NULL;
    }
    if (max_host_connections < 0 || max_total_connections < 0) {
        PyErr_SetString(PyExc_ValueError, "connection limits must not be negative");
        return NULL;
    }
    if (max_idle_connections < 1) {
        PyErr_SetString(PyExc_ValueError, "max_idle_connections must be at least 1");
        return NULL;
    }
    EventLoop *self = (EventLoop *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    self->wake[0] = self->wake[1] = self->out[0] = self->out[1] = self->stop_pipe[0] = self->stop_pipe[1] = -1;
    self->timer_id = -1;
    self->buffer_size = buffer_size > 0 ? (size_t)buffer_size : 16384;
    pthread_mutex_init(&self->lock, NULL);
    hx_config cfg = {.max_idle = (size_t)max_idle_connections, .read_size = self->buffer_size};
    if (max_host_connections || max_total_connections) {
        self->limiter = limiter_new(max_host_connections, max_total_connections);
        if (self->limiter == NULL) {
            Py_DECREF(self);
            return PyErr_NoMemory();
        }
    }
    self->ae = aeCreateEventLoop(200);
    self->client = hx_client_new(&cfg, (hx_io){watch, set_timer, io_now, self, on_connection});
    if (self->ae == NULL || self->client == NULL || make_pipe(self->wake) == -1 || make_pipe(self->out) == -1 ||
        make_pipe(self->stop_pipe) == -1 ||
        aeCreateFileEvent(self->ae, self->wake[0], AE_READABLE, on_wake, self) == AE_ERR ||
        aeCreateFileEvent(self->ae, self->stop_pipe[0], AE_READABLE, on_stop, self) == AE_ERR) {
        PyErr_SetFromErrno(PyExc_OSError);
        Py_DECREF(self);
        return NULL;
    }
    return (PyObject *)self;
}

static void free_cancels(Request *r) {
    while (r != NULL) {
        Request *next = r->cancel_next;
        request_decref(r);
        r = next;
    }
}

static void EventLoop_dealloc(EventLoop *self) {
    while (self->abandoned != NULL) {
        Request *r = self->abandoned;
        self->abandoned = r->next;
        request_decref(r);
    }
    while (self->closing != NULL) {
        Pool *p = self->closing;
        self->closing = p->next;
        pool_decref(p);
    }
    if (self->client != NULL) {
        hx_client_free(self->client);
    }
    limiter_decref(self->limiter);
    if (self->ae != NULL) {
        aeDeleteEventLoop(self->ae);
    }
    for (int i = 0; i < 2; i++) {
        int fds[] = {self->wake[i], self->out[i], self->stop_pipe[i]};
        for (int j = 0; j < 3; j++) {
            if (fds[j] != -1) {
                close(fds[j]);
            }
        }
    }
    pthread_mutex_destroy(&self->lock);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *EventLoop_main(EventLoop *self, PyObject *args) {
    self->running = true;
    Py_BEGIN_ALLOW_THREADS;
    while (!self->stop) {
        aeProcessEvents(self->ae, AE_ALL_EVENTS);
    }
    hx_client_free(self->client);
    self->client = NULL;
    Py_END_ALLOW_THREADS;
    self->running = false;
    Py_RETURN_NONE;
}

static PyObject *EventLoop_stop(EventLoop *self, PyObject *args) {
    atomic_store(&self->closed, true);
    poke(self->stop_pipe[1]);
    Py_RETURN_NONE;
}

static void free_notes(Note *n) {
    while (n != NULL) {
        Note *next = n->next;
        if (n->kind == NOTE_HEADERS) {
            request_decref(n->ptr);
        } else if (n->kind == NOTE_STREAM_WAKE || n->kind == NOTE_STREAM_RESUME) {
            stream_decref(n->ptr);
        } else {
            upload_decref(n->ptr);
        }
        free(n);
        n = next;
    }
}

static void abandon_list(EventLoop *loop, PyObject *futures, Request *r, bool live) {
    while (r != NULL) {
        Request *next = live ? r->live_next : r->next;
        if (r->future != NULL) {
            PyList_Append(futures, r->future);
            Py_CLEAR(r->future);
        }
        if (r->delivered) {
            Stream *st = r->stream;
            pthread_mutex_lock(&st->lock);
            Request *held = NULL;
            if (!st->complete) {
                st->complete = true;
                st->error_code = E_ABORTED_BY_CALLBACK;
                snprintf(st->error, sizeof(st->error), "engine closed before the request finished");
                held = st->req;
                st->req = NULL;
            }
            PyObject *w = st->waiter;
            st->waiter = NULL;
            pthread_mutex_unlock(&st->lock);
            if (w != NULL) {
                PyList_Append(futures, w);
                Py_DECREF(w);
            }
            if (held != NULL) {
                request_decref(held);
            }
        }
        Py_CLEAR(r->session);
        r->next = loop->abandoned;
        loop->abandoned = r;
        r = next;
    }
}

static PyObject *EventLoop_abandon(EventLoop *self, PyObject *args) {
    if (self->running) {
        PyErr_SetString(PyExc_RuntimeError, "the event loop is still running");
        return NULL;
    }
    PyObject *futures = PyList_New(0);
    if (futures == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&self->lock);
    Request *incoming = self->incoming, *completed = self->completed, *cancels = self->cancels;
    Note *notes = self->notes, *resumes = self->resumes;
    self->incoming = self->incoming_tail = self->completed = self->completed_tail = NULL;
    self->cancels = self->cancels_tail = NULL;
    self->notes = self->notes_tail = self->resumes = self->resumes_tail = NULL;
    pthread_mutex_unlock(&self->lock);
    free_notes(notes);
    free_notes(resumes);
    Request *live = self->live;
    self->live = NULL;
    free_cancels(cancels);
    abandon_list(self, futures, incoming, false);
    abandon_list(self, futures, live, true);
    abandon_list(self, futures, completed, false);
    return futures;
}

static PyObject *EventLoop_stats(EventLoop *self, PyObject *args) {
    return stats_dict(&self->stats, PyLong_FromLong(atomic_load_explicit(&self->open, memory_order_relaxed)));
}

static PyObject *EventLoop_host_stats(EventLoop *self, PyObject *args) { return PyDict_New(); }

static PyObject *EventLoop_limiter_state(EventLoop *self, PyObject *args) { return limiter_state(self->limiter); }

static PyObject *EventLoop_get_out_fd(EventLoop *self, PyObject *args) { return PyLong_FromLong(self->out[0]); }

static int error_code(hx_error e) {
    switch (e) {
    case HX_OK:
        return 0;
    case HX_ERR_DNS:
        return E_COULDNT_RESOLVE_HOST;
    case HX_ERR_CONNECT:
        return E_COULDNT_CONNECT;
    case HX_ERR_SEND:
        return E_SEND_ERROR;
    case HX_ERR_RECEIVE:
        return E_RECV_ERROR;
    case HX_ERR_PROTOCOL:
        return E_WEIRD_SERVER_REPLY;
    case HX_ERR_DECODE:
    case HX_ERR_UNKNOWN_ENCODING:
        return E_BAD_CONTENT_ENCODING;
    case HX_ERR_TIMEOUT:
        return E_OPERATION_TIMEDOUT;
    case HX_ERR_CANCELLED:
        return E_ABORTED_BY_CALLBACK;
    case HX_ERR_INVALID:
        return E_URL_MALFORMAT;
    case HX_ERR_NOMEM:
        return E_OUT_OF_MEMORY;
    }
    return E_RECV_ERROR;
}

static void error_text(const Request *r, char *message, size_t size) {
    const hx_event *e = &r->done;
    switch (e->error) {
    case HX_ERR_DNS:
        snprintf(message, size, "Could not resolve host: %s", r->host);
        break;
    case HX_ERR_CONNECT:
        snprintf(message, size, "Failed to connect to %s port %u: %s", r->host, r->port,
                 e->sys_errno ? strerror(e->sys_errno) : "Couldn't connect to server");
        break;
    case HX_ERR_TIMEOUT:
        snprintf(message, size, "Operation timed out after %lld milliseconds",
                 (long long)((e->times[HX_T_DONE] - e->times[HX_T_START]) / 1000000));
        break;
    case HX_ERR_SEND:
        snprintf(message, size, "Send failure: %s", strerror(e->sys_errno));
        break;
    case HX_ERR_RECEIVE:
        snprintf(message, size, "%s",
                 e->sys_errno ? strerror(e->sys_errno) : "Server closed the connection before the response ended");
        break;
    case HX_ERR_PROTOCOL:
        snprintf(message, size, "Invalid HTTP response from the server");
        break;
    case HX_ERR_DECODE:
        snprintf(message, size, "Error while processing content unencoding");
        break;
    case HX_ERR_UNKNOWN_ENCODING:
        snprintf(message, size, "Unrecognized content encoding type. hx understands deflate, gzip content encodings.");
        break;
    case HX_ERR_CANCELLED:
        snprintf(message, size, "Request cancelled");
        break;
    case HX_ERR_INVALID:
        snprintf(message, size, "Malformed request");
        break;
    default:
        snprintf(message, size, "Out of memory");
    }
}

static PyObject *error_tuple(Request *r) {
    char message[512];
    error_text(r, message, sizeof(message));
    return Py_BuildValue("(is)", error_code(r->done.error), message);
}

static double seconds(const hx_event *e, int point) {
    int64_t t = e->times[point];
    return t > 0 ? (double)((t - e->times[HX_T_START] + 500) / 1000) / 1e6 : 0.0;
}

static Response *response_from(Request *r) {
    Response *resp = PyObject_New(Response, &ResponseType);
    if (resp == NULL) {
        return NULL;
    }
    const hx_event *e = &r->done;
    resp->status = e->status;
    resp->version = e->version;
    resp->header = r->header;
    r->header = (hx_buf){0};
    resp->body = r->received;
    r->received = (hx_buf){0};
    resp->body_bytes = NULL;
    resp->body_discarded = r->discard_body;
    for (int i = 0; i < HX_T_COUNT; i++) {
        resp->times[i] = seconds(e, i);
    }
    resp->times[HX_T_TLS] = 0.0;
    resp->size_upload = r->body != NULL ? (long long)r->body_len : 0;
    resp->size_download = (long long)r->body_bytes;
    memcpy(resp->primary_ip, e->primary_ip, sizeof(resp->primary_ip));
    resp->url = r->url;
    r->url = NULL;
    resp->queue_time = (double)((r->queue_ns + 500) / 1000) / 1e6;
    resp->stream = NULL;
    resp->upload = NULL;
    resp->loop = NULL;
    resp->synced = true;
    if (r->upload != NULL) {
        resp->size_upload = r->upload->total;
        resp->upload = r->upload;
        atomic_fetch_add(&r->upload->refs, 1);
    }
    return resp;
}

static Response *response_at_headers(Request *r) {
    Response *resp = response_from(r);
    if (resp == NULL) {
        return NULL;
    }
    resp->status = r->head_status;
    resp->version = r->head_version;
    for (int i = 0; i < HX_T_COUNT; i++) {
        resp->times[i] = 0.0;
    }
    resp->size_download = 0;
    resp->primary_ip[0] = '\0';
    resp->stream = r->stream;
    atomic_fetch_add(&r->stream->refs, 1);
    resp->loop = r->loop;
    Py_INCREF(r->loop);
    resp->synced = false;
    return resp;
}

static bool response_sync(Response *self) {
    if (self->synced) {
        return true;
    }
    Stream *s = self->stream;
    pthread_mutex_lock(&s->lock);
    if (s->complete) {
        self->synced = true;
        for (int i = 0; i < HX_T_COUNT; i++) {
            self->times[i] = seconds(&s->done, i);
        }
        self->times[HX_T_TLS] = 0.0;
        self->size_download = s->size_download;
        self->size_upload = s->size_upload;
        memcpy(self->primary_ip, s->done.primary_ip, sizeof(self->primary_ip));
    }
    pthread_mutex_unlock(&s->lock);
    if (!self->synced) {
        PyErr_SetString(PyExc_RuntimeError, "the body hasn't been read yet");
    }
    return self->synced;
}

static void append_entry(PyObject **list, PyObject *entry) {
    if (*list != NULL && (entry == NULL || PyList_Append(*list, entry) == -1)) {
        Py_CLEAR(*list);
    }
    Py_XDECREF(entry);
}

static PyObject *EventLoop_get_completed(EventLoop *self, PyObject *args) {
    drain_pipe(self->out[0]);
    pthread_mutex_lock(&self->lock);
    Request *r = self->completed;
    Note *notes = self->notes;
    self->completed = self->completed_tail = NULL;
    self->notes = self->notes_tail = NULL;
    pthread_mutex_unlock(&self->lock);
    PyObject *list = PyList_New(0);
    while (notes != NULL) {
        Note *n = notes;
        notes = n->next;
        if (n->kind == NOTE_HEADERS) {
            Request *h = n->ptr;
            Response *resp = response_at_headers(h);
            append_entry(&list, resp != NULL ? Py_BuildValue("(ONO)", Py_None, resp, h->future) : NULL);
            Py_CLEAR(h->future);
            request_decref(h);
        } else if (n->kind == NOTE_STREAM_WAKE) {
            Stream *st = n->ptr;
            PyObject *w = take_waiter(&st->lock, &st->waiter);
            if (w != NULL) {
                append_entry(&list, Py_BuildValue("(OON)", Py_None, Py_None, w));
            }
            stream_decref(st);
        } else {
            Upload *up = n->ptr;
            PyObject *w = take_waiter(&up->lock, &up->waiter);
            if (w != NULL) {
                append_entry(&list, Py_BuildValue("(OON)", Py_None, Py_None, w));
            }
            upload_decref(up);
        }
        free(n);
    }
    while (r != NULL) {
        Request *next = r->next;
        PyObject *entry = NULL;
        if (r->delivered) {
            Py_CLEAR(r->session);
            request_decref(r);
            r = next;
            continue;
        }
        if (list != NULL) {
            if (r->done.error == HX_OK) {
                Response *resp = response_from(r);
                entry = resp != NULL ? Py_BuildValue("(ONO)", Py_None, resp, r->future) : NULL;
            } else {
                entry = Py_BuildValue("(NOO)", error_tuple(r), Py_None, r->future);
            }
        }
        if (entry == NULL || PyList_Append(list, entry) == -1) {
            Py_CLEAR(list);
        }
        Py_XDECREF(entry);
        Py_CLEAR(r->future);
        Py_CLEAR(r->session);
        request_decref(r);
        r = next;
    }
    return list;
}

static PyMethodDef EventLoop_methods[] = {
    {"main", (PyCFunction)EventLoop_main, METH_NOARGS, "Run the event loop"},
    {"abandon", (PyCFunction)EventLoop_abandon, METH_NOARGS, "Free requests in flight once stopped"},
    {"stop", (PyCFunction)EventLoop_stop, METH_NOARGS, "Stop the event loop"},
    {"_limiter_state", (PyCFunction)EventLoop_limiter_state, METH_NOARGS, "The limiter's counts, for tests"},
    {"stats", (PyCFunction)EventLoop_stats, METH_NOARGS, "The engine's request and connection counts"},
    {"host_stats", (PyCFunction)EventLoop_host_stats, METH_NOARGS, "The engine's counts for each host"},
    {"get_out_fd", (PyCFunction)EventLoop_get_out_fd, METH_NOARGS, "Get the outbound file descriptor"},
    {"get_completed", (PyCFunction)EventLoop_get_completed, METH_NOARGS, "Get the error, response and future"},
    {NULL}};

static PyTypeObject EventLoopType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "httpperf._httpperf_hx.EventLoop",
    .tp_basicsize = sizeof(EventLoop),
    .tp_dealloc = (destructor)EventLoop_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = EventLoop_methods,
    .tp_new = EventLoop_new,
};

static PyObject *Session_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    PyObject *loop;
    const char *connections, *tls, *http_version;
    long idle_timeout, max_connection_age;
    int max_host_connections, max_connections;
    if (!PyArg_ParseTuple(args, "Osssllii", &loop, &connections, &tls, &http_version, &idle_timeout,
                          &max_connection_age, &max_host_connections, &max_connections)) {
        return NULL;
    }
    if (loop != Py_None && !PyObject_TypeCheck(loop, &EventLoopType)) {
        PyErr_SetString(PyExc_TypeError, "loop should be an EventLoop or None");
        return NULL;
    }
    if (max_host_connections < 0 || max_connections < 0) {
        PyErr_SetString(PyExc_ValueError, "connection limits must not be negative");
        return NULL;
    }
    if (idle_timeout < 0 || max_connection_age < 0) {
        PyErr_SetString(PyExc_ValueError, "idle_timeout and max_connection_age must not be negative");
        return NULL;
    }
    bool own = strcmp(connections, "session") == 0, none = strcmp(connections, "none") == 0;
    if (!own && !none && strcmp(connections, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "connections must be 'shared', 'session' or 'none', not '%s'", connections);
        return NULL;
    }
    if (!own && !none && (max_host_connections || max_connections)) {
        PyErr_SetString(PyExc_ValueError, "session connection limits need connections='session' or 'none'; "
                                          "set limits for the shared pool on the Engine");
        return NULL;
    }
    if (strcmp(tls, "session") != 0 && strcmp(tls, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "tls must be 'shared' or 'session', not '%s'", tls);
        return NULL;
    }
    if (strcmp(http_version, "HTTP/2") != 0 && strcmp(http_version, "HTTP/1.1") != 0 &&
        strcmp(http_version, "HTTP/3") != 0) {
        PyErr_Format(PyExc_ValueError, "http_version must be 'HTTP/3', 'HTTP/2' or 'HTTP/1.1', not '%s'", http_version);
        return NULL;
    }
    Session *self = (Session *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    if (loop != Py_None) {
        Py_INCREF(loop);
        self->loop = (EventLoop *)loop;
    }
    self->own_pool = own;
    self->fresh = none;
    self->idle_timeout = idle_timeout;
    self->max_connection_age = max_connection_age;
    pthread_mutex_init(&self->jar_lock, NULL);
    self->jar = hx_jar_new();
    if (self->jar == NULL) {
        Py_DECREF(self);
        return PyErr_NoMemory();
    }
    if (own) {
        self->pool = calloc(1, sizeof(Pool));
        if (self->pool == NULL) {
            Py_DECREF(self);
            return PyErr_NoMemory();
        }
        atomic_init(&self->pool->refs, 1);
    }
    if (max_host_connections || max_connections) {
        self->limiter = limiter_new(max_host_connections, max_connections);
        if (self->limiter == NULL) {
            Py_DECREF(self);
            return PyErr_NoMemory();
        }
    }
    return (PyObject *)self;
}

static void close_pool(Session *self);

static void Session_dealloc(Session *self) {
    close_pool(self);
    if (self->jar != NULL) {
        hx_jar_free(self->jar);
        pthread_mutex_destroy(&self->jar_lock);
    }
    limiter_decref(self->limiter);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Session_attach(Session *self, PyObject *args) {
    PyObject *loop;
    if (!PyArg_ParseTuple(args, "O!", &EventLoopType, &loop)) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->loop == NULL) {
        Py_INCREF(loop);
        self->loop = (EventLoop *)loop;
    }
    Py_END_CRITICAL_SECTION();
    Py_RETURN_NONE;
}

static int hex(char c) {
    return c >= '0' && c <= '9'   ? c - '0'
           : c >= 'a' && c <= 'f' ? c - 'a' + 10
           : c >= 'A' && c <= 'F' ? c - 'A' + 10
                                  : -1;
}

static char *percent_decode(const char *s, size_t len) {
    char *out = malloc(len + 1), *o = out;
    if (out == NULL) {
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        if (s[i] == '%' && i + 2 < len && hex(s[i + 1]) >= 0 && hex(s[i + 2]) >= 0) {
            *o++ = (char)(hex(s[i + 1]) * 16 + hex(s[i + 2]));
            i += 2;
        } else {
            *o++ = s[i];
        }
    }
    *o = '\0';
    return out;
}

static const char BASE64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char *basic_auth(const char *user, const char *password) {
    size_t len = strlen(user) + 1 + strlen(password);
    char *plain = malloc(len + 1);
    char *header = malloc(21 + (len + 2) / 3 * 4 + 1);
    if (plain == NULL || header == NULL) {
        free(plain);
        free(header);
        return NULL;
    }
    snprintf(plain, len + 1, "%s:%s", user, password);
    char *o = header + sprintf(header, "Authorization: Basic ");
    const unsigned char *p = (const unsigned char *)plain;
    for (size_t i = 0; i < len; i += 3) {
        unsigned v = p[i] << 16 | (i + 1 < len ? p[i + 1] << 8 : 0) | (i + 2 < len ? p[i + 2] : 0);
        *o++ = BASE64[v >> 18 & 63];
        *o++ = BASE64[v >> 12 & 63];
        *o++ = i + 1 < len ? BASE64[v >> 6 & 63] : '=';
        *o++ = i + 2 < len ? BASE64[v & 63] : '=';
    }
    *o = '\0';
    free(plain);
    return header;
}

static char *copy(const char *s, size_t len) {
    char *out = malloc(len + 1);
    if (out != NULL) {
        memcpy(out, s, len);
        out[len] = '\0';
    }
    return out;
}

static int parse_url(Request *r, const char *url, char **user, char **password) {
    for (const char *p = url; *p; p++) {
        if ((unsigned char)*p <= ' ' || *p == 0x7f) {
            PyErr_SetString(PyExc_ValueError, "the URL has a space or control character in it");
            return -1;
        }
    }
    if (strncasecmp(url, "https://", 8) == 0) {
        return E_UNSUPPORTED_PROTOCOL;
    }
    if (strncasecmp(url, "http://", 7) != 0) {
        return E_UNSUPPORTED_PROTOCOL;
    }
    const char *authority = url + 7;
    const char *end = authority + strcspn(authority, "/?#");
    const char *at = NULL;
    for (const char *p = authority; p < end; p++) {
        if (*p == '@') {
            at = p;
        }
    }
    const char *hostport = authority;
    if (at != NULL) {
        const char *colon = memchr(authority, ':', (size_t)(at - authority));
        *user = percent_decode(authority, (size_t)((colon != NULL ? colon : at) - authority));
        *password = colon != NULL ? percent_decode(colon + 1, (size_t)(at - colon - 1)) : strdup("");
        hostport = at + 1;
    }
    const char *host_start = hostport, *host_end;
    const char *port_start = NULL;
    if (*hostport == '[') {
        const char *close = memchr(hostport, ']', (size_t)(end - hostport));
        if (close == NULL) {
            return E_URL_MALFORMAT;
        }
        host_start = hostport + 1;
        host_end = close;
        if (close + 1 < end) {
            if (close[1] != ':') {
                return E_URL_MALFORMAT;
            }
            port_start = close + 2;
        }
    } else {
        const char *colon = memchr(hostport, ':', (size_t)(end - hostport));
        host_end = colon != NULL ? colon : end;
        port_start = colon != NULL ? colon + 1 : NULL;
    }
    if (host_end == host_start) {
        return E_URL_MALFORMAT;
    }
    long port = 80;
    if (port_start != NULL && port_start < end) {
        port = 0;
        for (const char *p = port_start; p < end; p++) {
            if (*p < '0' || *p > '9' || (port = port * 10 + (*p - '0')) > 65535) {
                return E_URL_MALFORMAT;
            }
        }
    }
    if (port == 0) {
        return E_URL_MALFORMAT;
    }
    const char *path_end = end + strcspn(end, "#");
    r->port = (uint16_t)port;
    r->host = copy(host_start, (size_t)(host_end - host_start));
    size_t written_host = (size_t)((*hostport == '[' ? host_end + 1 : host_end) - hostport);
    char authority_buf[300];
    if (port != 80) {
        snprintf(authority_buf, sizeof(authority_buf), "%.*s:%ld", (int)written_host, hostport, port);
    } else {
        snprintf(authority_buf, sizeof(authority_buf), "%.*s", (int)written_host, hostport);
    }
    r->authority = strdup(authority_buf);
    size_t path_len = (size_t)(path_end - end);
    bool slash = path_len > 0 && *end == '/';
    r->target = malloc(path_len + 2);
    if (r->target != NULL) {
        snprintf(r->target, path_len + 2, "%s%.*s", slash ? "" : "/", (int)path_len, end);
    }
    size_t url_len = 7 + (size_t)(end - hostport) + path_len + 2;
    r->url = malloc(url_len);
    if (r->url != NULL) {
        snprintf(r->url, url_len, "http://%.*s%s%.*s", (int)(end - hostport), hostport, slash ? "" : "/", (int)path_len,
                 end);
    }
    if (r->host == NULL || r->authority == NULL || r->target == NULL || r->url == NULL) {
        return E_OUT_OF_MEMORY;
    }
    return 0;
}

static int add_header(Request *r, char *header) {
    if (header == NULL) {
        return -1;
    }
    char **grown = realloc(r->headers, (r->header_count + 1) * sizeof(char *));
    if (grown == NULL) {
        free(header);
        return -1;
    }
    r->headers = grown;
    r->headers[r->header_count++] = header;
    return 0;
}

static bool names(const char *header, const char *name) {
    size_t n = strlen(name);
    return strncasecmp(header, name, n) == 0 && (header[n] == ':' || header[n] == ';');
}

static int add_cookies(Session *self, PyObject *cookies) {
    if (cookies == Py_None) {
        return 0;
    }
    int64_t now = (int64_t)time(NULL);
    for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(cookies); i++) {
        PyObject *item = PyTuple_GET_ITEM(cookies, i);
        const char *line = PyUnicode_CheckExact(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (line == NULL) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_ValueError, "cookies should be a tuple of strings or None");
            }
            return -1;
        }
        pthread_mutex_lock(&self->jar_lock);
        hx_jar_add_line(self->jar, line, now);
        pthread_mutex_unlock(&self->jar_lock);
    }
    return 0;
}

static int add_headers(Request *r, PyObject *headers, bool *has_accept, bool *has_encoding, bool *has_cookie) {
    if (headers == Py_None) {
        return 0;
    }
    if (!PyTuple_CheckExact(headers)) {
        PyErr_SetString(PyExc_ValueError, "headers should be a tuple of strings or None");
        return -1;
    }
    for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(headers); i++) {
        PyObject *item = PyTuple_GET_ITEM(headers, i);
        const char *header = PyUnicode_CheckExact(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (header == NULL) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_ValueError, "headers should be a tuple of strings or None");
            }
            return -1;
        }
        if (names(header, "accept")) {
            *has_accept = true;
        }
        if (names(header, "accept-encoding")) {
            *has_encoding = true;
        }
        if (names(header, "cookie")) {
            *has_cookie = true;
        }
        size_t len = strlen(header);
        const char *colon = strchr(header, ':');
        if (colon != NULL && colon[1 + strspn(colon + 1, " \t")] == '\0') {
            continue;
        }
        if (colon == NULL && len > 0 && header[len - 1] == ';') {
            char *empty = copy(header, len);
            if (empty == NULL) {
                return PyErr_NoMemory(), -1;
            }
            empty[len - 1] = ':';
            if (add_header(r, empty) == -1) {
                return PyErr_NoMemory(), -1;
            }
            continue;
        }
        if (add_header(r, strdup(header)) == -1) {
            PyErr_NoMemory();
            return -1;
        }
    }
    return 0;
}

static int parse_timeout(PyObject *value, const char *name, int64_t *ns) {
    *ns = 0;
    if (value == Py_None) {
        return 0;
    }
    double s = PyFloat_AsDouble(value);
    if (s == -1.0 && PyErr_Occurred()) {
        return -1;
    }
    if (!(s >= 0 && s <= 1e9)) {
        PyErr_Format(PyExc_ValueError, "%s must be between 0 and 1e9 seconds, or None", name);
        return -1;
    }
    *ns = (int64_t)(s * 1e9);
    return 0;
}

static void upload_capsule_free(PyObject *capsule);

static PyObject *Session_request(Session *self, PyObject *args, PyObject *kwds) {
    PyObject *future, *headers, *auth, *cookies;
    const char *method, *url;
    const char *data = NULL;
    Py_ssize_t data_len = 0;
    PyObject *cert = Py_None, *verify = Py_False, *timeout = Py_None, *connect_timeout = Py_None;
    int discard_body = 0, stream = 0, websocket = 0;
    long long upload_size = -2;
    static char *kwlist[] = {
        "future",  "method",          "url",          "headers", "auth",        "cookies",   "data", "cert", "verify",
        "timeout", "connect_timeout", "discard_body", "stream",  "upload_size", "websocket", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OssOOOz#|OOOOppLp", kwlist, &future, &method, &url, &headers, &auth,
                                     &cookies, &data, &data_len, &cert, &verify, &timeout, &connect_timeout,
                                     &discard_body, &stream, &upload_size, &websocket)) {
        return NULL;
    }
    if (websocket) {
        PyErr_SetString(PyExc_NotImplementedError, "WebSockets aren't on the hx backend yet");
        return NULL;
    }
    if (cookies != Py_None && !PyTuple_CheckExact(cookies)) {
        PyErr_SetString(PyExc_ValueError, "cookies should be a tuple of strings or None");
        return NULL;
    }
    int64_t timeout_ns, connect_timeout_ns;
    if (parse_timeout(timeout, "timeout", &timeout_ns) ||
        parse_timeout(connect_timeout, "connect_timeout", &connect_timeout_ns)) {
        return NULL;
    }
    EventLoop *loop = NULL;
    bool closed;
    Py_BEGIN_CRITICAL_SECTION(self);
    closed = self->closed;
    loop = self->loop;
    Py_END_CRITICAL_SECTION();
    if (closed) {
        PyErr_SetString(PyExc_RuntimeError, "session is closed");
        return NULL;
    }
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "session has no engine");
        return NULL;
    }
    Request *r = calloc(1, sizeof(Request));
    if (r == NULL) {
        return PyErr_NoMemory();
    }
    atomic_init(&r->refs, 1);
    char *user = NULL, *password = NULL;
    int code = parse_url(r, url, &user, &password);
    bool has_accept = false, has_encoding = false, has_cookie = false;
    if (code == -1 || add_headers(r, headers, &has_accept, &has_encoding, &has_cookie) == -1 ||
        add_cookies(self, cookies) == -1) {
        free(user);
        free(password);
        request_free(r);
        return NULL;
    }
    if (auth != Py_None) {
        if (!PyTuple_CheckExact(auth) || PyTuple_GET_SIZE(auth) != 2 ||
            !PyUnicode_CheckExact(PyTuple_GET_ITEM(auth, 0)) || !PyUnicode_CheckExact(PyTuple_GET_ITEM(auth, 1))) {
            PyErr_SetString(PyExc_ValueError, "auth should be a tuple of strings (username, password) or None");
            free(user);
            free(password);
            request_free(r);
            return NULL;
        }
        free(user);
        free(password);
        const char *u = PyUnicode_AsUTF8(PyTuple_GET_ITEM(auth, 0));
        const char *p = u != NULL ? PyUnicode_AsUTF8(PyTuple_GET_ITEM(auth, 1)) : NULL;
        if (p == NULL) {
            request_free(r);
            return NULL;
        }
        user = strdup(u);
        password = strdup(p);
    }
    bool ok = code == 0 && (has_accept || add_header(r, strdup("Accept: */*")) == 0) &&
              (has_encoding || add_header(r, strdup("Accept-Encoding: deflate, gzip")) == 0) &&
              (user == NULL || add_header(r, basic_auth(user, password != NULL ? password : "")) == 0);
    free(user);
    free(password);
    r->method = strdup(method);
    if (data != NULL) {
        r->body = copy(data, (size_t)data_len);
        r->body_len = (size_t)data_len;
    }
    if (!ok || r->method == NULL || (data != NULL && r->body == NULL)) {
        request_free(r);
        if (code == E_UNSUPPORTED_PROTOCOL) {
            PyErr_SetString(PyExc_NotImplementedError, "the hx backend only does http:// so far");
            return NULL;
        }
        if (code == E_URL_MALFORMAT) {
            PyErr_Format(PyExc_ValueError, "malformed URL: %s", url);
            return NULL;
        }
        return PyErr_NoMemory();
    }
    r->loop = loop;
    r->send_cookies = !has_cookie;
    r->path = copy(r->target, strcspn(r->target, "?#"));
    if (r->path == NULL) {
        request_free(r);
        return PyErr_NoMemory();
    }
    r->discard_body = discard_body;
    r->fresh = self->fresh;
    r->close_after = self->fresh || self->idle_timeout == 0;
    r->pool = (uint64_t)(uintptr_t)self->pool;
    r->idle_timeout_ns = (int64_t)self->idle_timeout * 1000000000;
    r->max_age_ns = (int64_t)self->max_connection_age * 1000000000;
    r->limiter = self->limiter != NULL ? self->limiter : self->own_pool ? NULL : loop->limiter;
    if (r->limiter != NULL) {
        atomic_fetch_add(&r->limiter->refs, 1);
    }
    r->timeout_ns = timeout_ns;
    r->connect_timeout_ns = connect_timeout_ns;
    r->session_stats = &self->stats;
    PyObject *capsule = NULL;
    if (stream) {
        r->stream = stream_new();
        if (r->stream == NULL) {
            request_free(r);
            return PyErr_NoMemory();
        }
        r->stream->req = r;
        atomic_fetch_add(&r->refs, 1);
    }
    if (upload_size >= -1) {
        r->upload = upload_new();
        if (r->upload == NULL) {
            request_free(r);
            return PyErr_NoMemory();
        }
        r->upload_size = upload_size;
        r->upload->req = r;
        r->upload->loop = loop;
        Py_INCREF(loop);
        atomic_fetch_add(&r->upload->refs, 1);
        capsule = PyCapsule_New(r->upload, "httpperf.upload", upload_capsule_free);
        if (capsule == NULL) {
            Py_DECREF(loop);
            upload_decref(r->upload);
            request_free(r);
            return NULL;
        }
    }
    PyObject *handle = PyLong_FromVoidPtr(r);
    if (handle == NULL) {
        Py_XDECREF(capsule);
        request_free(r);
        return NULL;
    }
    Py_INCREF(future);
    r->future = future;
    Py_INCREF(self);
    r->session = self;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = inbox_empty(loop);
    push(&loop->incoming, &loop->incoming_tail, r);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->wake[1]);
    }
    return capsule != NULL ? Py_BuildValue("(NN)", handle, capsule) : handle;
}

static void send_cancel(EventLoop *loop, Request *r) {
    bool expected = false;
    if (atomic_load(&loop->closed) || !atomic_compare_exchange_strong(&r->cancel_sent, &expected, true)) {
        return;
    }
    atomic_fetch_add(&r->refs, 1);
    r->cancel_next = NULL;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = inbox_empty(loop);
    if (loop->cancels_tail != NULL) {
        loop->cancels_tail->cancel_next = r;
    } else {
        loop->cancels = r;
    }
    loop->cancels_tail = r;
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->wake[1]);
    }
}

static PyObject *Session_cancel(Session *self, PyObject *handle) {
    Request *r = PyLong_AsVoidPtr(handle);
    if (r == NULL) {
        return NULL;
    }
    send_cancel(r->loop, r);
    Py_RETURN_NONE;
}

static PyObject *Session_stats(Session *self, PyObject *args) {
    if (self->pool == NULL) {
        Py_INCREF(Py_None);
        return stats_dict(&self->stats, Py_None);
    }
    PyObject *open = PyLong_FromLong(atomic_load_explicit(&self->pool->open, memory_order_relaxed));
    return open != NULL ? stats_dict(&self->stats, open) : NULL;
}

static void close_pool(Session *self) {
    Pool *p = self->pool;
    EventLoop *loop = self->loop;
    self->pool = NULL;
    if (p == NULL || loop == NULL || atomic_load(&loop->closed)) {
        pool_decref(p);
        return;
    }
    pthread_mutex_lock(&loop->lock);
    bool was_empty = inbox_empty(loop);
    p->next = loop->closing;
    loop->closing = p;
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->wake[1]);
    }
}

static PyObject *Session_close(Session *self, PyObject *args) {
    Py_BEGIN_CRITICAL_SECTION(self);
    if (!self->closed) {
        self->closed = true;
        close_pool(self);
    }
    Py_END_CRITICAL_SECTION();
    Py_RETURN_NONE;
}

static PyObject *Session_get_cookielist(Session *self, PyObject *args) {
    hx_buf text = {0};
    pthread_mutex_lock(&self->jar_lock);
    bool ok = hx_jar_lines(self->jar, (int64_t)time(NULL), &text);
    pthread_mutex_unlock(&self->jar_lock);
    PyObject *list = ok ? PyList_New(0) : PyErr_NoMemory();
    for (size_t at = 0; list != NULL && at < text.len;) {
        const char *nl = memchr(text.data + at, '\n', text.len - at);
        size_t n = (size_t)(nl - (text.data + at));
        PyObject *line = PyUnicode_DecodeUTF8(text.data + at, (Py_ssize_t)n, "surrogateescape");
        if (line == NULL || PyList_Append(list, line) == -1) {
            Py_CLEAR(list);
        }
        Py_XDECREF(line);
        at += n + 1;
    }
    hx_buf_free(&text);
    return list;
}

static PyObject *Session_apply_cookielist(Session *self, PyObject *args) {
    PyObject *lines;
    if (!PyArg_ParseTuple(args, "O!", &PyTuple_Type, &lines)) {
        return NULL;
    }
    bool closed;
    Py_BEGIN_CRITICAL_SECTION(self);
    closed = self->closed;
    Py_END_CRITICAL_SECTION();
    if (closed) {
        PyErr_SetString(PyExc_RuntimeError, "session is closed");
        return NULL;
    }
    int64_t now = (int64_t)time(NULL);
    for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(lines); i++) {
        PyObject *item = PyTuple_GET_ITEM(lines, i);
        const char *line = PyUnicode_CheckExact(item) ? PyUnicode_AsUTF8(item) : NULL;
        if (line == NULL) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_ValueError, "cookie lines should be strings");
            }
            return NULL;
        }
        pthread_mutex_lock(&self->jar_lock);
        if (strcmp(line, "ALL") == 0 || strcmp(line, "SESS") == 0) {
            hx_jar_clear(self->jar, line[0] == 'S');
        } else if (strcmp(line, "FLUSH") != 0 && strcmp(line, "RELOAD") != 0) {
            hx_jar_add_line(self->jar, line, now);
        }
        pthread_mutex_unlock(&self->jar_lock);
    }
    Py_RETURN_NONE;
}

static PyObject *Session_limiter_state(Session *self, PyObject *args) { return limiter_state(self->limiter); }

static PyMethodDef Session_methods[] = {
    {"request", (PyCFunction)(void (*)(void))Session_request, METH_VARARGS | METH_KEYWORDS, "Send a request"},
    {"get_cookielist", (PyCFunction)Session_get_cookielist, METH_NOARGS, "Get the session's cookies"},
    {"apply_cookielist", (PyCFunction)Session_apply_cookielist, METH_VARARGS, "Apply cookie lines"},
    {"cancel", (PyCFunction)Session_cancel, METH_O, "Stop a request in flight"},
    {"_limiter_state", (PyCFunction)Session_limiter_state, METH_NOARGS, "The limiter's counts, for tests"},
    {"stats", (PyCFunction)Session_stats, METH_NOARGS, "The session's request counts"},
    {"close", (PyCFunction)Session_close, METH_NOARGS, "Close the session's connections"},
    {"attach", (PyCFunction)Session_attach, METH_VARARGS, "Attach the session to an EventLoop"},
    {NULL}};

static PyTypeObject SessionType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "httpperf._httpperf_hx.Session",
    .tp_basicsize = sizeof(Session),
    .tp_dealloc = (destructor)Session_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = Session_methods,
    .tp_new = Session_new,
};

static void stream_cancel(Response *self) {
    Stream *st = self->stream;
    pthread_mutex_lock(&st->lock);
    Request *r = st->req;
    if (r != NULL) {
        atomic_fetch_add(&r->refs, 1);
    }
    pthread_mutex_unlock(&st->lock);
    if (r != NULL) {
        send_cancel(self->loop, r);
        request_decref(r);
    }
}

static void Response_dealloc(Response *self) {
    if (self->stream != NULL) {
        stream_cancel(self);
        Py_XDECREF(take_waiter(&self->stream->lock, &self->stream->waiter));
        stream_decref(self->stream);
    }
    upload_decref(self->upload);
    Py_XDECREF(self->loop);
    hx_buf_free(&self->header);
    hx_buf_free(&self->body);
    Py_XDECREF(self->body_bytes);
    free(self->url);
    PyObject_Free(self);
}

static PyObject *Response_get_response_code(Response *self, PyObject *args) { return PyLong_FromLong(self->status); }

static PyObject *Response_get_http_version(Response *self, PyObject *args) {
    if (self->version == 10) {
        return PyUnicode_FromString("HTTP/1.0");
    }
    if (self->version == 11) {
        return PyUnicode_FromString("HTTP/1.1");
    }
    Py_RETURN_NONE;
}

static PyObject *Response_get_header(Response *self, PyObject *args) {
    return PyBytes_FromStringAndSize(self->header.data, (Py_ssize_t)self->header.len);
}

static PyObject *Response_get_body(Response *self, PyObject *args) {
    if (self->body_discarded) {
        Py_RETURN_NONE;
    }
    PyObject *body;
    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->body_bytes == NULL) {
        self->body_bytes = PyBytes_FromStringAndSize(self->body.data, (Py_ssize_t)self->body.len);
        if (self->body_bytes != NULL) {
            hx_buf_free(&self->body);
        }
    }
    body = self->body_bytes;
    Py_XINCREF(body);
    Py_END_CRITICAL_SECTION();
    return body;
}

static PyObject *Response_get_effective_url(Response *self, PyObject *args) { return PyUnicode_FromString(self->url); }

static PyObject *Response_get_primary_ip(Response *self, PyObject *args) {
    if (!response_sync(self)) {
        return NULL;
    }
    return PyUnicode_FromString(self->primary_ip);
}

static PyObject *Response_get_redirect_url(Response *self, PyObject *args) {
    if (self->status < 300 || self->status >= 400) {
        Py_RETURN_NONE;
    }
    PyObject *header = Response_get_header(self, NULL);
    PyObject *pairs = header != NULL ? module_parse_headers(NULL, header) : NULL;
    Py_XDECREF(header);
    if (pairs == NULL) {
        return NULL;
    }
    PyObject *location = NULL;
    for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(pairs); i++) {
        PyObject *name = PyTuple_GET_ITEM(PyTuple_GET_ITEM(pairs, i), 0);
        const char *s = PyUnicode_AsUTF8(name);
        if (s != NULL && strcasecmp(s, "location") == 0) {
            location = PyTuple_GET_ITEM(PyTuple_GET_ITEM(pairs, i), 1);
        }
    }
    PyObject *result = NULL;
    if (location == NULL) {
        result = Py_None;
        Py_INCREF(result);
    } else {
        PyObject *parse = PyImport_ImportModule("urllib.parse");
        if (parse != NULL) {
            result = PyObject_CallMethod(parse, "urljoin", "sO", self->url, location);
            Py_DECREF(parse);
        }
    }
    Py_DECREF(pairs);
    return result;
}

#define TIME(name, point)                                                  \
    static PyObject *Response_get_##name(Response *self, PyObject *args) { \
        if (!response_sync(self)) {                                        \
            return NULL;                                                   \
        }                                                                  \
        return PyFloat_FromDouble(self->times[point]);                     \
    }
TIME(total_time, HX_T_DONE)
TIME(namelookup_time, HX_T_DNS)
TIME(connect_time, HX_T_CONNECT)
TIME(appconnect_time, HX_T_TLS)
TIME(pretransfer_time, HX_T_FIRST_SENT)
TIME(posttransfer_time, HX_T_LAST_SENT)
TIME(starttransfer_time, HX_T_FIRST_BYTE)

static PyObject *Response_get_size_upload(Response *self, PyObject *args) {
    if (!response_sync(self)) {
        return NULL;
    }
    return PyLong_FromLongLong(self->size_upload);
}

static PyObject *Response_get_size_download(Response *self, PyObject *args) {
    if (!response_sync(self)) {
        return NULL;
    }
    return PyLong_FromLongLong(self->size_download);
}

static PyObject *Response_stream_take(Response *self, PyObject *args) {
    Stream *st = self->stream;
    pthread_mutex_lock(&st->lock);
    hx_buf taken = st->buf;
    st->buf = st->spare;
    st->spare = taken;
    bool resume = st->paused;
    if (resume) {
        st->paused = false;
        st->paused_ns += clock_ns() - st->paused_since;
        atomic_fetch_add(&st->refs, 1);
    }
    bool complete = st->complete;
    pthread_mutex_unlock(&st->lock);
    if (resume) {
        if (atomic_load(&self->loop->closed)) {
            stream_decref(st);
        } else {
            send_resume(self->loop, NOTE_STREAM_RESUME, st);
        }
    }
    if (st->spare.len == 0 && complete) {
        Py_RETURN_NONE;
    }
    PyObject *data = PyBytes_FromStringAndSize(st->spare.data, (Py_ssize_t)st->spare.len);
    st->spare.len = 0;
    return data;
}

static PyObject *Response_stream_wait(Response *self, PyObject *waiter) {
    Stream *st = self->stream;
    pthread_mutex_lock(&st->lock);
    bool wait = st->buf.len == 0 && !st->complete;
    PyObject *old = NULL;
    if (wait) {
        old = st->waiter;
        Py_INCREF(waiter);
        st->waiter = waiter;
        st->want_wake = true;
    }
    pthread_mutex_unlock(&st->lock);
    Py_XDECREF(old);
    return PyBool_FromLong(wait);
}

static PyObject *Response_stream_close(Response *self, PyObject *args) {
    if (self->stream != NULL) {
        stream_cancel(self);
    }
    Py_RETURN_NONE;
}

static PyObject *Response_stream_result(Response *self, PyObject *args) {
    Stream *st = self->stream;
    if (st == NULL) {
        Py_RETURN_NONE;
    }
    pthread_mutex_lock(&st->lock);
    int code = st->complete ? st->error_code : 0;
    char message[512];
    memcpy(message, st->error, sizeof(message));
    pthread_mutex_unlock(&st->lock);
    if (code == 0) {
        Py_RETURN_NONE;
    }
    return Py_BuildValue("(is)", code, message);
}

static PyObject *Response_get_paused_time(Response *self, PyObject *args) {
    int64_t ns = 0;
    if (self->stream != NULL) {
        pthread_mutex_lock(&self->stream->lock);
        bool complete = self->stream->complete;
        ns += self->stream->paused_ns;
        pthread_mutex_unlock(&self->stream->lock);
        if (!complete) {
            PyErr_SetString(PyExc_RuntimeError, "the body hasn't been read yet");
            return NULL;
        }
    }
    if (self->upload != NULL) {
        pthread_mutex_lock(&self->upload->lock);
        ns += self->upload->paused_ns;
        pthread_mutex_unlock(&self->upload->lock);
    }
    return PyFloat_FromDouble((double)ns / 1e9);
}

static PyObject *Response_get_queue_time(Response *self, PyObject *args) {
    return PyFloat_FromDouble(self->queue_time);
}

static PyMethodDef Response_methods[] = {
    {"get_effective_url", (PyCFunction)Response_get_effective_url, METH_NOARGS, ""},
    {"get_response_code", (PyCFunction)Response_get_response_code, METH_NOARGS, ""},
    {"get_total_time", (PyCFunction)Response_get_total_time, METH_NOARGS, ""},
    {"get_namelookup_time", (PyCFunction)Response_get_namelookup_time, METH_NOARGS, ""},
    {"get_connect_time", (PyCFunction)Response_get_connect_time, METH_NOARGS, ""},
    {"get_appconnect_time", (PyCFunction)Response_get_appconnect_time, METH_NOARGS, ""},
    {"get_pretransfer_time", (PyCFunction)Response_get_pretransfer_time, METH_NOARGS, ""},
    {"get_starttransfer_time", (PyCFunction)Response_get_starttransfer_time, METH_NOARGS, ""},
    {"get_posttransfer_time", (PyCFunction)Response_get_posttransfer_time, METH_NOARGS, ""},
    {"get_size_upload", (PyCFunction)Response_get_size_upload, METH_NOARGS, ""},
    {"get_size_download", (PyCFunction)Response_get_size_download, METH_NOARGS, ""},
    {"get_primary_ip", (PyCFunction)Response_get_primary_ip, METH_NOARGS, ""},
    {"get_http_version", (PyCFunction)Response_get_http_version, METH_NOARGS, ""},
    {"get_queue_time", (PyCFunction)Response_get_queue_time, METH_NOARGS, ""},
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, ""},
    {"get_header", (PyCFunction)Response_get_header, METH_NOARGS, ""},
    {"get_body", (PyCFunction)Response_get_body, METH_NOARGS, ""},
    {"get_paused_time", (PyCFunction)Response_get_paused_time, METH_NOARGS, ""},
    {"stream_take", (PyCFunction)Response_stream_take, METH_NOARGS, ""},
    {"stream_wait", (PyCFunction)Response_stream_wait, METH_O, ""},
    {"stream_close", (PyCFunction)Response_stream_close, METH_NOARGS, ""},
    {"stream_result", (PyCFunction)Response_stream_result, METH_NOARGS, ""},
    {NULL}};

static PyTypeObject ResponseType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "httpperf._httpperf_hx.Response",
    .tp_basicsize = sizeof(Response),
    .tp_dealloc = (destructor)Response_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = Response_methods,
};

static PyObject *module_default_ca_file(PyObject *module, PyObject *args) { Py_RETURN_NONE; }

static void upload_capsule_free(PyObject *capsule) {
    Upload *u = PyCapsule_GetPointer(capsule, "httpperf.upload");
    Py_XDECREF(take_waiter(&u->lock, &u->waiter));
    Py_XDECREF((PyObject *)u->loop);
    upload_decref(u);
}

static void upload_resume(Upload *u) {
    if (atomic_load(&u->loop->closed)) {
        return;
    }
    atomic_fetch_add(&u->refs, 1);
    send_resume(u->loop, NOTE_UPLOAD_RESUME, u);
}

static PyObject *module_upload_push(PyObject *module, PyObject *args) {
    PyObject *capsule;
    Py_buffer data;
    if (!PyArg_ParseTuple(args, "Oy*", &capsule, &data)) {
        return NULL;
    }
    Upload *u = PyCapsule_GetPointer(capsule, "httpperf.upload");
    if (u == NULL) {
        PyBuffer_Release(&data);
        return NULL;
    }
    pthread_mutex_lock(&u->lock);
    if (u->done) {
        pthread_mutex_unlock(&u->lock);
        PyBuffer_Release(&data);
        Py_RETURN_NONE;
    }
    size_t n = (size_t)data.len < STREAM_BUFFER - u->len ? (size_t)data.len : STREAM_BUFFER - u->len;
    size_t tail = (u->head + u->len) % STREAM_BUFFER;
    size_t first = STREAM_BUFFER - tail < n ? STREAM_BUFFER - tail : n;
    memcpy(u->ring + tail, data.buf, first);
    memcpy(u->ring, (char *)data.buf + first, n - first);
    u->len += n;
    bool resume = u->hungry && n > 0;
    pthread_mutex_unlock(&u->lock);
    PyBuffer_Release(&data);
    if (resume) {
        upload_resume(u);
    }
    return PyLong_FromSize_t(n);
}

static PyObject *module_upload_finish(PyObject *module, PyObject *capsule) {
    Upload *u = PyCapsule_GetPointer(capsule, "httpperf.upload");
    if (u == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&u->lock);
    u->eof = true;
    bool resume = u->hungry;
    pthread_mutex_unlock(&u->lock);
    if (resume) {
        upload_resume(u);
    }
    Py_RETURN_NONE;
}

static PyObject *module_upload_wait(PyObject *module, PyObject *args) {
    PyObject *capsule, *waiter;
    if (!PyArg_ParseTuple(args, "OO", &capsule, &waiter)) {
        return NULL;
    }
    Upload *u = PyCapsule_GetPointer(capsule, "httpperf.upload");
    if (u == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&u->lock);
    bool wait = !u->done && u->len > STREAM_BUFFER / 2;
    PyObject *old = NULL;
    if (wait) {
        old = u->waiter;
        Py_INCREF(waiter);
        u->waiter = waiter;
        u->want_wake = true;
    }
    pthread_mutex_unlock(&u->lock);
    Py_XDECREF(old);
    return PyBool_FromLong(wait);
}

static PyMethodDef module_methods[] = {
    {"default_ca_file", module_default_ca_file, METH_NOARGS, "The built-in CA bundle path, or None"},
    {"parse_headers", module_parse_headers, METH_O, "The (name, value) pairs of the final header block"},
    {"upload_push", module_upload_push, METH_VARARGS,
     "Copy what fits of the bytes into an upload; None once it's over"},
    {"upload_finish", module_upload_finish, METH_O, "Mark the end of an upload's body"},
    {"upload_wait", module_upload_wait, METH_VARARGS, "Resolve the future when an upload has room"},
    {NULL}};

static struct PyModuleDef hx_module = {PyModuleDef_HEAD_INIT, "httpperf._httpperf_hx", NULL, -1, module_methods};

PyMODINIT_FUNC PyInit__httpperf_hx(void) {
    if (PyType_Ready(&EventLoopType) < 0 || PyType_Ready(&SessionType) < 0 || PyType_Ready(&ResponseType) < 0) {
        return NULL;
    }
    PyObject *m = PyModule_Create(&hx_module);
    if (m == NULL) {
        return NULL;
    }
#ifdef Py_GIL_DISABLED
    if (PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED) < 0) {
        Py_DECREF(m);
        return NULL;
    }
#endif
    Py_INCREF(&EventLoopType);
    Py_INCREF(&SessionType);
    Py_INCREF(&ResponseType);
    if (PyModule_AddObject(m, "EventLoop", (PyObject *)&EventLoopType) < 0 ||
        PyModule_AddObject(m, "Session", (PyObject *)&SessionType) < 0 ||
        PyModule_AddObject(m, "Response", (PyObject *)&ResponseType) < 0 ||
        PyModule_AddIntConstant(m, "websockets", 0) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    return m;
}
