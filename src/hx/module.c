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
};

typedef struct {
    atomic_long requests, errors, cancelled, connections, bytes_sent, bytes_received, running;
    atomic_long http_versions[4];
} Stats;

typedef struct EventLoop EventLoop;
typedef struct Session Session;

typedef struct Request {
    atomic_int refs;
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
} Request;

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
    Request *cancels, *cancels_tail;
    Request *completed, *completed_tail;
    Request *live;
    Stats stats;
    atomic_long open;
    size_t buffer_size;
};

struct Session {
    PyObject_HEAD EventLoop *loop;
    bool closed;
    bool own_pool;
    bool fresh;
    long idle_timeout;
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

static void request_free(Request *r) {
    free(r->url);
    free(r->host);
    free(r->authority);
    free(r->target);
    free(r->method);
    free_strings(r->headers, r->header_count);
    free(r->body);
    hx_buf_free(&r->header);
    hx_buf_free(&r->received);
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
    bool was_empty = loop->completed == NULL;
    push(&loop->completed, &loop->completed_tail, r);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->out[1]);
    }
}

static void handle_events(EventLoop *loop) {
    hx_event events[64];
    int count;
    while ((count = hx_client_events(loop->client, events, 64)) > 0) {
        for (int i = 0; i < count; i++) {
            hx_event *e = &events[i];
            Request *r = e->user;
            if (e->type == HX_EV_HEADERS) {
                r->header.len = 0;
                hx_buf_append(&r->header, e->data, e->len);
            } else if (e->type == HX_EV_BODY) {
                r->body_bytes += e->len;
                if (!r->discard_body) {
                    hx_buf_append(&r->received, e->data, e->len);
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
                complete(loop, r);
            }
        }
    }
    atomic_store_explicit(&loop->open, (long)hx_client_open_connections(loop->client), memory_order_relaxed);
}

static void on_fd(aeEventLoop *ae, int fd, void *data, int mask) {
    EventLoop *loop = data;
    hx_client_on_socket(loop->client, fd, (mask & AE_READABLE ? HX_READ : 0) | (mask & AE_WRITABLE ? HX_WRITE : 0));
    handle_events(loop);
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

static void start(EventLoop *loop, Request *r) {
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
    };
    r->live_next = loop->live;
    if (loop->live != NULL) {
        loop->live->live_prev = r;
    }
    loop->live = r;
    r->started = true;
    stats_add(loop, r, stat_requests, 1);
    stats_add(loop, r, stat_running, 1);
    r->hx = hx_request_start(loop->client, &q, r);
    if (r->hx == NULL) {
        r->done.error = HX_ERR_NOMEM;
        r->finished = true;
        stats_add(loop, r, stat_running, -1);
        stats_add(loop, r, stat_errors, 1);
        complete(loop, r);
    }
}

static void on_wake(aeEventLoop *ae, int fd, void *data, int mask) {
    EventLoop *loop = data;
    drain_pipe(fd);
    pthread_mutex_lock(&loop->lock);
    Request *incoming = loop->incoming, *cancels = loop->cancels;
    loop->incoming = loop->incoming_tail = loop->cancels = loop->cancels_tail = NULL;
    pthread_mutex_unlock(&loop->lock);
    while (incoming != NULL) {
        Request *r = incoming;
        incoming = r->next;
        start(loop, r);
    }
    handle_events(loop);
    while (cancels != NULL) {
        Request *r = cancels;
        cancels = r->cancel_next;
        if (r->started && !r->finished && r->hx != NULL) {
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
                         LOAD(running), "queued", 0);
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
    hx_config cfg = {.max_idle_per_host = (size_t)max_idle_connections, .read_size = self->buffer_size};
    self->ae = aeCreateEventLoop(200);
    self->client = hx_client_new(&cfg, (hx_io){watch, set_timer, io_now, self});
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
    if (self->client != NULL) {
        hx_client_free(self->client);
    }
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

static void abandon_list(EventLoop *loop, PyObject *futures, Request *r, bool live) {
    while (r != NULL) {
        Request *next = live ? r->live_next : r->next;
        if (r->future != NULL) {
            PyList_Append(futures, r->future);
            Py_CLEAR(r->future);
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
    self->incoming = self->incoming_tail = self->completed = self->completed_tail = NULL;
    self->cancels = self->cancels_tail = NULL;
    pthread_mutex_unlock(&self->lock);
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

static PyObject *EventLoop_limiter_state(EventLoop *self, PyObject *args) { Py_RETURN_NONE; }

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

static PyObject *error_tuple(Request *r) {
    const hx_event *e = &r->done;
    char message[512];
    switch (e->error) {
    case HX_ERR_DNS:
        snprintf(message, sizeof(message), "Could not resolve host: %s", r->host);
        break;
    case HX_ERR_CONNECT:
        snprintf(message, sizeof(message), "Failed to connect to %s port %u: %s", r->host, r->port,
                 e->sys_errno ? strerror(e->sys_errno) : "Couldn't connect to server");
        break;
    case HX_ERR_TIMEOUT:
        snprintf(message, sizeof(message), "Operation timed out after %lld milliseconds",
                 (long long)((e->times[HX_T_DONE] - e->times[HX_T_START]) / 1000000));
        break;
    case HX_ERR_SEND:
        snprintf(message, sizeof(message), "Send failure: %s", strerror(e->sys_errno));
        break;
    case HX_ERR_RECEIVE:
        snprintf(message, sizeof(message), "%s",
                 e->sys_errno ? strerror(e->sys_errno) : "Server closed the connection before the response ended");
        break;
    case HX_ERR_PROTOCOL:
        snprintf(message, sizeof(message), "Invalid HTTP response from the server");
        break;
    case HX_ERR_CANCELLED:
        snprintf(message, sizeof(message), "Request cancelled");
        break;
    case HX_ERR_INVALID:
        snprintf(message, sizeof(message), "Malformed request");
        break;
    default:
        snprintf(message, sizeof(message), "Out of memory");
    }
    return Py_BuildValue("(is)", error_code(e->error), message);
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
    return resp;
}

static PyObject *EventLoop_get_completed(EventLoop *self, PyObject *args) {
    drain_pipe(self->out[0]);
    pthread_mutex_lock(&self->lock);
    Request *r = self->completed;
    self->completed = self->completed_tail = NULL;
    pthread_mutex_unlock(&self->lock);
    PyObject *list = PyList_New(0);
    while (r != NULL) {
        Request *next = r->next;
        PyObject *entry = NULL;
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
    return (PyObject *)self;
}

static void Session_dealloc(Session *self) {
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

static int add_headers(Request *r, PyObject *headers, bool *has_accept) {
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
    if (stream || websocket || upload_size >= -1) {
        PyErr_SetString(PyExc_NotImplementedError, "streaming, uploads and WebSockets aren't on the hx backend yet");
        return NULL;
    }
    if (cookies != Py_None) {
        PyErr_SetString(PyExc_NotImplementedError, "cookies aren't on the hx backend yet");
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
    bool has_accept = false;
    if (code == -1 || add_headers(r, headers, &has_accept) == -1) {
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
    r->discard_body = discard_body;
    r->fresh = self->fresh;
    r->close_after = self->fresh || self->idle_timeout == 0;
    r->pool = self->own_pool ? (uint64_t)(uintptr_t)self : 0;
    r->idle_timeout_ns = (int64_t)self->idle_timeout * 1000000000;
    r->timeout_ns = timeout_ns;
    r->connect_timeout_ns = connect_timeout_ns;
    r->session_stats = &self->stats;
    Py_INCREF(future);
    r->future = future;
    Py_INCREF(self);
    r->session = self;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = loop->incoming == NULL && loop->cancels == NULL;
    push(&loop->incoming, &loop->incoming_tail, r);
    pthread_mutex_unlock(&loop->lock);
    if (was_empty) {
        poke(loop->wake[1]);
    }
    return PyLong_FromVoidPtr(r);
}

static PyObject *Session_cancel(Session *self, PyObject *handle) {
    Request *r = PyLong_AsVoidPtr(handle);
    if (r == NULL) {
        return NULL;
    }
    EventLoop *loop = r->loop;
    if (atomic_load(&loop->closed)) {
        Py_RETURN_NONE;
    }
    atomic_fetch_add(&r->refs, 1);
    r->cancel_next = NULL;
    pthread_mutex_lock(&loop->lock);
    bool was_empty = loop->incoming == NULL && loop->cancels == NULL;
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
    Py_RETURN_NONE;
}

static PyObject *Session_stats(Session *self, PyObject *args) {
    Py_INCREF(Py_None);
    return stats_dict(&self->stats, Py_None);
}

static PyObject *Session_close(Session *self, PyObject *args) {
    Py_BEGIN_CRITICAL_SECTION(self);
    self->closed = true;
    Py_END_CRITICAL_SECTION();
    Py_RETURN_NONE;
}

static PyObject *Session_get_cookielist(Session *self, PyObject *args) { return PyList_New(0); }

static PyObject *Session_apply_cookielist(Session *self, PyObject *args) { Py_RETURN_NONE; }

static PyObject *Session_limiter_state(Session *self, PyObject *args) { Py_RETURN_NONE; }

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

static void Response_dealloc(Response *self) {
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
    return PyLong_FromLongLong(self->size_upload);
}

static PyObject *Response_get_size_download(Response *self, PyObject *args) {
    return PyLong_FromLongLong(self->size_download);
}

static PyObject *Response_get_zero(Response *self, PyObject *args) { return PyFloat_FromDouble(0.0); }

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
    {"get_queue_time", (PyCFunction)Response_get_zero, METH_NOARGS, ""},
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, ""},
    {"get_header", (PyCFunction)Response_get_header, METH_NOARGS, ""},
    {"get_body", (PyCFunction)Response_get_body, METH_NOARGS, ""},
    {"get_paused_time", (PyCFunction)Response_get_zero, METH_NOARGS, ""},
    {NULL}};

static PyTypeObject ResponseType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "httpperf._httpperf_hx.Response",
    .tp_basicsize = sizeof(Response),
    .tp_dealloc = (destructor)Response_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = Response_methods,
};

static PyObject *module_default_ca_file(PyObject *module, PyObject *args) { Py_RETURN_NONE; }

static PyObject *module_upload_push(PyObject *module, PyObject *args) { Py_RETURN_NONE; }

static PyObject *module_upload_finish(PyObject *module, PyObject *upload) { Py_RETURN_NONE; }

static PyObject *module_upload_wait(PyObject *module, PyObject *args) { Py_RETURN_FALSE; }

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
