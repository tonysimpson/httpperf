#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "ae/ae.h"
#include <curl/curl.h>
#include <pthread.h>
#include <unistd.h>
#include <strings.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "structmember.h"

#define NO_ACTIVE_TIMER_ID -1

/* Macro for debugging */

#define DEBUG 0
#if defined(DEBUG) && DEBUG > 0
#include <sys/syscall.h>
#define DEBUG_PRINT(fmt, args...)                                                                                    \
    fprintf(stderr, "DEBUG: %s:%d:%s() tid=%ld: " fmt "\n", __FILE__, __LINE__, __func__, (long)syscall(SYS_gettid), \
            ##args)
#else
#define DEBUG_PRINT(fmt, args...)
#endif

#include <time.h>
static inline double gettime(void) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    return ((double)tp.tv_sec) + ((double)tp.tv_nsec / 1000000000.0);
}

static inline double monotonic_time(void) {
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return ((double)tp.tv_sec) + ((double)tp.tv_nsec / 1000000000.0);
}

/* Macro for debugging */

#define REQUEST_TRACE 0
#if defined(REQUEST_TRACE) && REQUEST_TRACE > 0
#include <sys/syscall.h>
#define REQUEST_TRACE_PRINT(location, pointer) fprintf(stderr, "%s %p %f\n", location, pointer, gettime())
#else
#define REQUEST_TRACE_PRINT(location, pointer) /* Don't do anything in release builds */
#endif

/* Used to tell gcc about expected code branches, likely not necessary */

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

struct AcRequestData;

#define HANDLE_POOL_SIZE 256

typedef struct {
    PyObject_HEAD aeEventLoop *event_loop;
    PyThreadState *thread_state;
    CURLM *multi;
    long long timer_id;
    bool stop;
    int req_in_read;
    int req_in_write;
    int req_out_read;
    int req_out_write;
    int stop_read;
    int stop_write;
    int curl_easy_cleanup_read;
    int curl_easy_cleanup_write;
    struct AcRequestData *unsent_head;
    struct AcRequestData *unsent_tail;
    struct AcRequestData *live_head;
    bool running;
    bool closing;
    struct Share *limiter;
    CURL *free_handles[HANDLE_POOL_SIZE];
    int free_handle_count;
} EventLoop;

typedef struct HostSlot {
    char *key;
    long active;
    bool multiplexed;
    struct HostSlot *next;
} HostSlot;

typedef struct Share {
    CURLSH *share;
    pthread_mutex_t locks[CURL_LOCK_DATA_LAST];
    atomic_int refs;
    long max_host_connections;
    long max_connections;
    long connections;
    HostSlot *hosts;
    struct AcRequestData *waiting_head;
    struct AcRequestData *waiting_tail;
    bool dispatching;
} Share;

typedef struct {
    CURL *curl;
    Share *share;
} CleanupItem;

static void share_lock(CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) {
    pthread_mutex_lock(&((Share *)userptr)->locks[data]);
}

static void share_unlock(CURL *handle, curl_lock_data data, void *userptr) {
    pthread_mutex_unlock(&((Share *)userptr)->locks[data]);
}

static Share *share_new(bool share_connections, bool share_tls, long max_host_connections, long max_connections) {
    Share *share = calloc(1, sizeof(Share));
    if (share == NULL) {
        return NULL;
    }
    share->share = curl_share_init();
    if (share->share == NULL) {
        free(share);
        return NULL;
    }
    for (int i = 0; i < CURL_LOCK_DATA_LAST; i++) {
        pthread_mutex_init(&share->locks[i], NULL);
    }
    atomic_init(&share->refs, 1);
    share->max_host_connections = max_host_connections;
    share->max_connections = max_connections;
    curl_share_setopt(share->share, CURLSHOPT_LOCKFUNC, share_lock);
    curl_share_setopt(share->share, CURLSHOPT_UNLOCKFUNC, share_unlock);
    curl_share_setopt(share->share, CURLSHOPT_USERDATA, share);
    curl_share_setopt(share->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_COOKIE);
    curl_share_setopt(share->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    if (share_tls) {
        curl_share_setopt(share->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
    }
    if (share_connections) {
        curl_share_setopt(share->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
    }
    return share;
}

static void share_incref(Share *share) { atomic_fetch_add(&share->refs, 1); }

static void share_decref(Share *share) {
    if (atomic_fetch_sub(&share->refs, 1) != 1) {
        return;
    }
    CURLSHcode rc = curl_share_cleanup(share->share);
    if (rc != CURLSHE_OK) {
        fprintf(stderr, "httpperf: curl_share_cleanup failed: %s\n", curl_share_strerror(rc));
        return;
    }
    while (share->hosts != NULL) {
        HostSlot *next = share->hosts->next;
        free(share->hosts->key);
        free(share->hosts);
        share->hosts = next;
    }
    for (int i = 0; i < CURL_LOCK_DATA_LAST; i++) {
        pthread_mutex_destroy(&share->locks[i]);
    }
    free(share);
}

static bool pipe_write(int fd, const void *buf, size_t len) {
    ssize_t n;
    do {
        n = write(fd, buf, len);
    } while (n == -1 && errno == EINTR);
    return n == (ssize_t)len;
}

static int pipe_read(int fd, void *buf, size_t len) {
    ssize_t n;
    do {
        n = read(fd, buf, len);
    } while (n == -1 && errno == EINTR);
    if (n == (ssize_t)len) {
        return 1;
    }
    if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return 0;
    }
    return -1;
}

static void queue_cleanup(EventLoop *loop, CURL *curl, Share *share) {
    CleanupItem item = {curl, share};
    if (loop == NULL || !pipe_write(loop->curl_easy_cleanup_write, &item, sizeof(item))) {
        if (curl != NULL) {
            curl_easy_cleanup(curl);
        }
        if (share != NULL) {
            share_decref(share);
        }
    }
}

typedef struct {
    PyObject_HEAD EventLoop *loop;
    Share *shared;
    bool fresh_connect;
    bool own_pool;
    long http_version;
    long idle_timeout;
    long max_connection_age;
    char *http_proxy;
    char *https_proxy;
    char *no_proxy;
} Session;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} Buffer;

#define MAX_BODY_PREALLOC (64 * 1024 * 1024)

static bool buffer_append(Buffer *b, const char *src, size_t n, size_t initial) {
    if (b->len + n > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : initial;
        while (cap < b->len + n) {
            cap *= 2;
        }
        char *data = realloc(b->data, cap);
        if (data == NULL) {
            return false;
        }
        b->data = data;
        b->cap = cap;
    }
    memcpy(b->data + b->len, src, n);
    b->len += n;
    return true;
}

static PyObject *buffer_to_bytes(Buffer *b, PyObject **cache) {
    if (*cache == NULL) {
        *cache = PyBytes_FromStringAndSize(b->data, (Py_ssize_t)b->len);
        if (*cache == NULL) {
            return NULL;
        }
        free(b->data);
        b->data = NULL;
        b->len = b->cap = 0;
    }
    Py_INCREF(*cache);
    return *cache;
}

typedef struct AcRequestData {
    char *method;
    char *url;
    char *auth;
    char *cert_file;
    char *key_file;
    bool verify;
    char *ca_file;
    long timeout_ms;
    long connect_timeout_ms;
    char error[CURL_ERROR_SIZE];
    PyObject *cookies;
    int cookies_len;
    char **cookies_str;
    PyObject *future;
    struct curl_slist *headers;
    Py_ssize_t req_data_len;
    char *req_data_buf;
    Session *session;
    Share *share;
    Share *limiter;
    CURL *curl;
    CURLcode result;
    Buffer header;
    Buffer body;
    HostSlot *slot;
    struct AcRequestData *next_waiting;
    struct AcRequestData *next_unsent;
    struct AcRequestData *live_prev;
    struct AcRequestData *live_next;
    double queued_at;
    double queue_time;
    atomic_int state;
    bool waiting;
    bool finished;
} AcRequestData;

enum { REQUEST_RUNNING, REQUEST_SENT, REQUEST_CANCELLING };

typedef struct {
    PyObject_HEAD Buffer header;
    Buffer body;
    PyObject *header_bytes;
    PyObject *body_bytes;
    Session *session;
    CURL *curl;
    double queue_time;
} Response;

/* Python deallocator for Response Object. For GC */

static void Response_dealloc(Response *self) {
    DEBUG_PRINT("response=%p", self);
    free(self->header.data);
    free(self->body.data);
    Py_XDECREF(self->header_bytes);
    Py_XDECREF(self->body_bytes);
    queue_cleanup(self->session->loop, self->curl, NULL);
    Py_XDECREF(self->session);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Response_get_header(Response *self, PyObject *args) {
    DEBUG_PRINT("");
    return buffer_to_bytes(&self->header, &self->header_bytes);
}

static PyObject *Response_get_body(Response *self, PyObject *args) {
    DEBUG_PRINT("");
    return buffer_to_bytes(&self->body, &self->body_bytes);
}

PyObject *resp_get_info_long(Response *self, CURLINFO info) {
    long value;
    curl_easy_getinfo(self->curl, info, &value);
    return PyLong_FromLong(value);
}

PyObject *resp_get_info_double(Response *self, CURLINFO info) {
    double value;
    curl_easy_getinfo(self->curl, info, &value);
    return PyFloat_FromDouble(value);
}

PyObject *resp_get_info_off_t(Response *self, CURLINFO info) {
    curl_off_t value;
    curl_easy_getinfo(self->curl, info, &value);
    return PyLong_FromLongLong(value);
}

PyObject *resp_get_info_unicode(Response *self, CURLINFO info) {
    char *value = NULL;
    curl_easy_getinfo(self->curl, info, &value);
    if (value == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(value);
}

#define RESPONSE_GETINFO(name, kind, info) \
    static PyObject *Response_get_##name(Response *self, PyObject *args) { return resp_get_info_##kind(self, info); }

RESPONSE_GETINFO(effective_url, unicode, CURLINFO_EFFECTIVE_URL)
RESPONSE_GETINFO(response_code, long, CURLINFO_RESPONSE_CODE)
RESPONSE_GETINFO(total_time, double, CURLINFO_TOTAL_TIME)
RESPONSE_GETINFO(namelookup_time, double, CURLINFO_NAMELOOKUP_TIME)
RESPONSE_GETINFO(connect_time, double, CURLINFO_CONNECT_TIME)
RESPONSE_GETINFO(appconnect_time, double, CURLINFO_APPCONNECT_TIME)
RESPONSE_GETINFO(pretransfer_time, double, CURLINFO_PRETRANSFER_TIME)
RESPONSE_GETINFO(starttransfer_time, double, CURLINFO_STARTTRANSFER_TIME)
RESPONSE_GETINFO(size_upload, off_t, CURLINFO_SIZE_UPLOAD_T)
RESPONSE_GETINFO(size_download, off_t, CURLINFO_SIZE_DOWNLOAD_T)
RESPONSE_GETINFO(primary_ip, unicode, CURLINFO_PRIMARY_IP)
RESPONSE_GETINFO(redirect_url, unicode, CURLINFO_REDIRECT_URL)

static PyObject *Response_get_posttransfer_time(Response *self, PyObject *args) {
#if LIBCURL_VERSION_NUM >= 0x080a00
    curl_off_t value = 0;
    curl_easy_getinfo(self->curl, CURLINFO_POSTTRANSFER_TIME_T, &value);
    return PyFloat_FromDouble(value / 1e6);
#else
    Py_RETURN_NONE;
#endif
}

static PyObject *Response_get_queue_time(Response *self, PyObject *args) {
    return PyFloat_FromDouble(self->queue_time);
}

static PyObject *Response_get_http_version(Response *self, PyObject *args) {
    long version = 0;
    curl_easy_getinfo(self->curl, CURLINFO_HTTP_VERSION, &version);
    switch (version) {
    case CURL_HTTP_VERSION_1_0:
        return PyUnicode_FromString("HTTP/1.0");
    case CURL_HTTP_VERSION_1_1:
        return PyUnicode_FromString("HTTP/1.1");
    case CURL_HTTP_VERSION_2_0:
        return PyUnicode_FromString("HTTP/2");
    case CURL_HTTP_VERSION_3:
        return PyUnicode_FromString("HTTP/3");
    }
    Py_RETURN_NONE;
}

static PyObject *session_cookielist(Session *session) {
    struct curl_slist *start = NULL;
    struct curl_slist *node = NULL;
    int len = 0;
    int i = 0;
    PyObject *list = NULL;
    if (session->shared != NULL) {
        CURL *curl = curl_easy_init();
        if (curl == NULL) {
            return PyErr_NoMemory();
        }
        curl_easy_setopt(curl, CURLOPT_SHARE, session->shared->share);
        curl_easy_getinfo(curl, CURLINFO_COOKIELIST, &start);
        curl_easy_cleanup(curl);
    }
    node = start;
    while (node != NULL) {
        len++;
        node = node->next;
    }
    list = PyList_New(len);
    node = start;
    while (node != NULL) {
        PyList_SET_ITEM(list, i++, PyUnicode_FromString(node->data));
        node = node->next;
    };
    curl_slist_free_all(start);
    return list;
}

static PyMethodDef Response_methods[] = {
    {"get_effective_url", (PyCFunction)Response_get_effective_url, METH_NOARGS, ""},
    {"get_response_code", (PyCFunction)Response_get_response_code, METH_NOARGS, ""},
    {"get_total_time", (PyCFunction)Response_get_total_time, METH_NOARGS, ""},
    {"get_namelookup_time", (PyCFunction)Response_get_namelookup_time, METH_NOARGS,
     "Gets elapsed time from start of request to when DNS was resolved in seconds"},
    {"get_connect_time", (PyCFunction)Response_get_connect_time, METH_NOARGS,
     "Get elapsed time from start of request to TCP connect in seconds"},
    {"get_appconnect_time", (PyCFunction)Response_get_appconnect_time, METH_NOARGS,
     "Get elapsed time from start of request to TLS/SSL negotioation complete in seconds"},
    {"get_pretransfer_time", (PyCFunction)Response_get_pretransfer_time, METH_NOARGS,
     "Get elapsed time from start of request we've started to send the request"},
    {"get_starttransfer_time", (PyCFunction)Response_get_starttransfer_time, METH_NOARGS,
     "Get elapsed time from start of request until the first byte is recieved in seconds"},
    {"get_posttransfer_time", (PyCFunction)Response_get_posttransfer_time, METH_NOARGS,
     "Get elapsed time from start of request until the request was sent in seconds"},
    {"get_size_upload", (PyCFunction)Response_get_size_upload, METH_NOARGS, ""},
    {"get_size_download", (PyCFunction)Response_get_size_download, METH_NOARGS, ""},
    {"get_primary_ip", (PyCFunction)Response_get_primary_ip, METH_NOARGS, ""},
    {"get_http_version", (PyCFunction)Response_get_http_version, METH_NOARGS, "Get the HTTP version used"},
    {"get_queue_time", (PyCFunction)Response_get_queue_time, METH_NOARGS,
     "Get the time spent waiting for a connection"},
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, "Get the redirect URL or None"},
    {"get_header", (PyCFunction)Response_get_header, METH_NOARGS, "Get the header"},
    {"get_body", (PyCFunction)Response_get_body, METH_NOARGS, "Get the body"},
    {NULL, NULL, 0, NULL}};

static PyMemberDef Response_members[] = {{NULL}};

static PyTypeObject ResponseType = {
    PyVarObject_HEAD_INIT(NULL, 0) "httpperf._httpperf.Response", /* tp_name */
    sizeof(Response),                                             /* tp_basicsize */
    0,                                                            /* tp_itemsize */
    (destructor)Response_dealloc,                                 /* tp_dealloc */
    0,                                                            /* tp_print */
    0,                                                            /* tp_getattr */
    0,                                                            /* tp_setattr */
    0,                                                            /* tp_reserved */
    0,                                                            /* tp_repr */
    0,                                                            /* tp_as_number */
    0,                                                            /* tp_as_sequence */
    0,                                                            /* tp_as_mapping */
    0,                                                            /* tp_hash  */
    0,                                                            /* tp_call */
    0,                                                            /* tp_str */
    0,                                                            /* tp_getattro */
    0,                                                            /* tp_setattro */
    0,                                                            /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                                           /* tp_flags */
    "Response Type",                                              /* tp_doc */
    0,                                                            /* tp_traverse */
    0,                                                            /* tp_clear */
    0,                                                            /* tp_richcompare */
    0,                                                            /* tp_weaklistoffset */
    0,                                                            /* tp_iter */
    0,                                                            /* tp_iternext */
    Response_methods,                                             /* tp_methods */
    Response_members,                                             /* tp_members */
    0,                                                            /* tp_getset */
    0,                                                            /* tp_base */
    0,                                                            /* tp_dict */
    0,                                                            /* tp_descr_get */
    0,                                                            /* tp_descr_set */
    0,                                                            /* tp_dictoffset */
    0,                                                            /* tp_init */
    0,                                                            /* tp_alloc */
    0,                                                            /* tp_new */
};

/* When at least one request has completed, write completed responses onto completion queue*/

static void check_multi(EventLoop *loop, CURLMcode rc) {
    if (rc == CURLM_ABORTED_BY_CALLBACK) {
        int running_handles;
        fprintf(stderr, "httpperf: event loop error, aborting all requests\n");
        curl_multi_perform(loop->multi, &running_handles);
    }
}

static void add_transfer(EventLoop *loop, AcRequestData *rd);
static void send_completed(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);

static char *host_key(const char *url) {
    CURLU *u = curl_url();
    char *scheme = NULL;
    char *host = NULL;
    char *port = NULL;
    char *key = NULL;
    if (u != NULL && curl_url_set(u, CURLUPART_URL, url, 0) == CURLUE_OK &&
        curl_url_get(u, CURLUPART_SCHEME, &scheme, 0) == CURLUE_OK &&
        curl_url_get(u, CURLUPART_HOST, &host, 0) == CURLUE_OK &&
        curl_url_get(u, CURLUPART_PORT, &port, CURLU_DEFAULT_PORT) == CURLUE_OK) {
        size_t len = strlen(scheme) + strlen(host) + strlen(port) + 5;
        key = malloc(len);
        if (key != NULL) {
            snprintf(key, len, "%s://%s:%s", scheme, host, port);
        }
    }
    curl_free(scheme);
    curl_free(host);
    curl_free(port);
    curl_url_cleanup(u);
    return key;
}

static HostSlot *host_slot(Share *share, const char *url) {
    char *key = host_key(url);
    if (key == NULL) {
        return NULL;
    }
    for (HostSlot *h = share->hosts; h != NULL; h = h->next) {
        if (strcmp(h->key, key) == 0) {
            free(key);
            return h;
        }
    }
    HostSlot *h = calloc(1, sizeof(HostSlot));
    if (h == NULL) {
        free(key);
        return NULL;
    }
    h->key = key;
    h->next = share->hosts;
    share->hosts = h;
    return h;
}

static bool slot_free(Share *share, HostSlot *h) {
    if (h->multiplexed && h->active > 0) {
        return true;
    }
    if (share->max_host_connections && h->active >= share->max_host_connections) {
        return false;
    }
    return !share->max_connections || share->connections < share->max_connections;
}

static void slot_acquire(Share *share, HostSlot *h) {
    if (!h->multiplexed || h->active == 0) {
        share->connections++;
    }
    h->active++;
}

static void slot_release(Share *share, HostSlot *h, bool multiplexed) {
    if (multiplexed && !h->multiplexed) {
        h->multiplexed = true;
        share->connections -= h->active - 1;
    }
    h->active--;
    if (!h->multiplexed || h->active == 0) {
        share->connections--;
    }
}

static PyObject *limiter_state(Share *share) {
    if (share == NULL) {
        Py_RETURN_NONE;
    }
    PyObject *hosts = PyDict_New();
    if (hosts == NULL) {
        return NULL;
    }
    for (HostSlot *h = share->hosts; h != NULL; h = h->next) {
        long waiting = 0;
        for (AcRequestData *rd = share->waiting_head; rd != NULL; rd = rd->next_waiting) {
            waiting += rd->slot == h;
        }
        PyObject *state = Py_BuildValue("{s:l,s:l,s:O}", "active", h->active, "waiting", waiting, "multiplexed",
                                        h->multiplexed ? Py_True : Py_False);
        if (state == NULL || PyDict_SetItemString(hosts, h->key, state) == -1) {
            Py_XDECREF(state);
            Py_DECREF(hosts);
            return NULL;
        }
        Py_DECREF(state);
    }
    return Py_BuildValue("{s:l,s:N}", "connections", share->connections, "hosts", hosts);
}

static void dispatch_waiting(EventLoop *loop, Share *share) {
    if (share->dispatching) {
        return;
    }
    share_incref(share);
    share->dispatching = true;
    bool started = true;
    while (started) {
        started = false;
        AcRequestData *prev = NULL;
        for (AcRequestData *rd = share->waiting_head; rd != NULL; prev = rd, rd = rd->next_waiting) {
            if (!slot_free(share, rd->slot)) {
                continue;
            }
            if (prev != NULL) {
                prev->next_waiting = rd->next_waiting;
            } else {
                share->waiting_head = rd->next_waiting;
            }
            if (share->waiting_tail == rd) {
                share->waiting_tail = prev;
            }
            rd->next_waiting = NULL;
            rd->waiting = false;
            rd->queue_time = monotonic_time() - rd->queued_at;
            slot_acquire(share, rd->slot);
            add_transfer(loop, rd);
            started = true;
            break;
        }
    }
    share->dispatching = false;
    share_decref(share);
}

static void release_request(EventLoop *loop, AcRequestData *rd) {
    curl_easy_setopt(rd->curl, CURLOPT_SHARE, NULL);
    curl_easy_setopt(rd->curl, CURLOPT_ERRORBUFFER, NULL);
    if (rd->slot != NULL) {
        long version = 0;
        if (rd->result == CURLE_OK) {
            curl_easy_getinfo(rd->curl, CURLINFO_HTTP_VERSION, &version);
        }
        slot_release(rd->limiter, rd->slot, version == CURL_HTTP_VERSION_2_0 || version == CURL_HTTP_VERSION_3);
        rd->slot = NULL;
        dispatch_waiting(loop, rd->limiter);
    }
    share_decref(rd->share);
    rd->share = NULL;
}

static void send_request(EventLoop *loop, AcRequestData *rd) {
    if (loop->unsent_head == NULL) {
        if (pipe_write(loop->req_out_write, &rd, sizeof(rd))) {
            return;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("httpperf: failed to return completed request");
            return;
        }
        if (aeCreateFileEvent(loop->event_loop, loop->req_out_write, AE_WRITABLE, send_completed, loop) == AE_ERR) {
            fprintf(stderr, "httpperf: failed to watch the completed request pipe\n");
        }
        loop->unsent_head = rd;
    } else {
        loop->unsent_tail->next_unsent = rd;
    }
    loop->unsent_tail = rd;
}

static void complete_request(EventLoop *loop, AcRequestData *rd) {
    release_request(loop, rd);
    int expected = REQUEST_RUNNING;
    if (atomic_compare_exchange_strong(&rd->state, &expected, REQUEST_SENT)) {
        send_request(loop, rd);
    } else {
        rd->finished = true;
    }
}

static void cancel_request(EventLoop *loop, AcRequestData *rd) {
    if (rd->finished) {
        send_request(loop, rd);
        return;
    }
    if (rd->waiting) {
        AcRequestData *prev = NULL;
        for (AcRequestData *w = rd->limiter->waiting_head; w != rd; prev = w, w = w->next_waiting) {
        }
        if (prev != NULL) {
            prev->next_waiting = rd->next_waiting;
        } else {
            rd->limiter->waiting_head = rd->next_waiting;
        }
        if (rd->limiter->waiting_tail == rd) {
            rd->limiter->waiting_tail = prev;
        }
        rd->next_waiting = NULL;
        rd->waiting = false;
        rd->slot = NULL;
    } else {
        check_multi(loop, curl_multi_remove_handle(loop->multi, rd->curl));
    }
    rd->result = CURLE_ABORTED_BY_CALLBACK;
    curl_slist_free_all(rd->headers);
    rd->headers = NULL;
    free(rd->req_data_buf);
    rd->req_data_buf = NULL;
    rd->req_data_len = 0;
    release_request(loop, rd);
    send_request(loop, rd);
}

static void send_completed(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    EventLoop *loop = (EventLoop *)clientData;
    while (loop->unsent_head != NULL) {
        AcRequestData *rd = loop->unsent_head;
        if (!pipe_write(loop->req_out_write, &rd, sizeof(rd))) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            perror("httpperf: failed to return completed request");
        }
        loop->unsent_head = rd->next_unsent;
        rd->next_unsent = NULL;
    }
    loop->unsent_tail = NULL;
    aeDeleteFileEvent(loop->event_loop, loop->req_out_write, AE_WRITABLE);
}

void response_complete(EventLoop *loop) {
    DEBUG_PRINT("loop=%p", loop);
    int remaining_in_queue = 1;
    AcRequestData *rd;
    CURLMsg *msg;
    while (remaining_in_queue > 0) {
        DEBUG_PRINT("calling curl_multi_info_read");
        msg = curl_multi_info_read(loop->multi, &remaining_in_queue);
        if (msg == NULL) {
            break;
        }
        curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (void **)&rd);
        check_multi(loop, curl_multi_remove_handle(loop->multi, rd->curl));
        rd->result = msg->data.result;
        curl_slist_free_all(rd->headers);
        rd->headers = NULL;
        free(rd->req_data_buf);
        rd->req_data_buf = NULL;
        rd->req_data_len = 0;

        DEBUG_PRINT("writing to req_out_write");
        REQUEST_TRACE_PRINT("response_complete", rd);
        complete_request(loop, rd);
    }
}

void socket_action_and_response_complete(EventLoop *loop, curl_socket_t socket, int ev_bitmask) {
    DEBUG_PRINT("loop=%p socket=%d ev_bitmask=%d", loop, socket, ev_bitmask);
    int running_handles;
    check_multi(loop, curl_multi_socket_action(loop->multi, socket, ev_bitmask, &running_handles));
    DEBUG_PRINT("after running_handlesr=%d", running_handles);
    response_complete(loop);
}

/* See docs for CURLOPT_HEADERFUNCTION */

static size_t header_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    AcRequestData *rd = (AcRequestData *)userdata;
    size_t n = size * nmemb;
    return buffer_append(&rd->header, ptr, n, n > 1024 ? n : 1024) ? n : 0;
}

/* See docs for CURLOPT_WTIE_FUNCTION */

size_t body_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    AcRequestData *rd = (AcRequestData *)userdata;
    size_t n = size * nmemb;
    size_t initial = n;
    if (rd->body.cap == 0) {
        curl_off_t length = -1;
        curl_easy_getinfo(rd->curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &length);
        if (length > (curl_off_t)n) {
            initial = length < MAX_BODY_PREALLOC ? (size_t)length : MAX_BODY_PREALLOC;
        }
    }
    return buffer_append(&rd->body, ptr, n, initial) ? n : 0;
}

void start_request(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    AcRequestData *rd;
    EventLoop *loop = (EventLoop *)clientData;
    if (pipe_read(loop->req_in_read, &rd, sizeof(rd)) != 1) {
        return;
    }
    if ((uintptr_t)rd & 1) {
        cancel_request(loop, (AcRequestData *)((uintptr_t)rd & ~(uintptr_t)1));
        return;
    }
    REQUEST_TRACE_PRINT("start_request", rd);
    DEBUG_PRINT("read AcRequestData");
    rd->curl = loop->free_handle_count ? loop->free_handles[--loop->free_handle_count] : curl_easy_init();
    curl_easy_setopt(rd->curl, CURLOPT_SHARE, rd->share->share);
    if (rd->session->fresh_connect) {
        curl_easy_setopt(rd->curl, CURLOPT_FRESH_CONNECT, 1L);
        curl_easy_setopt(rd->curl, CURLOPT_FORBID_REUSE, 1L);
    }
    curl_easy_setopt(rd->curl, CURLOPT_HTTP_VERSION, rd->session->http_version);
    curl_easy_setopt(rd->curl, CURLOPT_MAXAGE_CONN, rd->session->idle_timeout);
    curl_easy_setopt(rd->curl, CURLOPT_MAXLIFETIME_CONN, rd->session->max_connection_age);
    if (rd->session->http_version != CURL_HTTP_VERSION_1_1 && strncasecmp(rd->url, "https:", 6) == 0) {
        curl_easy_setopt(rd->curl, CURLOPT_PIPEWAIT, 1L);
    }
    if (strncasecmp(rd->url, "https:", 6) == 0) {
        curl_easy_setopt(rd->curl, CURLOPT_PROXY, rd->session->https_proxy ? rd->session->https_proxy : "");
        curl_easy_setopt(rd->curl, CURLOPT_NOPROXY, rd->session->no_proxy ? rd->session->no_proxy : "");
    } else if (strncasecmp(rd->url, "http:", 5) == 0) {
        curl_easy_setopt(rd->curl, CURLOPT_PROXY, rd->session->http_proxy ? rd->session->http_proxy : "");
        curl_easy_setopt(rd->curl, CURLOPT_NOPROXY, rd->session->no_proxy ? rd->session->no_proxy : "");
    }
    curl_easy_setopt(rd->curl, CURLOPT_COOKIEFILE, "");
    curl_easy_setopt(rd->curl, CURLOPT_URL, rd->url);
    curl_easy_setopt(rd->curl, CURLOPT_CUSTOMREQUEST, rd->method);
    if (strcmp(rd->method, "HEAD") == 0) {
        curl_easy_setopt(rd->curl, CURLOPT_NOBODY, 1L);
    }
    // curl_easy_setopt(rd->curl, CURLOPT_VERBOSE, 1L); //DEBUG
    curl_easy_setopt(rd->curl, CURLOPT_ENCODING, "");
    if (rd->headers != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_HTTPHEADER, rd->headers);
    }
    if (rd->auth != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_USERPWD, rd->auth);
    }
    if (rd->cert_file != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_SSLCERT, rd->cert_file);
        curl_easy_setopt(rd->curl, CURLOPT_SSLKEY, rd->key_file);
    }
    for (int i = 0; i < rd->cookies_len; i++) {
        DEBUG_PRINT("set cookie [%s]", rd->cookies_str[i]);
        curl_easy_setopt(rd->curl, CURLOPT_COOKIELIST, rd->cookies_str[i]);
    }
    if (rd->req_data_buf != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)rd->req_data_len);
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDS, (char *)rd->req_data_buf);
    }
    if (rd->verify) {
        if (rd->ca_file != NULL) {
            curl_easy_setopt(rd->curl, CURLOPT_CAINFO, rd->ca_file);
        }
    } else {
        curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    if (rd->timeout_ms > 0) {
        curl_easy_setopt(rd->curl, CURLOPT_TIMEOUT_MS, rd->timeout_ms);
    }
    if (rd->connect_timeout_ms > 0) {
        curl_easy_setopt(rd->curl, CURLOPT_CONNECTTIMEOUT_MS, rd->connect_timeout_ms);
    }
    curl_easy_setopt(rd->curl, CURLOPT_ERRORBUFFER, rd->error);
    curl_easy_setopt(rd->curl, CURLOPT_PRIVATE, rd);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEFUNCTION, body_callback);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEDATA, rd);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERDATA, rd);
    if (rd->share->max_host_connections || rd->share->max_connections) {
        rd->limiter = rd->share;
    } else if (loop->limiter != NULL && !rd->session->own_pool) {
        rd->limiter = loop->limiter;
    }
    if (rd->limiter != NULL) {
        rd->slot = host_slot(rd->limiter, rd->url);
    }
    free(rd->method);
    rd->method = NULL;
    free(rd->url);
    rd->url = NULL;
    if (rd->auth != NULL) {
        free(rd->auth);
        rd->auth = NULL;
    }
    free(rd->cert_file);
    rd->cert_file = NULL;
    free(rd->key_file);
    rd->key_file = NULL;
    free(rd->ca_file);
    rd->ca_file = NULL;
    free(rd->cookies_str);
    rd->cookies_str = NULL;
    if (rd->slot != NULL && !slot_free(rd->limiter, rd->slot)) {
        rd->queued_at = monotonic_time();
        rd->waiting = true;
        if (rd->limiter->waiting_tail != NULL) {
            rd->limiter->waiting_tail->next_waiting = rd;
        } else {
            rd->limiter->waiting_head = rd;
        }
        rd->limiter->waiting_tail = rd;
    } else {
        if (rd->slot != NULL) {
            slot_acquire(rd->limiter, rd->slot);
        }
        add_transfer(loop, rd);
    }
}

static void add_transfer(EventLoop *loop, AcRequestData *rd) {
    DEBUG_PRINT("adding handle");
    CURLMcode rc = curl_multi_add_handle(loop->multi, rd->curl);
    if (rc != CURLM_OK) {
        check_multi(loop, rc);
        rd->result = CURLE_FAILED_INIT;
        curl_slist_free_all(rd->headers);
        rd->headers = NULL;
        complete_request(loop, rd);
        response_complete(loop);
    }
}

void stop_eventloop(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    char buffer;
    EventLoop *loop = (EventLoop *)clientData;
    pipe_read(loop->stop_read, &buffer, sizeof(buffer));
    loop->stop = true;
}

/* Used to cleanup curl handles by putting the handle back on a pipe to be picked up and cleaned up */

void curl_easy_cleanup_in_eventloop(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    CleanupItem item;
    EventLoop *loop = (EventLoop *)clientData;
    while (pipe_read(fd, &item, sizeof(item)) == 1) {
        DEBUG_PRINT("curl=%p", item.curl);
        if (item.curl != NULL) {
            if (loop != NULL && loop->free_handle_count < HANDLE_POOL_SIZE) {
                curl_easy_reset(item.curl);
                loop->free_handles[loop->free_handle_count++] = item.curl;
            } else {
                curl_easy_cleanup(item.curl);
            }
        }
        if (item.share != NULL) {
            share_decref(item.share);
        }
    }
}

void socket_event(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    DEBUG_PRINT("eventloop=%p fd=%d clientData=%p mask=%d (readable=%d writable=%d)", eventLoop, fd, clientData, mask,
                mask & AE_READABLE, mask & AE_WRITABLE);
    int ev_bitmask = 0;
    if (mask & AE_READABLE) {
        ev_bitmask |= CURL_CSELECT_IN;
    }
    if (mask & AE_WRITABLE) {
        ev_bitmask |= CURL_CSELECT_OUT;
    }
    socket_action_and_response_complete((EventLoop *)clientData, (curl_socket_t)fd, ev_bitmask);
}

int socket_callback(CURL *easy, curl_socket_t s, int what, void *userp, void *socketp) {
    EventLoop *loop = (EventLoop *)userp;
    int rc = AE_OK;
    switch (what) {
    case CURL_POLL_NONE:
        DEBUG_PRINT("NONE socket=%d what=%d easy=%p", s, what, easy);
        // do nothing
        break;
    case CURL_POLL_IN:
        DEBUG_PRINT("IN socket=%d what=%d easy=%p", s, what, easy);
        rc = aeCreateFileEvent(loop->event_loop, (int)s, AE_READABLE, socket_event, (void *)loop);
        aeDeleteFileEvent(loop->event_loop, (int)s, AE_WRITABLE);
        break;
    case CURL_POLL_OUT:
        DEBUG_PRINT("OUT socket=%d what=%d easy=%p", s, what, easy);
        rc = aeCreateFileEvent(loop->event_loop, (int)s, AE_WRITABLE, socket_event, (void *)loop);
        aeDeleteFileEvent(loop->event_loop, (int)s, AE_READABLE);
        break;
    case CURL_POLL_INOUT:
        DEBUG_PRINT("INOUT socket=%d what=%d easy=%p", s, what, easy);
        rc = aeCreateFileEvent(loop->event_loop, (int)s, AE_READABLE | AE_WRITABLE, socket_event, (void *)loop);
        break;
    case CURL_POLL_REMOVE:
        DEBUG_PRINT("REMOVE socket=%d what=%d easy=%p", s, what, easy);
        aeDeleteFileEvent(loop->event_loop, (int)s, AE_READABLE | AE_WRITABLE);
        break;
    };
    return rc == AE_ERR ? -1 : 0;
}

int timeout(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    DEBUG_PRINT("");
    EventLoop *loop = (EventLoop *)clientData;
    loop->timer_id = NO_ACTIVE_TIMER_ID;
    socket_action_and_response_complete(loop, CURL_SOCKET_TIMEOUT, 0);
    return AE_NOMORE;
}

/* See docs for CURLMOPT_TIMERFUNCTION */

int timer_callback(CURLM *multi, long timeout_ms, void *userp) {
    DEBUG_PRINT("timeout_ms=%ld", timeout_ms);
    EventLoop *loop = (EventLoop *)userp;
    if (loop->timer_id != NO_ACTIVE_TIMER_ID) {
        DEBUG_PRINT("DELETE timer_id=%ld", loop->timer_id);
        aeDeleteTimeEvent(loop->event_loop, loop->timer_id);
        loop->timer_id = NO_ACTIVE_TIMER_ID;
    }
    if (timeout_ms >= 0) {
        if ((loop->timer_id = aeCreateTimeEvent(loop->event_loop, timeout_ms, timeout, userp, NULL)) == AE_ERR) {
            loop->timer_id = NO_ACTIVE_TIMER_ID;
            return -1;
        }
        DEBUG_PRINT("CREATE timer_id=%ld", loop->timer_id);
    }
    return 0;
}

static int make_pipe(int *read_fd, int *write_fd, bool nonblocking_read, bool nonblocking_write) {
    int fds[2];
    if (pipe(fds) == -1) {
        return -1;
    }
    if ((nonblocking_read && fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK) == -1) ||
        (nonblocking_write && fcntl(fds[1], F_SETFL, fcntl(fds[1], F_GETFL) | O_NONBLOCK) == -1)) {
        int saved_errno = errno;
        close(fds[0]);
        close(fds[1]);
        errno = saved_errno;
        return -1;
    }
    *read_fd = fds[0];
    *write_fd = fds[1];
    return 0;
}

static PyObject *EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    long max_host_connections = 0;
    long max_total_connections = 0;
    long max_idle_connections = 1000;
    static char *kwlist[] = {"max_host_connections", "max_total_connections", "max_idle_connections", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|lll", kwlist, &max_host_connections, &max_total_connections,
                                     &max_idle_connections)) {
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
    self->timer_id = NO_ACTIVE_TIMER_ID;
    self->req_in_read = self->req_in_write = -1;
    self->req_out_read = self->req_out_write = -1;
    self->stop_read = self->stop_write = -1;
    self->curl_easy_cleanup_read = self->curl_easy_cleanup_write = -1;

    self->event_loop = aeCreateEventLoop(200);
    if (self->event_loop == NULL) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
    if (make_pipe(&self->req_in_read, &self->req_in_write, true, false) == -1 ||
        make_pipe(&self->req_out_read, &self->req_out_write, true, true) == -1 ||
        make_pipe(&self->stop_read, &self->stop_write, false, false) == -1 ||
        make_pipe(&self->curl_easy_cleanup_read, &self->curl_easy_cleanup_write, true, true) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
    if (aeCreateFileEvent(self->event_loop, self->req_in_read, AE_READABLE, start_request, self) == AE_ERR ||
        aeCreateFileEvent(self->event_loop, self->stop_read, AE_READABLE, stop_eventloop, self) == AE_ERR ||
        aeCreateFileEvent(self->event_loop, self->curl_easy_cleanup_read, AE_READABLE, curl_easy_cleanup_in_eventloop,
                          self) == AE_ERR) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
    errno = 0;
    self->multi = curl_multi_init();
    if (self->multi == NULL) {
        if (errno == EMFILE || errno == ENFILE) {
            PyErr_SetFromErrno(PyExc_OSError);
        } else {
            PyErr_SetString(PyExc_RuntimeError, "curl_multi_init failed");
        }
        goto error;
    }
    curl_multi_setopt(self->multi, CURLMOPT_MAXCONNECTS, max_idle_connections);
    if (max_host_connections || max_total_connections) {
        self->limiter = share_new(false, false, max_host_connections, max_total_connections);
        if (self->limiter == NULL) {
            PyErr_NoMemory();
            goto error;
        }
    }
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETFUNCTION, socket_callback);
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETDATA, self);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERFUNCTION, timer_callback);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERDATA, self);
    return (PyObject *)self;

error:
    Py_DECREF(self);
    return NULL;
}

static void close_fd(int fd) {
    if (fd != -1) {
        close(fd);
    }
}

static void EventLoop_dealloc(EventLoop *self) {
    DEBUG_PRINT("response=%p", self);
    if (self->curl_easy_cleanup_read != -1) {
        curl_easy_cleanup_in_eventloop(NULL, self->curl_easy_cleanup_read, NULL, 0);
    }
    while (self->free_handle_count > 0) {
        curl_easy_cleanup(self->free_handles[--self->free_handle_count]);
    }
    if (self->multi != NULL) {
        curl_multi_cleanup(self->multi);
    }
    if (self->limiter != NULL) {
        share_decref(self->limiter);
    }
    if (self->event_loop != NULL) {
        aeDeleteEventLoop(self->event_loop);
    }
    close_fd(self->req_in_read);
    close_fd(self->req_in_write);
    close_fd(self->req_out_read);
    close_fd(self->req_out_write);
    close_fd(self->stop_read);
    close_fd(self->stop_write);
    close_fd(self->curl_easy_cleanup_read);
    close_fd(self->curl_easy_cleanup_write);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *EventLoop_main(EventLoop *self, PyObject *args) {
    DEBUG_PRINT("Started");
    self->running = true;
    self->thread_state = PyEval_SaveThread();
    do {
        DEBUG_PRINT("Start of aeProcessEvents");
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS);
        DEBUG_PRINT("End of aeProcessEvents");
    } while (!self->stop);
    curl_multi_cleanup(self->multi);
    self->multi = NULL;
    PyEval_RestoreThread(self->thread_state);
    self->running = false;
    DEBUG_PRINT("Ended");
    Py_RETURN_NONE;
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
    while (self->live_head != NULL) {
        AcRequestData *rd = self->live_head;
        self->live_head = rd->live_next;
        PyList_Append(futures, rd->future);
        Py_DECREF(rd->future);
        if (rd->slot != NULL) {
            rd->limiter->waiting_head = rd->limiter->waiting_tail = NULL;
        }
        if (rd->curl != NULL) {
            curl_easy_cleanup(rd->curl);
        }
        if (rd->share != NULL) {
            share_decref(rd->share);
        }
        if (rd->headers != NULL) {
            curl_slist_free_all(rd->headers);
        }
        free(rd->method);
        free(rd->url);
        free(rd->auth);
        free(rd->cert_file);
        free(rd->key_file);
        free(rd->ca_file);
        free(rd->cookies_str);
        Py_XDECREF(rd->cookies);
        free(rd->req_data_buf);
        free(rd->header.data);
        free(rd->body.data);
        Py_DECREF(rd->session);
        free(rd);
    }
    self->unsent_head = self->unsent_tail = NULL;
    return futures;
}

static PyObject *EventLoop_limiter_state(EventLoop *self, PyObject *args) { return limiter_state(self->limiter); }

static PyObject *EventLoop_stop(PyObject *self, PyObject *args) {
    ((EventLoop *)self)->closing = true;
    if (!pipe_write(((EventLoop *)self)->stop_write, "\0", 1)) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Get the out pipe */

static PyObject *Eventloop_get_out_fd(PyObject *self, PyObject *args) {
    DEBUG_PRINT("");
    return Py_BuildValue("i", ((EventLoop *)self)->req_out_read);
}

static PyObject *Eventloop_get_completed(PyObject *self, PyObject *args) {
    AcRequestData *rd;
    PyObject *list = PyList_New(0);
    int status;
    while ((status = pipe_read(((EventLoop *)self)->req_out_read, &rd, sizeof(rd))) == 1) {
        REQUEST_TRACE_PRINT("Eventloop_get_completed", rd);
        if (rd->live_prev != NULL) {
            rd->live_prev->live_next = rd->live_next;
        } else {
            ((EventLoop *)self)->live_head = rd->live_next;
        }
        if (rd->live_next != NULL) {
            rd->live_next->live_prev = rd->live_prev;
        }
        DEBUG_PRINT("read AcRequestData; address=%p", rd);
        PyObject *tuple = PyTuple_New(3);
        if (rd->result == CURLE_OK) {
            Response *response = PyObject_New(Response, (PyTypeObject *)&ResponseType);
            response->header = rd->header;
            response->body = rd->body;
            response->header_bytes = NULL;
            response->body_bytes = NULL;
            response->curl = rd->curl;
            response->session = rd->session;
            response->queue_time = rd->queue_time;

            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(tuple, 0, Py_None);
            PyTuple_SET_ITEM(tuple, 1, (PyObject *)response);
            PyTuple_SET_ITEM(tuple, 2, rd->future);
        } else {
            PyObject *error =
                Py_BuildValue("(is)", rd->result, rd->error[0] ? rd->error : curl_easy_strerror(rd->result));
            free(rd->header.data);
            free(rd->body.data);
            queue_cleanup(rd->session->loop, rd->curl, NULL);
            Py_DECREF(rd->session);

            PyTuple_SET_ITEM(tuple, 0, error);
            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(tuple, 1, Py_None);
            PyTuple_SET_ITEM(tuple, 2, rd->future);
        }
        PyList_Append(list, tuple);
        Py_DECREF(tuple);
        if (rd->req_data_buf != NULL) {
            free(rd->req_data_buf);
        }
        Py_XDECREF(rd->cookies);
        free(rd);
    }
    if (status == -1 && PyList_GET_SIZE(list) == 0) {
        Py_DECREF(list);
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    return list;
}

static PyMethodDef EventLoop_methods[] = {
    {"main", (PyCFunction)EventLoop_main, METH_NOARGS, "Run the event loop"},
    {"abandon", (PyCFunction)EventLoop_abandon, METH_NOARGS, "Free requests in flight once stopped"},
    {"stop", EventLoop_stop, METH_NOARGS, "Stop the event loop"},
    {"_limiter_state", (PyCFunction)EventLoop_limiter_state, METH_NOARGS, "The limiter's counts, for tests"},
    {"get_out_fd", Eventloop_get_out_fd, METH_NOARGS, "Get the outbound file dscriptor"},
    {"get_completed", Eventloop_get_completed, METH_NOARGS, "Get the user_object, response and error"},
    {NULL, NULL, 0, NULL}};

static PyMemberDef EventLoop_members[] = {{NULL}};

static PyTypeObject EventLoopType = {
    PyVarObject_HEAD_INIT(NULL, 0) "httpperf._httpperf.EventLoop", /* tp_name */
    sizeof(EventLoop),                                             /* tp_basicsize */
    0,                                                             /* tp_itemsize */
    (destructor)EventLoop_dealloc,                                 /* tp_dealloc */
    0,                                                             /* tp_print */
    0,                                                             /* tp_getattr */
    0,                                                             /* tp_setattr */
    0,                                                             /* tp_reserved */
    0,                                                             /* tp_repr */
    0,                                                             /* tp_as_number */
    0,                                                             /* tp_as_sequence */
    0,                                                             /* tp_as_mapping */
    0,                                                             /* tp_hash  */
    0,                                                             /* tp_call */
    0,                                                             /* tp_str */
    0,                                                             /* tp_getattro */
    0,                                                             /* tp_setattro */
    0,                                                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                                            /* tp_flags */
    "Event Loop Type",                                             /* tp_doc */
    0,                                                             /* tp_traverse */
    0,                                                             /* tp_clear */
    0,                                                             /* tp_richcompare */
    0,                                                             /* tp_weaklistoffset */
    0,                                                             /* tp_iter */
    0,                                                             /* tp_iternext */
    EventLoop_methods,                                             /* tp_methods */
    EventLoop_members,                                             /* tp_members */
    0,                                                             /* tp_getset */
    0,                                                             /* tp_base */
    0,                                                             /* tp_dict */
    0,                                                             /* tp_descr_get */
    0,                                                             /* tp_descr_set */
    0,                                                             /* tp_dictoffset */
    0,                                                             /* tp_init */
    0,                                                             /* tp_alloc */
    EventLoop_new,                                                 /* tp_new */
};

static char *getenv_dup(const char *name, const char *fallback) {
    const char *value = getenv(name);
    if (value == NULL && fallback != NULL) {
        value = getenv(fallback);
    }
    return value ? strdup(value) : NULL;
}

static PyObject *Session_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    Session *self;
    EventLoop *loop;

    const char *connections = "shared";
    const char *tls = "shared";
    const char *http_version = "HTTP/2";
    long idle_timeout = 118;
    long max_connection_age = 0;
    long max_host_connections = 0;
    long max_connections = 0;

    static char *kwlist[] = {"loop",
                             "connections",
                             "tls",
                             "http_version",
                             "idle_timeout",
                             "max_connection_age",
                             "max_host_connections",
                             "max_connections",
                             NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|sssllll", kwlist, &loop, &connections, &tls, &http_version,
                                     &idle_timeout, &max_connection_age, &max_host_connections, &max_connections)) {
        return NULL;
    }
    if ((PyObject *)loop == Py_None) {
        loop = NULL;
    } else if (!PyObject_TypeCheck(loop, &EventLoopType)) {
        PyErr_SetString(PyExc_TypeError, "loop must be an EventLoop or None");
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
    bool share_connections = strcmp(connections, "session") == 0;
    bool fresh_connect = strcmp(connections, "none") == 0;
    if (!share_connections && !fresh_connect && strcmp(connections, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "connections must be 'shared', 'session' or 'none', not '%s'", connections);
        return NULL;
    }
    if (!share_connections && !fresh_connect && (max_host_connections || max_connections)) {
        PyErr_SetString(PyExc_ValueError, "session connection limits need connections='session' or 'none'; "
                                          "set limits for the shared pool on the Engine");
        return NULL;
    }
    bool share_tls = strcmp(tls, "session") == 0;
    if (!share_tls && strcmp(tls, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "tls must be 'shared' or 'session', not '%s'", tls);
        return NULL;
    }
    long curl_http_version;
    if (strcmp(http_version, "HTTP/2") == 0) {
        curl_http_version = CURL_HTTP_VERSION_2TLS;
    } else if (strcmp(http_version, "HTTP/1.1") == 0) {
        curl_http_version = CURL_HTTP_VERSION_1_1;
    } else if (strcmp(http_version, "HTTP/3") == 0) {
        if (!(curl_version_info(CURLVERSION_NOW)->features & CURL_VERSION_HTTP3)) {
            PyErr_SetString(PyExc_ValueError, "this libcurl was built without HTTP/3");
            return NULL;
        }
        curl_http_version = CURL_HTTP_VERSION_3ONLY;
    } else {
        PyErr_Format(PyExc_ValueError, "http_version must be 'HTTP/3', 'HTTP/2' or 'HTTP/1.1', not '%s'", http_version);
        return NULL;
    }

    self = (Session *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }

    Py_XINCREF(loop);
    self->loop = loop;
    self->fresh_connect = fresh_connect;
    self->own_pool = share_connections;
    self->http_version = curl_http_version;
    self->idle_timeout = idle_timeout;
    self->max_connection_age = max_connection_age;
    self->http_proxy = getenv_dup("http_proxy", NULL);
    if (self->http_proxy == NULL) {
        self->http_proxy = getenv_dup("all_proxy", "ALL_PROXY");
    }
    self->https_proxy = getenv_dup("https_proxy", "HTTPS_PROXY");
    if (self->https_proxy == NULL) {
        self->https_proxy = getenv_dup("all_proxy", "ALL_PROXY");
    }
    self->no_proxy = getenv_dup("no_proxy", "NO_PROXY");
    self->shared = share_new(share_connections, share_tls, max_host_connections, max_connections);
    if (self->shared == NULL) {
        Py_DECREF(self);
        return PyErr_NoMemory();
    }
    return (PyObject *)self;
}

static void Session_dealloc(Session *self) {
    DEBUG_PRINT("response=%p", self);
    if (self->shared != NULL) {
        queue_cleanup(self->loop, NULL, self->shared);
    }
    free(self->http_proxy);
    free(self->https_proxy);
    free(self->no_proxy);
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

#define CHECK_ALLOC(p)          \
    do {                        \
        if ((p) == NULL) {      \
            PyErr_NoMemory();   \
            goto error_cleanup; \
        }                       \
    } while (0)

static int parse_timeout(PyObject *value, const char *name, long *ms) {
    *ms = 0;
    if (value == Py_None) {
        return 0;
    }
    double seconds = PyFloat_AsDouble(value);
    if (seconds == -1.0 && PyErr_Occurred()) {
        return -1;
    }
    if (!(seconds >= 0 && seconds <= 1e9)) {
        PyErr_Format(PyExc_ValueError, "%s must be between 0 and 1e9 seconds, or None", name);
        return -1;
    }
    *ms = (long)(seconds * 1000);
    if (seconds > 0 && *ms == 0) {
        *ms = 1;
    }
    return 0;
}

static PyObject *Session_request(Session *self, PyObject *args, PyObject *kwds) {
    char *method;
    char *url;
    PyObject *future;
    PyObject *headers;
    PyObject *auth;
    PyObject *cookies;
    Py_ssize_t req_data_len = 0;
    char *req_data_buf = NULL;
    PyObject *cert = Py_None;
    PyObject *verify = Py_False;
    PyObject *timeout = Py_None;
    PyObject *connect_timeout = Py_None;

    static char *kwlist[] = {"future", "method", "url",     "headers",         "auth", "cookies", "data",
                             "cert",   "verify", "timeout", "connect_timeout", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OssOOOz#|OOOO", kwlist, &future, &method, &url, &headers, &auth,
                                     &cookies, &req_data_buf, &req_data_len, &cert, &verify, &timeout,
                                     &connect_timeout)) {
        return NULL;
    }
    long timeout_ms, connect_timeout_ms;
    if (parse_timeout(timeout, "timeout", &timeout_ms) ||
        parse_timeout(connect_timeout, "connect_timeout", &connect_timeout_ms)) {
        return NULL;
    }

    if (self->shared == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "session is closed");
        return NULL;
    }
    if (self->loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "session has no engine");
        return NULL;
    }

    AcRequestData *rd = (AcRequestData *)calloc(1, sizeof(AcRequestData));
    if (rd == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    REQUEST_TRACE_PRINT("Session_request", rd);
    rd->timeout_ms = timeout_ms;
    rd->connect_timeout_ms = connect_timeout_ms;
    bool has_content_type = false;
    if (headers != Py_None) {
        if (!PyTuple_CheckExact(headers)) {
            PyErr_SetString(PyExc_ValueError, "headers should be a tuple of strings or None");
            goto error_cleanup;
        }
        for (int i = 0; i < PyTuple_GET_SIZE(headers); i++) {
            if (!PyUnicode_CheckExact(PyTuple_GET_ITEM(headers, i))) {
                PyErr_SetString(PyExc_ValueError, "headers should be a tuple of strings or None");
                goto error_cleanup;
            }
            const char *header = PyUnicode_AsUTF8(PyTuple_GET_ITEM(headers, i));
            if (header == NULL) {
                goto error_cleanup;
            }
            if (strncasecmp(header, "content-type", 12) == 0 && strchr(":; \t", header[12]) != NULL) {
                has_content_type = true;
            }
            struct curl_slist *appended = curl_slist_append(rd->headers, header);
            CHECK_ALLOC(appended);
            rd->headers = appended;
        }
    }
    if (req_data_buf != NULL && !has_content_type) {
        struct curl_slist *appended = curl_slist_append(rd->headers, "Content-Type:");
        CHECK_ALLOC(appended);
        rd->headers = appended;
    }
    if (auth != Py_None) {
        if (!PyTuple_CheckExact(auth) || PyTuple_GET_SIZE(auth) != 2 ||
            !PyUnicode_CheckExact(PyTuple_GET_ITEM(auth, 0)) || !PyUnicode_CheckExact(PyTuple_GET_ITEM(auth, 1))) {
            PyErr_SetString(PyExc_ValueError, "auth should be a tuple of strings (username, password) or None");
            goto error_cleanup;
        }
        const char *username = PyUnicode_AsUTF8(PyTuple_GET_ITEM(auth, 0));
        const char *password = PyUnicode_AsUTF8(PyTuple_GET_ITEM(auth, 1));
        if (username == NULL || password == NULL) {
            goto error_cleanup;
        }
        rd->auth = (char *)malloc(strlen(username) + 1 + strlen(password) + 1);
        CHECK_ALLOC(rd->auth);
        sprintf(rd->auth, "%s:%s", username, password);
    }
    if (cert != Py_None) {
        if (!PyTuple_CheckExact(cert) || PyTuple_GET_SIZE(cert) != 2 ||
            !PyUnicode_CheckExact(PyTuple_GET_ITEM(cert, 0)) || !PyUnicode_CheckExact(PyTuple_GET_ITEM(cert, 1))) {
            PyErr_SetString(PyExc_ValueError, "cert should be a tuple of strings (certfile, keyfile) or None");
            goto error_cleanup;
        }
        const char *cert_file = PyUnicode_AsUTF8(PyTuple_GET_ITEM(cert, 0));
        const char *key_file = PyUnicode_AsUTF8(PyTuple_GET_ITEM(cert, 1));
        if (cert_file == NULL || key_file == NULL) {
            goto error_cleanup;
        }
        rd->cert_file = strdup(cert_file);
        rd->key_file = strdup(key_file);
        CHECK_ALLOC(rd->cert_file);
        CHECK_ALLOC(rd->key_file);
    }
    if (verify == Py_True) {
        rd->verify = true;
    } else if (PyUnicode_CheckExact(verify)) {
        const char *ca_file = PyUnicode_AsUTF8(verify);
        if (ca_file == NULL) {
            goto error_cleanup;
        }
        rd->verify = true;
        rd->ca_file = strdup(ca_file);
        CHECK_ALLOC(rd->ca_file);
    } else if (verify != Py_False) {
        PyErr_SetString(PyExc_ValueError, "verify should be True, False or a CA bundle path");
        goto error_cleanup;
    }
    if (cookies != Py_None) {
        Py_INCREF(cookies);
        rd->cookies = cookies;
        if (!PyTuple_CheckExact(cookies)) {
            PyErr_SetString(PyExc_ValueError, "cookies should be a tuple of strings or None");
            goto error_cleanup;
        }
        rd->cookies_len = PyTuple_GET_SIZE(cookies);
        if (rd->cookies_len > 0) {
            rd->cookies_str = (char **)calloc(PyTuple_GET_SIZE(cookies), sizeof(char *));
            CHECK_ALLOC(rd->cookies_str);
            for (int i = 0; i < PyTuple_GET_SIZE(cookies); i++) {
                if (!PyUnicode_CheckExact(PyTuple_GET_ITEM(cookies, i))) {
                    PyErr_SetString(PyExc_ValueError, "cookies should be a tuple of strings or None");
                    goto error_cleanup;
                }
                rd->cookies_str[i] = (char *)PyUnicode_AsUTF8(PyTuple_GET_ITEM(cookies, i));
                if (rd->cookies_str[i] == NULL) {
                    goto error_cleanup;
                }
            }
        }
    }

    rd->method = strdup(method);
    rd->url = strdup(url);
    CHECK_ALLOC(rd->method);
    CHECK_ALLOC(rd->url);
    if (req_data_buf != NULL) {
        rd->req_data_buf = malloc(req_data_len + 1);
        CHECK_ALLOC(rd->req_data_buf);
        memcpy(rd->req_data_buf, req_data_buf, req_data_len);
    }
    rd->req_data_len = req_data_len;

    Py_INCREF(self);
    rd->session = self;
    Py_INCREF(future);
    rd->future = future;
    share_incref(self->shared);
    rd->share = self->shared;

    if (!pipe_write(self->loop->req_in_write, &rd, sizeof(rd))) {
        PyErr_SetFromErrno(PyExc_OSError);
        Py_CLEAR(rd->session);
        Py_CLEAR(rd->future);
        share_decref(rd->share);
        goto error_cleanup;
    }
    rd->live_next = self->loop->live_head;
    if (rd->live_next != NULL) {
        rd->live_next->live_prev = rd;
    }
    self->loop->live_head = rd;
    DEBUG_PRINT("scheduling request");
    return PyLong_FromVoidPtr(rd);

error_cleanup:
    if (rd->headers) {
        curl_slist_free_all(rd->headers);
    }
    if (rd->auth) {
        free(rd->auth);
    }
    free(rd->cert_file);
    free(rd->key_file);
    free(rd->ca_file);
    if (rd->cookies) {
        Py_DECREF(rd->cookies);
        free(rd->cookies_str);
    }
    free(rd->method);
    free(rd->url);
    free(rd->req_data_buf);
    free(rd);
    return NULL;
}

static PyObject *Session_limiter_state(Session *self, PyObject *args) {
    bool limited = self->shared != NULL && (self->shared->max_host_connections || self->shared->max_connections);
    return limiter_state(limited ? self->shared : NULL);
}

static PyObject *Session_cancel(Session *self, PyObject *handle) {
    AcRequestData *rd = PyLong_AsVoidPtr(handle);
    if (rd == NULL) {
        return NULL;
    }
    if (self->loop == NULL || self->loop->closing) {
        Py_RETURN_NONE;
    }
    int expected = REQUEST_RUNNING;
    if (atomic_compare_exchange_strong(&rd->state, &expected, REQUEST_CANCELLING)) {
        uintptr_t message = (uintptr_t)rd | 1;
        if (!pipe_write(self->loop->req_in_write, &message, sizeof(message))) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    Py_RETURN_NONE;
}

static PyObject *Session_close(Session *self, PyObject *args) {
    if (self->shared != NULL) {
        queue_cleanup(self->loop, NULL, self->shared);
        self->shared = NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *Session_attach(Session *self, PyObject *args) {
    EventLoop *loop;
    if (!PyArg_ParseTuple(args, "O!", &EventLoopType, &loop)) {
        return NULL;
    }
    if (self->loop != NULL && self->loop != loop) {
        PyErr_SetString(PyExc_RuntimeError, "session already has an engine");
        return NULL;
    }
    if (self->loop == NULL) {
        Py_INCREF(loop);
        self->loop = loop;
    }
    Py_RETURN_NONE;
}

static PyObject *Session_get_cookielist(Session *self, PyObject *args) { return session_cookielist(self); }

static PyObject *Session_apply_cookielist(Session *self, PyObject *args) {
    PyObject *lines;
    if (!PyArg_ParseTuple(args, "O!", &PyTuple_Type, &lines)) {
        return NULL;
    }
    if (self->shared == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "session is closed");
        return NULL;
    }
    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        return PyErr_NoMemory();
    }
    curl_easy_setopt(curl, CURLOPT_SHARE, self->shared->share);
    for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(lines); i++) {
        if (!PyUnicode_CheckExact(PyTuple_GET_ITEM(lines, i))) {
            PyErr_SetString(PyExc_ValueError, "cookie lines should be strings");
            curl_easy_cleanup(curl);
            return NULL;
        }
        const char *line = PyUnicode_AsUTF8(PyTuple_GET_ITEM(lines, i));
        if (line == NULL) {
            curl_easy_cleanup(curl);
            return NULL;
        }
        curl_easy_setopt(curl, CURLOPT_COOKIELIST, line);
    }
    curl_easy_cleanup(curl);
    Py_RETURN_NONE;
}

static PyMethodDef Session_methods[] = {
    {"request", (PyCFunction)Session_request, METH_VARARGS | METH_KEYWORDS, "Send a request"},
    {"get_cookielist", (PyCFunction)Session_get_cookielist, METH_NOARGS, "Get the session's cookies"},
    {"apply_cookielist", (PyCFunction)Session_apply_cookielist, METH_VARARGS, "Apply CURLOPT_COOKIELIST lines"},
    {"cancel", (PyCFunction)Session_cancel, METH_O, "Stop a request in flight"},
    {"_limiter_state", (PyCFunction)Session_limiter_state, METH_NOARGS, "The limiter's counts, for tests"},
    {"close", (PyCFunction)Session_close, METH_NOARGS, "Close the session's connections"},
    {"attach", (PyCFunction)Session_attach, METH_VARARGS, "Attach the session to an EventLoop"},
    {NULL, NULL, 0, NULL}};

static PyTypeObject SessionType = {
    PyVarObject_HEAD_INIT(NULL, 0) "httpperf._httpperf.Session", /* tp_name */
    sizeof(Session),                                             /* tp_basicsize */
    0,                                                           /* tp_itemsize */
    (destructor)Session_dealloc,                                 /* tp_dealloc */
    0,                                                           /* tp_print */
    0,                                                           /* tp_getattr */
    0,                                                           /* tp_setattr */
    0,                                                           /* tp_reserved */
    0,                                                           /* tp_repr */
    0,                                                           /* tp_as_number */
    0,                                                           /* tp_as_sequence */
    0,                                                           /* tp_as_mapping */
    0,                                                           /* tp_hash  */
    0,                                                           /* tp_call */
    0,                                                           /* tp_str */
    0,                                                           /* tp_getattro */
    0,                                                           /* tp_setattro */
    0,                                                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                                          /* tp_flags */
    "Session Type",                                              /* tp_doc */
    0,                                                           /* tp_traverse */
    0,                                                           /* tp_clear */
    0,                                                           /* tp_richcompare */
    0,                                                           /* tp_weaklistoffset */
    0,                                                           /* tp_iter */
    0,                                                           /* tp_iternext */
    Session_methods,                                             /* tp_methods */
    0,                                                           /* tp_members */
    0,                                                           /* tp_getset */
    0,                                                           /* tp_base */
    0,                                                           /* tp_dict */
    0,                                                           /* tp_descr_get */
    0,                                                           /* tp_descr_set */
    0,                                                           /* tp_dictoffset */
    0,                                                           /* tp_init */
    0,                                                           /* tp_alloc */
    Session_new,                                                 /* tp_new */
};

const static char MODULE_NAME[] = "_httpperf";

static PyObject *module_default_ca_file(PyObject *self, PyObject *args) {
#if LIBCURL_VERSION_NUM >= 0x075400
    CURL *curl = curl_easy_init();
    if (curl == NULL) {
        return PyErr_NoMemory();
    }
    char *path = NULL;
    curl_easy_getinfo(curl, CURLINFO_CAINFO, &path);
    PyObject *result = path != NULL ? PyUnicode_DecodeFSDefault(path) : Py_None;
    if (path == NULL) {
        Py_INCREF(Py_None);
    }
    curl_easy_cleanup(curl);
    return result;
#else
    Py_RETURN_NONE;
#endif
}

static PyMethodDef module_methods[] = {
    {"default_ca_file", module_default_ca_file, METH_NOARGS, "libcurl's built-in CA bundle path, or None"},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef _httpperf_module = {PyModuleDef_HEAD_INIT, MODULE_NAME, NULL, -1, module_methods};

PyMODINIT_FUNC PyInit__httpperf(void) {
    PyObject *m;

    if (PyType_Ready(&SessionType) < 0)
        return NULL;

    if (PyType_Ready(&EventLoopType) < 0)
        return NULL;

    if (PyType_Ready(&ResponseType) < 0)
        return NULL;

    m = PyModule_Create(&_httpperf_module);

    if (m != NULL) {
        curl_global_init(CURL_GLOBAL_ALL); // init curl library
        Py_INCREF(&SessionType);
        PyModule_AddObject(m, "Session", (PyObject *)&SessionType);
        Py_INCREF(&EventLoopType);
        PyModule_AddObject(m, "EventLoop", (PyObject *)&EventLoopType);
        Py_INCREF(&ResponseType);
        PyModule_AddObject(m, "Response", (PyObject *)&ResponseType);
    }

    return m;
}
