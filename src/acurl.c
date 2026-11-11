#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "ae/ae.h"
#include <curl/multi.h>
#include <pthread.h>
#include <unistd.h>
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
} EventLoop;

typedef struct {
    CURLSH *share;
    pthread_mutex_t locks[CURL_LOCK_DATA_LAST];
    atomic_int refs;
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

static Share *share_new(bool share_connections, bool share_tls) {
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
        fprintf(stderr, "acurl: curl_share_cleanup failed: %s\n", curl_share_strerror(rc));
        return;
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
    if (!pipe_write(loop->curl_easy_cleanup_write, &item, sizeof(item))) {
        if (curl != NULL) {
            curl_easy_cleanup(curl);
        }
        share_decref(share);
    }
}

typedef struct {
    PyObject_HEAD EventLoop *loop;
    Share *shared;
    bool fresh_connect;
} Session;

/* Node in a linked list structure. Used for piecing together sections of resposnes e.g. headers and body.
 * Possible optimisation to have a memory pool for buffer nodes so they aren't being malloc'ed all the time */

struct BufferNode {
    int len;
    char *buffer;
    struct BufferNode *next;
};

typedef struct {
    char *method;
    char *url;
    char *auth;
    PyObject *cookies;
    int cookies_len;
    char **cookies_str;
    PyObject *future;
    struct curl_slist *headers;
    Py_ssize_t req_data_len;
    char *req_data_buf;
    Session *session;
    CURL *curl;
    CURLcode result;
    struct BufferNode *header_buffer_head;
    struct BufferNode *header_buffer_tail;
    struct BufferNode *body_buffer_head;
    struct BufferNode *body_buffer_tail;
    int dummy;
} AcRequestData;

typedef struct {
    PyObject_HEAD struct BufferNode *header_buffer;
    struct BufferNode *body_buffer;
    Session *session;
    CURL *curl;
} Response;

void free_buffer_nodes(struct BufferNode *start) {
    struct BufferNode *node = start;
    while (node != NULL) {
        struct BufferNode *next = node->next;
        free(node->buffer);
        free(node);
        node = next;
    };
}

/* Python deallocator for Response Object. For GC */

static void Response_dealloc(Response *self) {
    DEBUG_PRINT("response=%p", self);
    free_buffer_nodes(self->header_buffer);
    free_buffer_nodes(self->body_buffer);
    share_incref(self->session->shared);
    queue_cleanup(self->session->loop, self->curl, self->session->shared);
    Py_XDECREF(self->session);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

PyObject *get_buffer_as_pylist(struct BufferNode *start) {
    int i = 0, len = 0;
    PyObject *list;
    struct BufferNode *node = start;
    while (node != NULL) {
        len++;
        node = node->next;
    };
    list = PyList_New(len);
    node = start;
    ;
    while (node != NULL) {
        PyList_SET_ITEM(list, i++, PyBytes_FromStringAndSize(node->buffer, node->len));
        node = node->next;
    };
    DEBUG_PRINT("list=%p", list);
    return list;
}

static PyObject *Response_get_header(Response *self, PyObject *args) {
    DEBUG_PRINT("");
    return get_buffer_as_pylist(self->header_buffer);
}

static PyObject *Response_get_body(Response *self, PyObject *args) {
    DEBUG_PRINT("");
    return get_buffer_as_pylist(self->body_buffer);
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

static PyObject *Response_get_cookielist(Response *self, PyObject *args) {
    struct curl_slist *start = NULL;
    struct curl_slist *node = NULL;
    int len = 0;
    int i = 0;
    PyObject *list = NULL;
    curl_easy_getinfo(self->curl, CURLINFO_COOKIELIST, &start);
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
    {"get_size_upload", (PyCFunction)Response_get_size_upload, METH_NOARGS, ""},
    {"get_size_download", (PyCFunction)Response_get_size_download, METH_NOARGS, ""},
    {"get_primary_ip", (PyCFunction)Response_get_primary_ip, METH_NOARGS, ""},
    {"get_cookielist", (PyCFunction)Response_get_cookielist, METH_NOARGS, ""},
    {"get_redirect_url", (PyCFunction)Response_get_redirect_url, METH_NOARGS, "Get the redirect URL or None"},
    {"get_header", (PyCFunction)Response_get_header, METH_NOARGS, "Get the header"},
    {"get_body", (PyCFunction)Response_get_body, METH_NOARGS, "Get the body"},
    {NULL, NULL, 0, NULL}};

static PyMemberDef Response_members[] = {{NULL}};

static PyTypeObject ResponseType = {
    PyVarObject_HEAD_INIT(NULL, 0) "_acurl.Response", /* tp_name */
    sizeof(Response),                                 /* tp_basicsize */
    0,                                                /* tp_itemsize */
    (destructor)Response_dealloc,                     /* tp_dealloc */
    0,                                                /* tp_print */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_reserved */
    0,                                                /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash  */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                               /* tp_flags */
    "Response Type",                                  /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    Response_methods,                                 /* tp_methods */
    Response_members,                                 /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    0,                                                /* tp_init */
    0,                                                /* tp_alloc */
    0,                                                /* tp_new */
};

/* When at least one request has completed, write completed responses onto completion queue*/

static void check_multi(EventLoop *loop, CURLMcode rc) {
    if (rc == CURLM_ABORTED_BY_CALLBACK) {
        int running_handles;
        fprintf(stderr, "acurl: event loop error, aborting all requests\n");
        curl_multi_perform(loop->multi, &running_handles);
    }
}

static void complete_request(EventLoop *loop, AcRequestData *rd) {
    if (!pipe_write(loop->req_out_write, &rd, sizeof(rd))) {
        perror("acurl: failed to return completed request");
    }
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
    struct BufferNode *node = (struct BufferNode *)malloc(sizeof(struct BufferNode));
    node->len = size * nmemb;
    node->buffer = (char *)malloc(node->len);
    memcpy(node->buffer, ptr, node->len);
    node->next = NULL;
    if (unlikely(rd->header_buffer_head == NULL)) {
        rd->header_buffer_head = node;
    }
    if (likely(rd->header_buffer_tail != NULL)) {
        rd->header_buffer_tail->next = node;
    }
    rd->header_buffer_tail = node;
    return node->len;
}

/* See docs for CURLOPT_WTIE_FUNCTION */

size_t body_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    AcRequestData *rd = (AcRequestData *)userdata;
    struct BufferNode *node = (struct BufferNode *)malloc(sizeof(struct BufferNode));
    node->len = size * nmemb;
    node->buffer = (char *)malloc(node->len);
    memcpy(node->buffer, ptr, node->len);
    node->next = NULL;
    if (unlikely(rd->body_buffer_head == NULL)) {
        rd->body_buffer_head = node;
    }
    if (likely(rd->body_buffer_tail != NULL)) {
        rd->body_buffer_tail->next = node;
    }
    rd->body_buffer_tail = node;
    return node->len;
}

void start_request(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    AcRequestData *rd;
    EventLoop *loop = (EventLoop *)clientData;
    if (pipe_read(loop->req_in_read, &rd, sizeof(rd)) != 1) {
        return;
    }
    REQUEST_TRACE_PRINT("start_request", rd);
    DEBUG_PRINT("read AcRequestData");
    rd->curl = curl_easy_init();
    curl_easy_setopt(rd->curl, CURLOPT_SHARE, rd->session->shared->share);
    if (rd->session->fresh_connect) {
        curl_easy_setopt(rd->curl, CURLOPT_FRESH_CONNECT, 1L);
        curl_easy_setopt(rd->curl, CURLOPT_FORBID_REUSE, 1L);
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
    for (int i = 0; i < rd->cookies_len; i++) {
        DEBUG_PRINT("set cookie [%s]", rd->cookies_str[i]);
        curl_easy_setopt(rd->curl, CURLOPT_COOKIELIST, rd->cookies_str[i]);
    }
    if (rd->req_data_buf != NULL) {
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)rd->req_data_len);
        curl_easy_setopt(rd->curl, CURLOPT_POSTFIELDS, (char *)rd->req_data_buf);
    }
    curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(rd->curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(rd->curl, CURLOPT_PRIVATE, rd);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEFUNCTION, body_callback);
    curl_easy_setopt(rd->curl, CURLOPT_WRITEDATA, rd);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(rd->curl, CURLOPT_HEADERDATA, rd);
    free(rd->method);
    rd->method = NULL;
    free(rd->url);
    rd->url = NULL;
    if (rd->auth != NULL) {
        free(rd->auth);
        rd->auth = NULL;
    }
    free(rd->cookies_str);
    if (rd->dummy) {
        rd->result = CURLE_OK;
        curl_slist_free_all(rd->headers);
        rd->headers = NULL;
        complete_request(loop, rd);
    } else {
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
    while (pipe_read(fd, &item, sizeof(item)) == 1) {
        DEBUG_PRINT("curl=%p", item.curl);
        if (item.curl != NULL) {
            curl_easy_cleanup(item.curl);
        }
        share_decref(item.share);
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

static int make_pipe(int *read_fd, int *write_fd, bool nonblocking_read) {
    int fds[2];
    if (pipe(fds) == -1) {
        return -1;
    }
    if (nonblocking_read && fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK) == -1) {
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
    EventLoop *self = (EventLoop *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    self->timer_id = NO_ACTIVE_TIMER_ID;
    self->req_in_read = self->req_in_write = -1;
    self->req_out_read = self->req_out_write = -1;
    self->stop_read = self->stop_write = -1;
    self->curl_easy_cleanup_read = self->curl_easy_cleanup_write = -1;

    self->multi = curl_multi_init();
    if (self->multi == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "curl_multi_init failed");
        goto error;
    }
    curl_multi_setopt(self->multi, CURLMOPT_MAXCONNECTS, 1000);
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETFUNCTION, socket_callback);
    curl_multi_setopt(self->multi, CURLMOPT_SOCKETDATA, self);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERFUNCTION, timer_callback);
    curl_multi_setopt(self->multi, CURLMOPT_TIMERDATA, self);

    self->event_loop = aeCreateEventLoop(200);
    if (self->event_loop == NULL) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
    if (make_pipe(&self->req_in_read, &self->req_in_write, true) == -1 ||
        make_pipe(&self->req_out_read, &self->req_out_write, true) == -1 ||
        make_pipe(&self->stop_read, &self->stop_write, false) == -1 ||
        make_pipe(&self->curl_easy_cleanup_read, &self->curl_easy_cleanup_write, true) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
    if (aeCreateFileEvent(self->event_loop, self->req_in_read, AE_READABLE, start_request, self) == AE_ERR ||
        aeCreateFileEvent(self->event_loop, self->stop_read, AE_READABLE, stop_eventloop, self) == AE_ERR ||
        aeCreateFileEvent(self->event_loop, self->curl_easy_cleanup_read, AE_READABLE, curl_easy_cleanup_in_eventloop,
                          NULL) == AE_ERR) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto error;
    }
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
        curl_easy_cleanup_in_eventloop(NULL, self->curl_easy_cleanup_read, self, 0);
    }
    if (self->multi != NULL) {
        curl_multi_cleanup(self->multi);
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

static PyObject *EventLoop_once(EventLoop *self, PyObject *args) {
    aeProcessEvents(self->event_loop, AE_ALL_EVENTS | AE_DONT_WAIT);
    Py_RETURN_NONE;
}

static PyObject *EventLoop_main(EventLoop *self, PyObject *args) {
    DEBUG_PRINT("Started");
    self->thread_state = PyEval_SaveThread();
    do {
        DEBUG_PRINT("Start of aeProcessEvents");
        aeProcessEvents(self->event_loop, AE_ALL_EVENTS);
        DEBUG_PRINT("End of aeProcessEvents");
    } while (!self->stop);
    PyEval_RestoreThread(self->thread_state);
    DEBUG_PRINT("Ended");
    Py_RETURN_NONE;
}

static PyObject *EventLoop_stop(PyObject *self, PyObject *args) {
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
        DEBUG_PRINT("read AcRequestData; address=%p", rd);
        PyObject *tuple = PyTuple_New(3);
        if (rd->result == CURLE_OK) {
            Response *response = PyObject_New(Response, (PyTypeObject *)&ResponseType);
            response->header_buffer = rd->header_buffer_head;
            response->body_buffer = rd->body_buffer_head;
            response->curl = rd->curl;
            response->session = rd->session;

            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(tuple, 0, Py_None);
            PyTuple_SET_ITEM(tuple, 1, (PyObject *)response);
            PyTuple_SET_ITEM(tuple, 2, rd->future);
        } else {
            PyObject *error = PyUnicode_FromString(curl_easy_strerror(rd->result));
            free_buffer_nodes(rd->header_buffer_head);
            free_buffer_nodes(rd->body_buffer_head);
            share_incref(rd->session->shared);
            queue_cleanup(rd->session->loop, rd->curl, rd->session->shared);
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
    {"once", (PyCFunction)EventLoop_once, METH_NOARGS, "Run the event loop once"},
    {"stop", EventLoop_stop, METH_NOARGS, "Stop the event loop"},
    {"get_out_fd", Eventloop_get_out_fd, METH_NOARGS, "Get the outbound file dscriptor"},
    {"get_completed", Eventloop_get_completed, METH_NOARGS, "Get the user_object, response and error"},
    {NULL, NULL, 0, NULL}};

static PyMemberDef EventLoop_members[] = {{NULL}};

static PyTypeObject EventLoopType = {
    PyVarObject_HEAD_INIT(NULL, 0) "acurl.EventLoop", /* tp_name */
    sizeof(EventLoop),                                /* tp_basicsize */
    0,                                                /* tp_itemsize */
    (destructor)EventLoop_dealloc,                    /* tp_dealloc */
    0,                                                /* tp_print */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_reserved */
    0,                                                /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash  */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                               /* tp_flags */
    "Event Loop Type",                                /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    EventLoop_methods,                                /* tp_methods */
    EventLoop_members,                                /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    0,                                                /* tp_init */
    0,                                                /* tp_alloc */
    EventLoop_new,                                    /* tp_new */
};

static PyObject *Session_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    Session *self;
    EventLoop *loop;

    const char *connections = "shared";
    const char *tls = "shared";

    static char *kwlist[] = {"loop", "connections", "tls", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|ss", kwlist, &loop, &connections, &tls)) {
        return NULL;
    }
    bool share_connections = strcmp(connections, "session") == 0;
    bool fresh_connect = strcmp(connections, "none") == 0;
    if (!share_connections && !fresh_connect && strcmp(connections, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "connections must be 'shared', 'session' or 'none', not '%s'", connections);
        return NULL;
    }
    bool share_tls = strcmp(tls, "session") == 0;
    if (!share_tls && strcmp(tls, "shared") != 0) {
        PyErr_Format(PyExc_ValueError, "tls must be 'shared' or 'session', not '%s'", tls);
        return NULL;
    }

    self = (Session *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }

    Py_INCREF(loop);
    self->loop = loop;
    self->fresh_connect = fresh_connect;
    self->shared = share_new(share_connections, share_tls);
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

static PyObject *Session_request(Session *self, PyObject *args, PyObject *kwds) {
    char *method;
    char *url;
    PyObject *future;
    PyObject *headers;
    PyObject *auth;
    PyObject *cookies;
    Py_ssize_t req_data_len = 0;
    char *req_data_buf = NULL;
    int dummy;

    static char *kwlist[] = {"future", "method", "url", "headers", "auth", "cookies", "data", "dummy", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OssOOOz#p", kwlist, &future, &method, &url, &headers, &auth, &cookies,
                                     &req_data_buf, &req_data_len, &dummy)) {
        return NULL;
    }

    AcRequestData *rd = (AcRequestData *)calloc(1, sizeof(AcRequestData));
    if (rd == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    REQUEST_TRACE_PRINT("Session_request", rd);
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
            struct curl_slist *appended = curl_slist_append(rd->headers, header);
            CHECK_ALLOC(appended);
            rd->headers = appended;
        }
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
    rd->dummy = dummy;

    Py_INCREF(self);
    rd->session = self;
    Py_INCREF(future);
    rd->future = future;

    if (!pipe_write(self->loop->req_in_write, &rd, sizeof(rd))) {
        PyErr_SetFromErrno(PyExc_OSError);
        Py_CLEAR(rd->session);
        Py_CLEAR(rd->future);
        goto error_cleanup;
    }
    DEBUG_PRINT("scheduling request");
    Py_RETURN_NONE;

error_cleanup:
    if (rd->headers) {
        curl_slist_free_all(rd->headers);
    }
    if (rd->auth) {
        free(rd->auth);
    }
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

static PyMethodDef Session_methods[] = {
    {"request", (PyCFunction)Session_request, METH_VARARGS | METH_KEYWORDS, "Send a request"}, {NULL, NULL, 0, NULL}};

static PyTypeObject SessionType = {
    PyVarObject_HEAD_INIT(NULL, 0) "acurl.Session", /* tp_name */
    sizeof(Session),                                /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Session_dealloc,                    /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_reserved */
    0,                                              /* tp_repr */
    0,                                              /* tp_as_number */
    0,                                              /* tp_as_sequence */
    0,                                              /* tp_as_mapping */
    0,                                              /* tp_hash  */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    0,                                              /* tp_getattro */
    0,                                              /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                             /* tp_flags */
    "Session Type",                                 /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Session_methods,                                /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    0,                                              /* tp_init */
    0,                                              /* tp_alloc */
    Session_new,                                    /* tp_new */
};

const static char MODULE_NAME[] = "_acurl";

static PyMethodDef module_methods[] = {{NULL, NULL, 0, NULL}};

static struct PyModuleDef _acurl_module = {PyModuleDef_HEAD_INIT, MODULE_NAME, NULL, -1, module_methods};

PyMODINIT_FUNC PyInit__acurl(void) {
    PyObject *m;

    if (PyType_Ready(&SessionType) < 0)
        return NULL;

    if (PyType_Ready(&EventLoopType) < 0)
        return NULL;

    if (PyType_Ready(&ResponseType) < 0)
        return NULL;

    m = PyModule_Create(&_acurl_module);

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
