#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <unistd.h>

#ifndef Py_BEGIN_CRITICAL_SECTION
#define Py_BEGIN_CRITICAL_SECTION(op) {
#define Py_END_CRITICAL_SECTION() }
#endif

#define NOT_BUILT_IN 4

typedef struct {
    PyObject_HEAD int out[2];
    int wake[2];
    PyObject *completed;
    long requests;
} EventLoop;

typedef struct {
    PyObject_HEAD EventLoop *loop;
    bool closed;
    long requests;
} Session;

static PyTypeObject EventLoopType;

static int nonblocking_pipe(int fds[2]) {
    if (pipe(fds) == -1) {
        return -1;
    }
    if (fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK) == -1 || fcntl(fds[0], F_SETFD, FD_CLOEXEC) == -1 ||
        fcntl(fds[1], F_SETFD, FD_CLOEXEC) == -1) {
        close(fds[0]);
        close(fds[1]);
        return -1;
    }
    return 0;
}

static PyObject *stats_dict(long requests, PyObject *open) {
    return Py_BuildValue("{s:l,s:l,s:l,s:l,s:N,s:l,s:{s:i,s:i,s:i,s:i},s:i,s:i,s:i,s:i}", "requests", requests,
                         "errors", requests, "cancelled", 0L, "connections", 0L, "connections_open", open,
                         "tls_handshakes", 0L, "http_versions", "HTTP/1.0", 0, "HTTP/1.1", 0, "HTTP/2", 0, "HTTP/3", 0,
                         "bytes_sent", 0, "bytes_received", 0, "running", 0, "queued", 0);
}

static PyObject *EventLoop_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    int max_host_connections, max_total_connections, max_idle_connections, buffer_size, host_stats;
    if (!PyArg_ParseTuple(args, "iiiip", &max_host_connections, &max_total_connections, &max_idle_connections,
                          &buffer_size, &host_stats)) {
        return NULL;
    }
    EventLoop *self = (EventLoop *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    self->out[0] = self->out[1] = self->wake[0] = self->wake[1] = -1;
    self->completed = PyList_New(0);
    if (self->completed == NULL || nonblocking_pipe(self->out) == -1 || nonblocking_pipe(self->wake) == -1) {
        if (!PyErr_Occurred()) {
            PyErr_SetFromErrno(PyExc_OSError);
        }
        Py_DECREF(self);
        return NULL;
    }
    return (PyObject *)self;
}

static void EventLoop_dealloc(EventLoop *self) {
    for (int i = 0; i < 2; i++) {
        if (self->out[i] != -1) {
            close(self->out[i]);
        }
        if (self->wake[i] != -1) {
            close(self->wake[i]);
        }
    }
    Py_XDECREF(self->completed);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *EventLoop_main(EventLoop *self, PyObject *args) {
    struct timeval forever;
    fd_set readable;
    int ready;
    Py_BEGIN_ALLOW_THREADS;
    do {
        FD_ZERO(&readable);
        FD_SET(self->wake[0], &readable);
        forever = (struct timeval){3600, 0};
        ready = select(self->wake[0] + 1, &readable, NULL, NULL, &forever);
    } while (ready == 0 || (ready == -1 && errno == EINTR));
    Py_END_ALLOW_THREADS;
    Py_RETURN_NONE;
}

static PyObject *EventLoop_stop(EventLoop *self, PyObject *args) {
    char byte = 0;
    if (write(self->wake[1], &byte, 1) == -1 && errno != EAGAIN) {
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    Py_RETURN_NONE;
}

static PyObject *EventLoop_abandon(EventLoop *self, PyObject *args) { return PyList_New(0); }

static PyObject *EventLoop_stats(EventLoop *self, PyObject *args) {
    long requests;
    Py_BEGIN_CRITICAL_SECTION(self);
    requests = self->requests;
    Py_END_CRITICAL_SECTION();
    return stats_dict(requests, PyLong_FromLong(0));
}

static PyObject *EventLoop_host_stats(EventLoop *self, PyObject *args) { return PyDict_New(); }

static PyObject *EventLoop_limiter_state(EventLoop *self, PyObject *args) { Py_RETURN_NONE; }

static PyObject *EventLoop_get_out_fd(EventLoop *self, PyObject *args) { return PyLong_FromLong(self->out[0]); }

static PyObject *EventLoop_get_completed(EventLoop *self, PyObject *args) {
    char buffer[256];
    while (read(self->out[0], buffer, sizeof(buffer)) > 0) {
    }
    PyObject *empty = PyList_New(0);
    if (empty == NULL) {
        return NULL;
    }
    PyObject *completed;
    Py_BEGIN_CRITICAL_SECTION(self);
    completed = self->completed;
    self->completed = empty;
    Py_END_CRITICAL_SECTION();
    return completed;
}

static int complete(EventLoop *loop, PyObject *future) {
    PyObject *entry =
        Py_BuildValue("((is)OO)", NOT_BUILT_IN, "the hx backend can't send requests yet", Py_None, future);
    if (entry == NULL) {
        return -1;
    }
    int result;
    Py_BEGIN_CRITICAL_SECTION(loop);
    result = PyList_Append(loop->completed, entry);
    loop->requests++;
    Py_END_CRITICAL_SECTION();
    Py_DECREF(entry);
    char byte = 0;
    if (result == 0 && write(loop->out[1], &byte, 1) == -1 && errno != EAGAIN) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    return result;
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
    PyObject *loop, *connections, *tls, *http_version, *idle_timeout, *max_connection_age;
    int max_host_connections, max_connections;
    if (!PyArg_ParseTuple(args, "OOOOOOii", &loop, &connections, &tls, &http_version, &idle_timeout,
                          &max_connection_age, &max_host_connections, &max_connections)) {
        return NULL;
    }
    if (loop != Py_None && !PyObject_TypeCheck(loop, &EventLoopType)) {
        PyErr_SetString(PyExc_TypeError, "loop should be an EventLoop or None");
        return NULL;
    }
    Session *self = (Session *)type->tp_alloc(type, 0);
    if (self != NULL && loop != Py_None) {
        Py_INCREF(loop);
        self->loop = (EventLoop *)loop;
    }
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

static PyObject *Session_request(Session *self, PyObject *args, PyObject *kwds) {
    if (PyTuple_GET_SIZE(args) < 1) {
        PyErr_SetString(PyExc_TypeError, "request needs a future");
        return NULL;
    }
    PyObject *future = PyTuple_GET_ITEM(args, 0);
    EventLoop *loop = NULL;
    bool closed;
    Py_BEGIN_CRITICAL_SECTION(self);
    closed = self->closed;
    if (!closed && self->loop != NULL) {
        loop = self->loop;
        Py_INCREF(loop);
        self->requests++;
    }
    Py_END_CRITICAL_SECTION();
    if (closed) {
        PyErr_SetString(PyExc_RuntimeError, "session is closed");
        return NULL;
    }
    if (loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "session has no engine");
        return NULL;
    }
    int result = complete(loop, future);
    Py_DECREF(loop);
    if (result == -1) {
        return NULL;
    }
    PyObject *upload_size = kwds != NULL ? PyDict_GetItemString(kwds, "upload_size") : NULL;
    if (upload_size != NULL) {
        return Py_BuildValue("(iO)", 0, Py_None);
    }
    return PyLong_FromLong(0);
}

static PyObject *Session_cancel(Session *self, PyObject *handle) { Py_RETURN_NONE; }

static PyObject *Session_stats(Session *self, PyObject *args) {
    long requests;
    Py_BEGIN_CRITICAL_SECTION(self);
    requests = self->requests;
    Py_END_CRITICAL_SECTION();
    Py_INCREF(Py_None);
    return stats_dict(requests, Py_None);
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

static PyObject *module_default_ca_file(PyObject *module, PyObject *args) { Py_RETURN_NONE; }

static PyObject *module_upload_push(PyObject *module, PyObject *args) { Py_RETURN_NONE; }

static PyObject *module_upload_finish(PyObject *module, PyObject *upload) { Py_RETURN_NONE; }

static PyObject *module_upload_wait(PyObject *module, PyObject *args) { Py_RETURN_FALSE; }

static PyMethodDef module_methods[] = {
    {"default_ca_file", module_default_ca_file, METH_NOARGS, "The built-in CA bundle path, or None"},
    {"upload_push", module_upload_push, METH_VARARGS,
     "Copy what fits of the bytes into an upload; None once it's over"},
    {"upload_finish", module_upload_finish, METH_O, "Mark the end of an upload's body"},
    {"upload_wait", module_upload_wait, METH_VARARGS, "Resolve the future when an upload has room"},
    {NULL}};

static struct PyModuleDef hx_module = {PyModuleDef_HEAD_INIT, "httpperf._httpperf_hx", NULL, -1, module_methods};

PyMODINIT_FUNC PyInit__httpperf_hx(void) {
    if (PyType_Ready(&EventLoopType) < 0 || PyType_Ready(&SessionType) < 0) {
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
    if (PyModule_AddObject(m, "EventLoop", (PyObject *)&EventLoopType) < 0 ||
        PyModule_AddObject(m, "Session", (PyObject *)&SessionType) < 0 ||
        PyModule_AddIntConstant(m, "websockets", 0) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    return m;
}
