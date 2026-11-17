#ifndef HTTPPERF_PARSE_HEADERS_H
#define HTTPPERF_PARSE_HEADERS_H

#include <Python.h>
#include <stdbool.h>
#include <string.h>

static const char *find_crlf(const char *p, const char *end) {
    while (p < end && (p = memchr(p, '\r', end - p)) != NULL && p + 1 < end) {
        if (p[1] == '\n') {
            return p;
        }
        p++;
    }
    return NULL;
}

static const char *find_blank_line(const char *p, const char *end) {
    while ((p = find_crlf(p, end)) != NULL) {
        if (end - p >= 4 && p[2] == '\r' && p[3] == '\n') {
            return p;
        }
        p++;
    }
    return NULL;
}

static bool is_latin1_space(unsigned char c) {
    return (c >= 0x09 && c <= 0x0d) || (c >= 0x1c && c <= 0x20) || c == 0x85 || c == 0xa0;
}

static PyObject *header_pair(const char *line, const char *end) {
    const char *colon = memchr(line, ':', end - line);
    const char *value = colon != NULL ? colon + 1 : end;
    const char *value_end = end;
    while (value < value_end && is_latin1_space(*value)) {
        value++;
    }
    while (value_end > value && is_latin1_space(value_end[-1])) {
        value_end--;
    }
    PyObject *pair = PyTuple_New(2);
    if (pair == NULL) {
        return NULL;
    }
    PyObject *name_str = PyUnicode_DecodeLatin1(line, (colon != NULL ? colon : end) - line, NULL);
    if (name_str == NULL) {
        Py_DECREF(pair);
        return NULL;
    }
    PyTuple_SET_ITEM(pair, 0, name_str);
    PyObject *value_str = PyUnicode_DecodeLatin1(value, value_end - value, NULL);
    if (value_str == NULL) {
        Py_DECREF(pair);
        return NULL;
    }
    PyTuple_SET_ITEM(pair, 1, value_str);
    return pair;
}

static PyObject *module_parse_headers(PyObject *self, PyObject *arg) {
    if (!PyBytes_Check(arg)) {
        PyErr_SetString(PyExc_TypeError, "parse_headers expects bytes");
        return NULL;
    }
    const char *start = PyBytes_AS_STRING(arg);
    const char *end = start + PyBytes_GET_SIZE(arg);
    const char *block = NULL;
    const char *block_end = NULL;
    for (;;) {
        const char *blank = find_blank_line(start, end);
        const char *stop = blank != NULL ? blank : end;
        if (stop > start) {
            block = start;
            block_end = stop;
        }
        if (blank == NULL) {
            break;
        }
        start = blank + 4;
    }
    PyObject *pairs = PyList_New(0);
    if (pairs == NULL) {
        return NULL;
    }
    const char *line = block != NULL ? find_crlf(block, block_end) : NULL;
    while (line != NULL) {
        line += 2;
        const char *next = find_crlf(line, block_end);
        PyObject *pair = header_pair(line, next != NULL ? next : block_end);
        if (pair == NULL || PyList_Append(pairs, pair) == -1) {
            Py_XDECREF(pair);
            Py_DECREF(pairs);
            return NULL;
        }
        Py_DECREF(pair);
        line = next;
    }
    PyObject *result = PyList_AsTuple(pairs);
    Py_DECREF(pairs);
    return result;
}

#endif
