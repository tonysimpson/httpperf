#include "h1.h"

#include <stdio.h>
#include <strings.h>

static bool has_header(const hx_h1_request *r, const char *name) {
    size_t n = strlen(name);
    for (size_t i = 0; i < r->header_count; i++) {
        if (strncasecmp(r->headers[i], name, n) == 0 && r->headers[i][n] == ':') {
            return true;
        }
    }
    return false;
}

static bool clean(const char *s) { return s[strcspn(s, "\r\n")] == '\0'; }

int hx_h1_write_request(hx_buf *out, const hx_h1_request *r) {
    if (!clean(r->method) || !clean(r->target) || !clean(r->host)) {
        return HX_EINVAL;
    }
    for (size_t i = 0; i < r->header_count; i++) {
        if (!clean(r->headers[i])) {
            return HX_EINVAL;
        }
    }
    bool ok = hx_buf_append_str(out, r->method) && hx_buf_append(out, " ", 1) && hx_buf_append_str(out, r->target) &&
              hx_buf_append(out, " HTTP/1.1\r\n", 11);
    if (ok && !has_header(r, "host")) {
        ok = hx_buf_append(out, "Host: ", 6) && hx_buf_append_str(out, r->host) && hx_buf_append(out, "\r\n", 2);
    }
    for (size_t i = 0; ok && i < r->header_count; i++) {
        ok = hx_buf_append_str(out, r->headers[i]) && hx_buf_append(out, "\r\n", 2);
    }
    if (ok && !has_header(r, "content-length") && !has_header(r, "transfer-encoding")) {
        char line[48];
        if (r->body != NULL || r->upload_size >= 0) {
            int n = snprintf(line, sizeof(line), "Content-Length: %lld\r\n",
                             (long long)(r->body != NULL ? (int64_t)r->body_len : r->upload_size));
            ok = hx_buf_append(out, line, (size_t)n);
        } else if (r->upload_size == HX_H1_CHUNKED) {
            ok = hx_buf_append_str(out, "Transfer-Encoding: chunked\r\n");
        }
    }
    ok = ok && hx_buf_append(out, "\r\n", 2);
    if (ok && r->body != NULL) {
        ok = hx_buf_append(out, r->body, r->body_len);
    }
    return ok ? 0 : HX_ENOMEM;
}

int hx_h1_write_chunk(hx_buf *out, const char *data, size_t len) {
    char size[24];
    int n = snprintf(size, sizeof(size), "%zx\r\n", len);
    bool ok = hx_buf_append(out, size, (size_t)n) && hx_buf_append(out, data, len) && hx_buf_append(out, "\r\n", 2);
    return ok ? 0 : HX_ENOMEM;
}

static hx_h1 *of(llhttp_t *p) { return (hx_h1 *)p->data; }

static int push(hx_h1 *h, hx_h1_event_type type, const char *data, size_t len) {
    h->events[h->count++] = (hx_h1_event){type, data, len};
    return h->count == h->max ? HPE_PAUSED : 0;
}

static int append(hx_h1 *h, const char *data, size_t len) { return hx_buf_append(&h->head, data, len) ? 0 : -1; }

static int start_line(hx_h1 *h) {
    if (h->line_started) {
        return 0;
    }
    h->line_started = true;
    char line[32];
    hx_text t = hx_text_on(line, sizeof(line));
    hx_text_str(&t, "HTTP/");
    hx_text_uint(&t, h->parser.http_major, 0);
    hx_text_str(&t, ".");
    hx_text_uint(&t, h->parser.http_minor, 0);
    hx_text_str(&t, " ");
    hx_text_uint(&t, h->parser.status_code, 3);
    hx_text_str(&t, " ");
    return append(h, line, t.len);
}

static int on_message_begin(llhttp_t *p) {
    hx_h1 *h = of(p);
    h->head.len = 0;
    h->line_started = false;
    return 0;
}

static int on_status(llhttp_t *p, const char *at, size_t len) {
    hx_h1 *h = of(p);
    return start_line(h) || append(h, at, len);
}

static int on_status_complete(llhttp_t *p) {
    hx_h1 *h = of(p);
    return start_line(h) || append(h, "\r\n", 2);
}

static int on_header_field(llhttp_t *p, const char *at, size_t len) { return append(of(p), at, len); }

static int on_header_field_complete(llhttp_t *p) { return append(of(p), ": ", 2); }

static int on_header_value(llhttp_t *p, const char *at, size_t len) { return append(of(p), at, len); }

static int on_header_value_complete(llhttp_t *p) { return append(of(p), "\r\n", 2); }

static int on_headers_complete(llhttp_t *p) {
    hx_h1 *h = of(p);
    if (append(h, "\r\n", 2)) {
        return -1;
    }
    if (p->status_code < 200 && p->status_code != 101) {
        return 0;
    }
    h->status = p->status_code;
    h->version = p->http_major * 10 + p->http_minor;
    if (push(h, HX_H1_HEADERS, h->head.data, h->head.len) == HPE_PAUSED) {
        llhttp_pause(p);
    }
    return h->head_request ? 1 : 0;
}

static int on_body(llhttp_t *p, const char *at, size_t len) { return push(of(p), HX_H1_BODY, at, len); }

static int on_message_complete(llhttp_t *p) {
    hx_h1 *h = of(p);
    if (p->status_code < 200 && p->status_code != 101) {
        return 0;
    }
    h->keep_alive = llhttp_should_keep_alive(p);
    push(h, HX_H1_DONE, NULL, 0);
    return HPE_PAUSED;
}

void hx_h1_init(hx_h1 *h, bool head_request) {
    *h = (hx_h1){0};
    llhttp_settings_init(&h->settings);
    h->settings.on_message_begin = on_message_begin;
    h->settings.on_status = on_status;
    h->settings.on_status_complete = on_status_complete;
    h->settings.on_header_field = on_header_field;
    h->settings.on_header_field_complete = on_header_field_complete;
    h->settings.on_header_value = on_header_value;
    h->settings.on_header_value_complete = on_header_value_complete;
    h->settings.on_headers_complete = on_headers_complete;
    h->settings.on_body = on_body;
    h->settings.on_message_complete = on_message_complete;
    llhttp_init(&h->parser, HTTP_RESPONSE, &h->settings);
    h->parser.data = h;
    h->head_request = head_request;
}

void hx_h1_free(hx_h1 *h) { hx_buf_free(&h->head); }

void hx_h1_next(hx_h1 *h, bool head_request) { h->head_request = head_request; }

int hx_h1_feed(hx_h1 *h, const char *data, size_t len, size_t *used, hx_h1_event *events, int max) {
    h->events = events;
    h->count = 0;
    h->max = max;
    llhttp_errno_t err = llhttp_execute(&h->parser, data, len);
    if (err == HPE_OK) {
        *used = len;
        return h->count;
    }
    if (err == HPE_PAUSED || err == HPE_PAUSED_UPGRADE) {
        *used = (size_t)(llhttp_get_error_pos(&h->parser) - data);
        llhttp_resume(&h->parser);
        return h->count;
    }
    *used = 0;
    return HX_EPROTO;
}

int hx_h1_finish(hx_h1 *h, hx_h1_event *events, int max) {
    h->events = events;
    h->count = 0;
    h->max = max;
    llhttp_errno_t err = llhttp_finish(&h->parser);
    return err == HPE_OK || err == HPE_PAUSED ? h->count : HX_EPROTO;
}

const char *hx_h1_error(const hx_h1 *h) { return llhttp_get_error_reason(&h->parser); }
