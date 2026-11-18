#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include "../src/ng.h"
#include <unistd.h>

#include "../include/hx.h"

#define CERT "build/hx/tls-cert.pem"
#define KEY "build/hx/tls-key.pem"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

static int server_port;
static SSL_CTX *server_ctx;
static int server_connections;
static int serving;

typedef struct {
    char path[64];
    const char *body;
    size_t len, at;
} stream;

static nghttp2_ssize send_body(nghttp2_session *ng, int32_t id, uint8_t *buf, size_t len, uint32_t *flags,
                               nghttp2_data_source *source, void *user) {
    stream *s = source->ptr;
    size_t n = s->len - s->at < len ? s->len - s->at : len;
    memcpy(buf, s->body + s->at, n);
    s->at += n;
    if (s->at == s->len) {
        *flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return (nghttp2_ssize)n;
}

static int begin_headers(nghttp2_session *ng, const nghttp2_frame *frame, void *user) {
    if (frame->hd.type == NGHTTP2_HEADERS) {
        nghttp2_session_set_stream_user_data(ng, frame->hd.stream_id, calloc(1, sizeof(stream)));
    }
    return 0;
}

static int on_header(nghttp2_session *ng, const nghttp2_frame *frame, const uint8_t *name, size_t namelen,
                     const uint8_t *value, size_t valuelen, uint8_t flags, void *user) {
    stream *s = nghttp2_session_get_stream_user_data(ng, frame->hd.stream_id);
    if (s != NULL && namelen == 5 && memcmp(name, ":path", 5) == 0 && valuelen < sizeof(s->path)) {
        memcpy(s->path, value, valuelen);
    }
    return 0;
}

static int on_frame(nghttp2_session *ng, const nghttp2_frame *frame, void *user) {
    stream *s = nghttp2_session_get_stream_user_data(ng, frame->hd.stream_id);
    if (s == NULL || !(frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
        return 0;
    }
    static char big[100000];
    memset(big, 'x', sizeof(big));
    if (strcmp(s->path, "/big") == 0) {
        s->body = big;
        s->len = sizeof(big);
    } else {
        s->body = "ok";
        s->len = 2;
    }
    nghttp2_nv status = {(uint8_t *)":status", (uint8_t *)"200", 7, 3, 0};
    nghttp2_data_provider2 body = {.source.ptr = s, .read_callback = send_body};
    nghttp2_submit_response2(ng, frame->hd.stream_id, &status, 1, &body);
    return 0;
}

static int on_close(nghttp2_session *ng, int32_t id, uint32_t code, void *user) {
    free(nghttp2_session_get_stream_user_data(ng, id));
    return 0;
}

static int choose_h2(SSL *ssl, const unsigned char **out, unsigned char *outlen, const unsigned char *in,
                     unsigned int inlen, void *arg) {
    for (unsigned int i = 0; i < inlen; i += 1 + in[i]) {
        if (in[i] == 2 && memcmp(in + i + 1, "h2", 2) == 0) {
            *out = in + i + 1;
            *outlen = 2;
            return SSL_TLSEXT_ERR_OK;
        }
    }
    return SSL_TLSEXT_ERR_NOACK;
}

static void *serve_one(void *arg) {
    int fd = (int)(intptr_t)arg;
    SSL *ssl = SSL_new(server_ctx);
    SSL_set_fd(ssl, fd);
    nghttp2_session_callbacks *cb;
    nghttp2_session_callbacks_new(&cb);
    nghttp2_session_callbacks_set_on_begin_headers_callback(cb, begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(cb, on_header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cb, on_frame);
    nghttp2_session_callbacks_set_on_stream_close_callback(cb, on_close);
    nghttp2_session *ng;
    nghttp2_session_server_new(&ng, cb, NULL);
    nghttp2_session_callbacks_del(cb);
    nghttp2_submit_settings(ng, NGHTTP2_FLAG_NONE, NULL, 0);
    if (SSL_accept(ssl) == 1) {
        __atomic_add_fetch(&server_connections, 1, __ATOMIC_SEQ_CST);
        for (;;) {
            const uint8_t *data;
            nghttp2_ssize n;
            while ((n = nghttp2_session_mem_send2(ng, &data)) > 0) {
                SSL_write(ssl, data, (int)n);
            }
            if (!nghttp2_session_want_read(ng) && !nghttp2_session_want_write(ng)) {
                break;
            }
            char buf[16384];
            int got = SSL_read(ssl, buf, sizeof(buf));
            if (got <= 0 || nghttp2_session_mem_recv2(ng, (const uint8_t *)buf, (size_t)got) < 0) {
                break;
            }
        }
    }
    nghttp2_session_del(ng);
    SSL_free(ssl);
    close(fd);
    __atomic_sub_fetch(&serving, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void *server(void *arg) {
    int listener = (int)(intptr_t)arg;
    for (;;) {
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            return NULL;
        }
        pthread_t t;
        __atomic_add_fetch(&serving, 1, __ATOMIC_SEQ_CST);
        pthread_create(&t, NULL, serve_one, (void *)(intptr_t)fd);
        pthread_detach(t);
    }
}

static void start_server(void) {
    server_ctx = SSL_CTX_new(TLS_server_method());
    if (SSL_CTX_use_certificate_chain_file(server_ctx, CERT) != 1 ||
        SSL_CTX_use_PrivateKey_file(server_ctx, KEY, SSL_FILETYPE_PEM) != 1) {
        printf("can't load %s and %s\n", CERT, KEY);
        exit(1);
    }
    SSL_CTX_set_alpn_select_cb(server_ctx, choose_h2, NULL);
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(listener, (struct sockaddr *)&addr, sizeof(addr));
    listen(listener, 64);
    socklen_t len = sizeof(addr);
    getsockname(listener, (struct sockaddr *)&addr, &len);
    server_port = ntohs(addr.sin_port);
    pthread_t t;
    pthread_create(&t, NULL, server, (void *)(intptr_t)listener);
    pthread_detach(t);
}

typedef struct {
    int epoll;
    int64_t deadline;
} loop;

static int64_t clock_ns(void *ctx) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static void watch(void *ctx, hx_socket s, int events) {
    loop *l = ctx;
    struct epoll_event ev = {.events = (events & HX_READ ? EPOLLIN : 0) | (events & HX_WRITE ? EPOLLOUT : 0),
                             .data.fd = s};
    if (events == 0) {
        epoll_ctl(l->epoll, EPOLL_CTL_DEL, s, NULL);
    } else if (epoll_ctl(l->epoll, EPOLL_CTL_MOD, s, &ev) == -1 && errno == ENOENT) {
        epoll_ctl(l->epoll, EPOLL_CTL_ADD, s, &ev);
    }
}

static void set_timer(void *ctx, int64_t deadline) { ((loop *)ctx)->deadline = deadline; }

typedef struct {
    int status;
    char body[16];
    size_t body_len;
    size_t body_total;
    int done;
    hx_event last;
} outcome;

static loop the_loop;

static hx_client *client(size_t read_size) {
    the_loop = (loop){.epoll = epoll_create1(0)};
    hx_config cfg = {.max_idle = 8, .read_size = read_size};
    return hx_client_new(&cfg, (hx_io){watch, set_timer, clock_ns, &the_loop, NULL});
}

static void run(hx_client *c, int wanted, int max_ms) {
    int64_t end = clock_ns(NULL) + (int64_t)max_ms * 1000000;
    int done = 0;
    while (done < wanted && clock_ns(NULL) < end) {
        hx_event events[16];
        int count;
        while ((count = hx_client_events(c, events, 16)) > 0) {
            for (int i = 0; i < count; i++) {
                outcome *mine = events[i].user;
                if (events[i].type == HX_EV_HEADERS) {
                    mine->status = events[i].status;
                } else if (events[i].type == HX_EV_BODY) {
                    size_t room = sizeof(mine->body) - mine->body_len;
                    size_t n = events[i].len < room ? events[i].len : room;
                    memcpy(mine->body + mine->body_len, events[i].data, n);
                    mine->body_len += n;
                    mine->body_total += events[i].len;
                } else if (events[i].type == HX_EV_DONE) {
                    mine->done++;
                    mine->last = events[i];
                    done++;
                }
            }
        }
        if (done >= wanted) {
            break;
        }
        struct epoll_event ready[16];
        int n = epoll_wait(the_loop.epoll, ready, 16, 10);
        for (int i = 0; i < n; i++) {
            hx_client_on_socket(c, ready[i].data.fd,
                                (ready[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR) ? HX_READ : 0) |
                                    (ready[i].events & EPOLLOUT ? HX_WRITE : 0));
        }
        if (the_loop.deadline > 0 && clock_ns(NULL) >= the_loop.deadline) {
            the_loop.deadline = 0;
            hx_client_on_timer(c);
        }
    }
}

static hx_request get(const char *target) {
    static char authority[32];
    snprintf(authority, sizeof(authority), "127.0.0.1:%d", server_port);
    return (hx_request){.method = "GET",
                        .host = "127.0.0.1",
                        .port = (uint16_t)server_port,
                        .authority = authority,
                        .target = target,
                        .tls = true,
                        .verify = true,
                        .ca_file = CERT,
                        .h2 = true};
}

static outcome fetch(hx_client *c, hx_request r) {
    outcome o = {0};
    hx_request_start(c, &r, &o);
    run(c, 1, 3000);
    return o;
}

static void finish_client(hx_client *c) {
    hx_client_free(c);
    close(the_loop.epoll);
}

static void test_a_request_offering_h2_uses_it(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/"));
    CHECK(o.last.error == HX_OK && o.last.version == 20 && o.status == 200);
    CHECK(o.body_len == 2 && memcmp(o.body, "ok", 2) == 0);
    finish_client(c);
}

static void test_requests_started_together_share_one_connection(void) {
    hx_client *c = client(0);
    int before = __atomic_load_n(&server_connections, __ATOMIC_SEQ_CST);
    outcome o[3] = {0};
    hx_request r = get("/");
    for (int i = 0; i < 3; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, 3, 3000);
    CHECK(o[0].last.error == HX_OK && o[1].last.error == HX_OK && o[2].last.error == HX_OK);
    CHECK(o[0].last.connection_id == o[1].last.connection_id && o[1].last.connection_id == o[2].last.connection_id);
    CHECK(__atomic_load_n(&server_connections, __ATOMIC_SEQ_CST) == before + 1);
    finish_client(c);
}

static void test_the_next_request_reuses_the_connection(void) {
    hx_client *c = client(0);
    outcome first = fetch(c, get("/"));
    outcome second = fetch(c, get("/"));
    CHECK(second.last.error == HX_OK && second.last.reused);
    CHECK(second.last.connection_id == first.last.connection_id);
    finish_client(c);
}

static void test_a_body_bigger_than_the_read_buffer_arrives_whole(void) {
    hx_client *c = client(1024);
    outcome o = fetch(c, get("/big"));
    CHECK(o.last.error == HX_OK && o.body_total == 100000);
    finish_client(c);
}

static void test_cancelling_one_stream_leaves_the_others(void) {
    hx_client *c = client(0);
    outcome a = {0}, b = {0};
    hx_request r = get("/");
    hx_req *qa = hx_request_start(c, &r, &a);
    hx_request_start(c, &r, &b);
    hx_request_cancel(qa);
    run(c, 2, 3000);
    outcome next = fetch(c, get("/"));
    CHECK(a.last.error == HX_ERR_CANCELLED && b.last.error == HX_OK);
    CHECK(next.last.error == HX_OK && next.last.connection_id == b.last.connection_id);
    finish_client(c);
}

static void test_freeing_the_client_with_requests_waiting_is_safe(void) {
    hx_client *c = client(0);
    outcome a = {0}, b = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &a);
    hx_request_start(c, &r, &b);
    finish_client(c);
    CHECK(b.done == 0);
}

static void test_freeing_the_client_with_streams_open_is_safe(void) {
    hx_client *c = client(0);
    fetch(c, get("/"));
    outcome a = {0}, b = {0};
    hx_request r = get("/big");
    hx_request_start(c, &r, &a);
    hx_request_start(c, &r, &b);
    finish_client(c);
    CHECK(b.done == 0);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    start_server();
    test_a_request_offering_h2_uses_it();
    test_requests_started_together_share_one_connection();
    test_the_next_request_reuses_the_connection();
    test_a_body_bigger_than_the_read_buffer_arrives_whole();
    test_cancelling_one_stream_leaves_the_others();
    test_freeing_the_client_with_requests_waiting_is_safe();
    test_freeing_the_client_with_streams_open_is_safe();
    int64_t end = clock_ns(NULL) + 3000000000;
    while (__atomic_load_n(&serving, __ATOMIC_SEQ_CST) > 0 && clock_ns(NULL) < end) {
        usleep(1000);
    }
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
