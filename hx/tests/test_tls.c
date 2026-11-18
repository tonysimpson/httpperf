#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../include/hx.h"

#define CERT "build/hx/tls-cert.pem"
#define KEY "build/hx/tls-key.pem"
#define OTHER_CERT "build/hx/tls-other-cert.pem"
#define OTHER_KEY "build/hx/tls-other-key.pem"

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

static void reply(SSL *ssl, const char *body, size_t len) {
    char head[96];
    int n = snprintf(head, sizeof(head), "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n", len);
    SSL_write(ssl, head, n);
    for (size_t at = 0; at < len;) {
        int w = SSL_write(ssl, body + at, (int)(len - at));
        if (w <= 0) {
            return;
        }
        at += (size_t)w;
    }
}

static void *serve_one(void *arg) {
    int fd = (int)(intptr_t)arg;
    SSL *ssl = SSL_new(server_ctx);
    SSL_set_fd(ssl, fd);
    if (SSL_accept(ssl) == 1) {
        char buf[4096];
        size_t len = 0;
        for (;;) {
            int n = SSL_read(ssl, buf + len, (int)(sizeof(buf) - 1 - len));
            if (n <= 0) {
                break;
            }
            len += (size_t)n;
            buf[len] = '\0';
            if (strstr(buf, "\r\n\r\n") == NULL) {
                continue;
            }
            if (strstr(buf, " /reused ") != NULL) {
                reply(ssl, SSL_session_reused(ssl) ? "1" : "0", 1);
            } else if (strstr(buf, " /big ") != NULL) {
                static char big[100000];
                memset(big, 'x', sizeof(big));
                reply(ssl, big, sizeof(big));
            } else {
                reply(ssl, "ok", 2);
            }
            len = 0;
        }
    }
    SSL_free(ssl);
    close(fd);
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

static int proxy_port;
static bool proxy_refuses;
static char proxy_saw[1024];

static int listen_any(int *port) {
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(listener, (struct sockaddr *)&addr, sizeof(addr));
    listen(listener, 64);
    socklen_t len = sizeof(addr);
    getsockname(listener, (struct sockaddr *)&addr, &len);
    *port = ntohs(addr.sin_port);
    return listener;
}

static void relay(int a, int b) {
    char buf[16384];
    struct pollfd p[2] = {{.fd = a, .events = POLLIN}, {.fd = b, .events = POLLIN}};
    while (poll(p, 2, 5000) > 0) {
        for (int i = 0; i < 2; i++) {
            if (p[i].revents == 0) {
                continue;
            }
            ssize_t n = recv(p[i].fd, buf, sizeof(buf), 0);
            if (n <= 0 || send(p[1 - i].fd, buf, (size_t)n, MSG_NOSIGNAL) != n) {
                return;
            }
        }
    }
}

static void *proxy_one(void *arg) {
    int fd = (int)(intptr_t)arg;
    char head[1024];
    size_t len = 0;
    while (len < sizeof(head) - 1) {
        ssize_t n = recv(fd, head + len, sizeof(head) - 1 - len, 0);
        if (n <= 0) {
            break;
        }
        len += (size_t)n;
        head[len] = '\0';
        if (strstr(head, "\r\n\r\n") != NULL) {
            break;
        }
    }
    head[len] = '\0';
    snprintf(proxy_saw, sizeof(proxy_saw), "%s", head);
    if (proxy_refuses) {
        const char *r = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
        send(fd, r, strlen(r), MSG_NOSIGNAL);
    } else {
        int up = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr = {
            .sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = htons((uint16_t)server_port)};
        if (connect(up, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            const char *r = "HTTP/1.1 200 Connection established\r\n\r\n";
            send(fd, r, strlen(r), MSG_NOSIGNAL);
            relay(fd, up);
        }
        close(up);
    }
    close(fd);
    return NULL;
}

static void *proxy(void *arg) {
    int listener = (int)(intptr_t)arg;
    for (;;) {
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            return NULL;
        }
        pthread_t t;
        pthread_create(&t, NULL, proxy_one, (void *)(intptr_t)fd);
        pthread_detach(t);
    }
}

static void start_proxy(void) {
    int listener = listen_any(&proxy_port);
    pthread_t t;
    pthread_create(&t, NULL, proxy, (void *)(intptr_t)listener);
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
                        .ca_file = CERT};
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

static void test_a_request_to_a_trusted_server_succeeds(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/"));
    CHECK(o.last.error == HX_OK && o.status == 200 && o.body_len == 2 && memcmp(o.body, "ok", 2) == 0);
    finish_client(c);
}

static void test_the_handshake_is_timed_after_the_connect(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/"));
    const int64_t *t = o.last.times;
    CHECK(t[HX_T_CONNECT] <= t[HX_T_TLS] && t[HX_T_TLS] <= t[HX_T_FIRST_SENT]);
    finish_client(c);
}

static void test_an_untrusted_server_fails_verification(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.ca_file = OTHER_CERT;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_ERR_TLS_VERIFY && o.last.detail != NULL);
    finish_client(c);
}

static void test_an_untrusted_server_is_fine_without_verification(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.verify = false;
    r.ca_file = NULL;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && o.status == 200);
    finish_client(c);
}

static void test_ca_certificates_can_be_given_as_pem(void) {
    FILE *f = fopen(CERT, "rb");
    static char pem[8192];
    size_t len = f != NULL ? fread(pem, 1, sizeof(pem), f) : 0;
    if (f != NULL) {
        fclose(f);
    }
    hx_client *c = client(0);
    hx_request r = get("/");
    r.ca_file = NULL;
    r.ca_pem = pem;
    r.ca_pem_len = len;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK);
    finish_client(c);
}

static void test_empty_pem_trusts_nothing(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.ca_file = NULL;
    r.ca_pem = "";
    r.ca_pem_len = 0;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_ERR_TLS_VERIFY);
    finish_client(c);
}

static void test_a_missing_ca_file_is_a_ca_error(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.ca_file = "/missing/ca.pem";
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_ERR_TLS_CA);
    finish_client(c);
}

static void test_a_missing_client_certificate_is_a_certificate_error(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.cert_file = "/missing/cert.pem";
    r.key_file = "/missing/key.pem";
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_ERR_TLS_CERT);
    finish_client(c);
}

static void test_the_next_request_reuses_the_tls_connection(void) {
    hx_client *c = client(0);
    outcome first = fetch(c, get("/"));
    outcome second = fetch(c, get("/"));
    CHECK(second.last.error == HX_OK && second.last.reused && second.last.connection_id == first.last.connection_id);
    finish_client(c);
}

static void test_a_new_connection_resumes_the_tls_session(void) {
    hx_client *c = client(0);
    fetch(c, get("/"));
    hx_request r = get("/reused");
    r.fresh_connection = true;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && !o.last.reused && o.body_len == 1 && o.body[0] == '1');
    finish_client(c);
}

static void test_a_session_outlives_its_closed_connection(void) {
    hx_client *c = client(0);
    hx_request first = get("/");
    first.close_after = true;
    fetch(c, first);
    outcome o = fetch(c, get("/reused"));
    CHECK(o.last.error == HX_OK && !o.last.reused && o.body_len == 1 && o.body[0] == '1');
    finish_client(c);
}

static void test_another_tls_cache_does_not_resume(void) {
    hx_client *c = client(0);
    fetch(c, get("/"));
    hx_request r = get("/reused");
    r.fresh_connection = true;
    r.tls_cache = 1;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && o.body_len == 1 && o.body[0] == '0');
    finish_client(c);
}

static void test_a_body_bigger_than_the_read_buffer_arrives_whole(void) {
    hx_client *c = client(1024);
    outcome o = fetch(c, get("/big"));
    CHECK(o.last.error == HX_OK && o.body_total == 100000);
    finish_client(c);
}

static hx_request proxied(void) {
    hx_request r = get("/");
    r.proxy_host = "127.0.0.1";
    r.proxy_port = (uint16_t)proxy_port;
    return r;
}

static void test_a_request_tunnels_through_a_proxy(void) {
    proxy_refuses = false;
    hx_client *c = client(0);
    outcome o = fetch(c, proxied());
    char want[64];
    snprintf(want, sizeof(want), "CONNECT 127.0.0.1:%d HTTP/1.1\r\n", server_port);
    CHECK(o.last.error == HX_OK && o.status == 200 && strncmp(proxy_saw, want, strlen(want)) == 0);
    finish_client(c);
}

static void test_a_refused_tunnel_is_a_tunnel_error(void) {
    proxy_refuses = true;
    hx_client *c = client(0);
    outcome o = fetch(c, proxied());
    CHECK(o.last.error == HX_ERR_TUNNEL && o.last.sys_errno == 403);
    finish_client(c);
}

static void test_proxy_credentials_go_with_the_connect(void) {
    proxy_refuses = true;
    hx_client *c = client(0);
    hx_request r = proxied();
    r.proxy_auth = "Proxy-Authorization: Basic dXNlcjpwdw==";
    fetch(c, r);
    CHECK(strstr(proxy_saw, "\r\nProxy-Authorization: Basic dXNlcjpwdw==\r\n") != NULL);
    finish_client(c);
}

static void test_plain_http_to_a_tls_server_does_not_succeed(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.tls = false;
    r.timeout_ns = 500000000;
    outcome o = fetch(c, r);
    CHECK(o.last.error != HX_OK);
    finish_client(c);
}

int main(void) {
    start_server();
    start_proxy();
    test_a_request_to_a_trusted_server_succeeds();
    test_the_handshake_is_timed_after_the_connect();
    test_an_untrusted_server_fails_verification();
    test_an_untrusted_server_is_fine_without_verification();
    test_ca_certificates_can_be_given_as_pem();
    test_empty_pem_trusts_nothing();
    test_a_missing_ca_file_is_a_ca_error();
    test_a_missing_client_certificate_is_a_certificate_error();
    test_the_next_request_reuses_the_tls_connection();
    test_a_new_connection_resumes_the_tls_session();
    test_a_session_outlives_its_closed_connection();
    test_another_tls_cache_does_not_resume();
    test_a_body_bigger_than_the_read_buffer_arrives_whole();
    test_a_request_tunnels_through_a_proxy();
    test_a_refused_tunnel_is_a_tunnel_error();
    test_proxy_credentials_go_with_the_connect();
    test_plain_http_to_a_tls_server_does_not_succeed();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
