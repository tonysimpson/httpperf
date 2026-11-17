#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../include/hx.h"

static int failures;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

static int server_port;

static void *serve_one(void *arg) {
    int fd = (int)(intptr_t)arg;
    char buf[4096];
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            break;
        }
        buf[n] = '\0';
        if (strstr(buf, " /hang ") != NULL) {
            continue;
        }
        if (strstr(buf, " /close ") != NULL) {
            const char *r = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
            send(fd, r, strlen(r), MSG_NOSIGNAL);
            break;
        }
        if (strstr(buf, " /drop ") != NULL) {
            const char *r = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
            send(fd, r, strlen(r), MSG_NOSIGNAL);
            usleep(20000);
            break;
        }
        if (strstr(buf, " /gzip ") != NULL) {
            static const char r[] = "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 25\r\n\r\n"
                                    "\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xcb\x48\xcd\xc9\xc9\x07\x00\x86"
                                    "\xa6\x10\x36\x05\x00\x00\x00";
            send(fd, r, sizeof(r) - 1, MSG_NOSIGNAL);
            continue;
        }
        if (strstr(buf, " /big ") != NULL) {
            static char big[200000];
            int head = snprintf(big, sizeof(big), "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n", sizeof(big) - 100);
            memset(big + head, 'x', sizeof(big) - 100);
            send(fd, big, (size_t)head + sizeof(big) - 100, MSG_NOSIGNAL);
            continue;
        }
        const char *r = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        send(fd, r, strlen(r), MSG_NOSIGNAL);
    }
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
    char body[256];
    size_t body_len;
    size_t body_total;
    int done;
    hx_event last;
} outcome;

static loop the_loop;

static long pool_seven_open;

static void count_connections(void *ctx, uint64_t pool, int delta) {
    if (pool == 7) {
        pool_seven_open += delta;
    }
}

static hx_client *client_with(hx_config cfg) {
    the_loop = (loop){.epoll = epoll_create1(0)};
    return hx_client_new(&cfg, (hx_io){watch, set_timer, clock_ns, &the_loop, count_connections});
}

static hx_client *client(int64_t idle_timeout_ns) {
    return client_with((hx_config){.max_idle = 8, .idle_timeout_ns = idle_timeout_ns});
}

static void run(hx_client *c, outcome *o, int wanted, int max_ms) {
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
                } else {
                    mine->done++;
                    mine->last = events[i];
                    done++;
                }
            }
        }
        if (done >= wanted) {
            break;
        }
        int64_t t = clock_ns(NULL);
        int timeout = 10;
        if (the_loop.deadline > 0) {
            int64_t ms = (the_loop.deadline - t) / 1000000;
            timeout = ms < 0 ? 0 : ms < timeout ? (int)ms : timeout;
        }
        struct epoll_event ready[16];
        int n = epoll_wait(the_loop.epoll, ready, 16, timeout);
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
    (void)o;
}

static hx_request get(const char *target) {
    static char authority[32];
    snprintf(authority, sizeof(authority), "127.0.0.1:%d", server_port);
    return (hx_request){
        .method = "GET", .host = "127.0.0.1", .port = (uint16_t)server_port, .authority = authority, .target = target};
}

static void finish_client(hx_client *c) {
    hx_client_free(c);
    close(the_loop.epoll);
}

static void test_a_get_gives_status_body_and_done(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.status == 200 && o.body_len == 2 && memcmp(o.body, "ok", 2) == 0);
    CHECK(o.done == 1 && o.last.error == HX_OK && !o.last.reused && o.last.version == 11);
    CHECK(strcmp(o.last.primary_ip, "127.0.0.1") == 0);
    finish_client(c);
}

static void test_the_next_request_reuses_the_connection(void) {
    hx_client *c = client(0);
    outcome a = {0}, b = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &a);
    run(c, &a, 1, 2000);
    hx_request_start(c, &r, &b);
    run(c, &b, 1, 2000);
    CHECK(b.done == 1 && b.last.reused && b.last.connection_id == a.last.connection_id);
    CHECK(hx_client_open_connections(c) == 1);
    finish_client(c);
}

static void test_a_fresh_connection_is_not_reused(void) {
    hx_client *c = client(0);
    outcome a = {0}, b = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &a);
    run(c, &a, 1, 2000);
    r.fresh_connection = true;
    hx_request_start(c, &r, &b);
    run(c, &b, 1, 2000);
    CHECK(b.done == 1 && !b.last.reused && b.last.connection_id != a.last.connection_id);
    finish_client(c);
}

static void test_another_pool_gets_its_own_connection(void) {
    hx_client *c = client(0);
    outcome a = {0}, b = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &a);
    run(c, &a, 1, 2000);
    r.pool = 7;
    hx_request_start(c, &r, &b);
    run(c, &b, 1, 2000);
    CHECK(b.done == 1 && !b.last.reused && hx_client_open_connections(c) == 2);
    finish_client(c);
}

static void test_a_requests_idle_timeout_applies_to_its_connection(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.idle_timeout_ns = 50 * 1000000;
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    outcome none = {0};
    run(c, &none, 1, 200);
    CHECK(hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_connection_close_from_the_server_closes_it(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/close");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.last.error == HX_OK && o.last.connection_closed && hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_an_idle_connection_the_server_closes_leaves_the_pool(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/drop");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(hx_client_open_connections(c) == 1);
    outcome none = {0};
    run(c, &none, 1, 200);
    CHECK(hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_an_idle_timeout_closes_the_connection(void) {
    hx_client *c = client(50 * 1000000);
    outcome o = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    outcome none = {0};
    run(c, &none, 1, 200);
    CHECK(hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_a_refused_connection_is_a_connect_error(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.port = 1;
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.last.error == HX_ERR_CONNECT && o.last.sys_errno == ECONNREFUSED);
    finish_client(c);
}

static void test_localhost_is_looked_up(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.host = "localhost";
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.last.error == HX_OK && o.status == 200);
    finish_client(c);
}

static void test_an_unknown_name_is_a_dns_error(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.host = "nothing.invalid";
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 5000);
    CHECK(o.done == 1 && o.last.error == HX_ERR_DNS);
    finish_client(c);
}

static void test_requests_waiting_on_one_lookup_all_complete(void) {
    hx_client *c = client(0);
    outcome o[3] = {0};
    hx_request r = get("/");
    r.host = "localhost";
    for (int i = 0; i < 3; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, NULL, 3, 2000);
    CHECK(o[0].status == 200 && o[1].status == 200 && o[2].status == 200);
    finish_client(c);
}

static void test_a_cancel_during_a_lookup_gives_exactly_one_done(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.host = "localhost";
    hx_req *q = hx_request_start(c, &r, &o);
    hx_request_cancel(q);
    run(c, &o, 2, 300);
    CHECK(o.done == 1 && o.last.error == HX_ERR_CANCELLED);
    finish_client(c);
}

static void test_a_lookup_is_cached(void) {
    hx_client *c = client(0);
    outcome first = {0}, second = {0};
    hx_request r = get("/");
    r.host = "localhost";
    r.fresh_connection = true;
    hx_request_start(c, &r, &first);
    run(c, &first, 1, 2000);
    hx_request_start(c, &r, &second);
    hx_event e;
    int count = hx_client_events(c, &e, 1);
    run(c, &second, 1, 2000);
    CHECK(count == 0 && second.status == 200 && second.last.times[HX_T_DNS] - second.last.times[HX_T_START] < 1000000);
    finish_client(c);
}

static void test_a_slow_response_times_out(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/hang");
    r.timeout_ns = 50 * 1000000;
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.last.error == HX_ERR_TIMEOUT && hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_a_cancel_gives_exactly_one_done(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/hang");
    hx_req *q = hx_request_start(c, &r, &o);
    run(c, &o, 1, 50);
    hx_request_cancel(q);
    hx_request_cancel(q);
    run(c, &o, 2, 100);
    CHECK(o.done == 1 && o.last.error == HX_ERR_CANCELLED);
    finish_client(c);
}

static void test_a_big_body_arrives_whole(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/big");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.last.error == HX_OK && o.body_total == 200000 - 100);
    finish_client(c);
}

static void test_a_gzip_body_is_decoded_when_asked(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/gzip");
    r.decode = true;
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.body_len == 5 && memcmp(o.body, "hello", 5) == 0);
    finish_client(c);
}

static void test_a_gzip_body_is_left_alone_otherwise(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/gzip");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.done == 1 && o.body_len == 25);
    finish_client(c);
}

static void test_the_connection_is_reused_after_a_decoded_body(void) {
    hx_client *c = client(0);
    outcome first = {0}, second = {0};
    hx_request r = get("/gzip");
    r.decode = true;
    hx_request_start(c, &r, &first);
    run(c, &first, 1, 2000);
    hx_request plain = get("/");
    plain.decode = true;
    hx_request_start(c, &plain, &second);
    run(c, &second, 1, 2000);
    CHECK(second.last.reused && second.body_len == 2 && memcmp(second.body, "ok", 2) == 0);
    finish_client(c);
}

static void test_only_max_idle_connections_stay_open(void) {
    hx_client *c = client_with((hx_config){.max_idle = 1});
    outcome o[3] = {0};
    hx_request r = get("/");
    for (int i = 0; i < 3; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, NULL, 3, 2000);
    CHECK(o[0].status == 200 && o[1].status == 200 && o[2].status == 200 && hx_client_open_connections(c) == 1);
    finish_client(c);
}

static void test_a_connection_past_its_max_age_is_not_reused(void) {
    hx_client *c = client(0);
    outcome first = {0}, second = {0};
    hx_request r = get("/");
    r.max_age_ns = 1000000;
    hx_request_start(c, &r, &first);
    run(c, &first, 1, 2000);
    usleep(5000);
    hx_request_start(c, &r, &second);
    run(c, &second, 1, 2000);
    CHECK(second.status == 200 && !second.last.reused && hx_client_open_connections(c) == 1);
    finish_client(c);
}

static void test_the_connection_hook_counts_a_pools_connections(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.pool = 7;
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    CHECK(o.status == 200 && pool_seven_open == 1);
    finish_client(c);
    CHECK(pool_seven_open == 0);
}

static void test_closing_a_pool_closes_its_idle_connections(void) {
    hx_client *c = client(0);
    outcome mine = {0}, other = {0};
    hx_request r = get("/");
    r.pool = 7;
    hx_request shared = get("/");
    hx_request_start(c, &r, &mine);
    hx_request_start(c, &shared, &other);
    run(c, NULL, 2, 2000);
    hx_client_close_pool(c, 7);
    CHECK(pool_seven_open == 0 && hx_client_open_connections(c) == 1);
    finish_client(c);
}

static void test_closing_a_pool_closes_a_busy_connection_once_its_request_ends(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    r.pool = 7;
    hx_request_start(c, &r, &o);
    hx_client_close_pool(c, 7);
    run(c, &o, 1, 2000);
    CHECK(o.status == 200 && o.last.connection_closed && hx_client_open_connections(c) == 0);
    finish_client(c);
}

static void test_times_are_in_order(void) {
    hx_client *c = client(0);
    outcome o = {0};
    hx_request r = get("/");
    hx_request_start(c, &r, &o);
    run(c, &o, 1, 2000);
    const int64_t *t = o.last.times;
    for (int i = 1; i < HX_T_COUNT; i++) {
        CHECK(t[i] >= t[i - 1] && t[i] > 0);
    }
    CHECK(o.last.bytes_sent > 0 && o.last.bytes_received == strlen("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"));
    finish_client(c);
}

static void test_many_requests_at_once(void) {
    hx_client *c = client(0);
    outcome o[20] = {{0}};
    hx_request r = get("/");
    for (int i = 0; i < 20; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, o, 20, 2000);
    int ok = 0;
    for (int i = 0; i < 20; i++) {
        ok += o[i].done == 1 && o[i].status == 200;
    }
    CHECK(ok == 20);
    finish_client(c);
}

int main(void) {
    start_server();
    test_a_get_gives_status_body_and_done();
    test_the_next_request_reuses_the_connection();
    test_a_fresh_connection_is_not_reused();
    test_another_pool_gets_its_own_connection();
    test_a_requests_idle_timeout_applies_to_its_connection();
    test_connection_close_from_the_server_closes_it();
    test_an_idle_connection_the_server_closes_leaves_the_pool();
    test_an_idle_timeout_closes_the_connection();
    test_a_refused_connection_is_a_connect_error();
    test_localhost_is_looked_up();
    test_an_unknown_name_is_a_dns_error();
    test_requests_waiting_on_one_lookup_all_complete();
    test_a_cancel_during_a_lookup_gives_exactly_one_done();
    test_a_lookup_is_cached();
    test_a_slow_response_times_out();
    test_a_cancel_gives_exactly_one_done();
    test_a_big_body_arrives_whole();
    test_a_gzip_body_is_decoded_when_asked();
    test_a_gzip_body_is_left_alone_otherwise();
    test_the_connection_is_reused_after_a_decoded_body();
    test_only_max_idle_connections_stay_open();
    test_a_connection_past_its_max_age_is_not_reused();
    test_the_connection_hook_counts_a_pools_connections();
    test_closing_a_pool_closes_its_idle_connections();
    test_closing_a_pool_closes_a_busy_connection_once_its_request_ends();
    test_times_are_in_order();
    test_many_requests_at_once();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
