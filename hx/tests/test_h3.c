#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../include/hx.h"
#include "h3_server.h"

#define CERT "build/hx/tls-cert.pem"
#define KEY "build/hx/tls-key.pem"

static int failures;
static int port;

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond)) {                                               \
            printf("FAILED %s:%d: %s\n", __func__, __LINE__, #cond); \
            failures++;                                              \
        }                                                            \
    } while (0)

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
    hx_req *req;
    size_t to_send;
    size_t piece;
    int send_ready;
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
                } else if (events[i].type == HX_EV_SEND_READY) {
                    static char piece[4096];
                    mine->send_ready++;
                    size_t n = mine->to_send < mine->piece ? mine->to_send : mine->piece;
                    mine->to_send -= n;
                    if (n > 0) {
                        hx_request_send(mine->req, piece, n);
                    } else {
                        hx_request_end_body(mine->req);
                    }
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
    snprintf(authority, sizeof(authority), "127.0.0.1:%d", port);
    return (hx_request){.method = "GET",
                        .host = "127.0.0.1",
                        .port = (uint16_t)port,
                        .authority = authority,
                        .target = target,
                        .tls = true,
                        .verify = true,
                        .ca_file = CERT,
                        .http = HX_HTTP3};
}

static outcome fetch(hx_client *c, hx_request r) {
    outcome o = {0};
    hx_request_start(c, &r, &o);
    run(c, 1, 3000);
    return o;
}

static outcome upload_to(hx_client *c, const char *target, int64_t size, size_t total, size_t piece) {
    hx_request r = get(target);
    r.method = "POST";
    r.upload = true;
    r.upload_size = size;
    outcome o = {.to_send = total, .piece = piece};
    o.req = hx_request_start(c, &r, &o);
    run(c, 1, 3000);
    return o;
}

static outcome upload(hx_client *c, int64_t size, size_t total, size_t piece) {
    return upload_to(c, "/count", size, total, piece);
}

static void finish_client(hx_client *c) {
    hx_client_free(c);
    close(the_loop.epoll);
}

static void test_a_request_asking_for_h3_uses_it(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/"));
    CHECK(o.last.error == HX_OK && o.last.version == 30 && o.status == 200);
    CHECK(o.body_len == 2 && memcmp(o.body, "ok", 2) == 0);
    finish_client(c);
}

static void test_a_missing_page_reports_its_status(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/nowhere"));
    CHECK(o.last.error == HX_OK && o.status == 404);
    finish_client(c);
}

static void test_requests_started_together_share_one_connection(void) {
    hx_client *c = client(0);
    int before = h3_server_connections();
    outcome o[3] = {0};
    hx_request r = get("/");
    for (int i = 0; i < 3; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, 3, 3000);
    CHECK(o[0].last.error == HX_OK && o[1].last.error == HX_OK && o[2].last.error == HX_OK);
    CHECK(o[0].last.connection_id == o[1].last.connection_id && o[1].last.connection_id == o[2].last.connection_id);
    CHECK(h3_server_connections() == before + 1);
    finish_client(c);
}

static void test_refused_requests_retry_together_on_one_new_connection(void) {
    hx_client *c = client(0);
    int before = h3_server_connections();
    h3_server_refuse_next_connection();
    outcome o[3] = {0};
    hx_request r = get("/refuse");
    for (int i = 0; i < 3; i++) {
        hx_request_start(c, &r, &o[i]);
    }
    run(c, 3, 3000);
    CHECK(o[0].last.error == HX_OK && o[1].last.error == HX_OK && o[2].last.error == HX_OK);
    CHECK(o[0].last.connection_id == o[1].last.connection_id && o[1].last.connection_id == o[2].last.connection_id);
    CHECK(h3_server_connections() == before + 2);
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

static void test_a_new_connection_resumes_the_tls_session(void) {
    hx_client *c = client(0);
    fetch(c, get("/"));
    hx_request r = get("/resumed");
    r.fresh_connection = true;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && !o.last.reused && o.body_len == 1 && o.body[0] == '1');
    finish_client(c);
}

static void test_the_first_connection_does_not_resume(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/resumed"));
    CHECK(o.last.error == HX_OK && o.body_len == 1 && o.body[0] == '0');
    finish_client(c);
}

static void test_another_tls_cache_does_not_resume(void) {
    hx_client *c = client(0);
    fetch(c, get("/"));
    hx_request r = get("/resumed");
    r.fresh_connection = true;
    r.tls_cache = 1;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && o.body_len == 1 && o.body[0] == '0');
    finish_client(c);
}

static void test_a_slow_response_still_arrives(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/slow"));
    CHECK(o.last.error == HX_OK && o.status == 200);
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

static void test_freeing_the_client_during_the_handshake_is_safe(void) {
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

static void test_an_upload_of_unknown_size_goes_over_h3(void) {
    hx_client *c = client(0);
    outcome o = upload(c, -1, 100000, 4096);
    CHECK(o.last.error == HX_OK && o.last.version == 30 && o.status == 200);
    CHECK(o.body_len == 6 && memcmp(o.body, "100000", 6) == 0);
    finish_client(c);
}

static void test_an_upload_bigger_than_the_stream_window_arrives_whole(void) {
    hx_client *c = client(0);
    outcome o = upload(c, -1, 3000000, 4096);
    CHECK(o.last.error == HX_OK && o.body_len == 7 && memcmp(o.body, "3000000", 7) == 0);
    finish_client(c);
}

static void test_an_upload_of_known_size_goes_over_h3(void) {
    hx_client *c = client(0);
    outcome o = upload(c, 5000, 5000, 1000);
    CHECK(o.last.error == HX_OK && o.last.version == 30);
    CHECK(o.body_len == 4 && memcmp(o.body, "5000", 4) == 0);
    finish_client(c);
}

static void test_an_empty_upload_goes_over_h3(void) {
    hx_client *c = client(0);
    outcome o = upload(c, -1, 0, 1000);
    CHECK(o.last.error == HX_OK && o.last.version == 30);
    CHECK(o.body_len == 1 && o.body[0] == '0');
    finish_client(c);
}

static void test_an_upload_asks_for_each_piece(void) {
    hx_client *c = client(0);
    outcome o = upload(c, -1, 10000, 1000);
    CHECK(o.last.error == HX_OK && o.send_ready == 11);
    finish_client(c);
}

static void test_a_post_with_a_fixed_body_sends_it(void) {
    hx_client *c = client(0);
    hx_request r = get("/count");
    r.method = "POST";
    r.body = "hello";
    r.body_len = 5;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_OK && o.body_len == 1 && o.body[0] == '5');
    finish_client(c);
}

static void test_a_server_hanging_up_fails_the_request(void) {
    hx_client *c = client(0);
    outcome o = fetch(c, get("/hang-up"));
    CHECK(o.done == 1 && o.last.error != HX_OK);
    finish_client(c);
}

static void test_an_untrusted_certificate_fails_verification(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.ca_file = NULL;
    outcome o = fetch(c, r);
    CHECK(o.last.error == HX_ERR_TLS_VERIFY);
    finish_client(c);
}

static void test_a_server_that_never_answers_times_out_connecting(void) {
    int silent = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t len = sizeof(a);
    bind(silent, (struct sockaddr *)&a, sizeof(a));
    getsockname(silent, (struct sockaddr *)&a, &len);
    hx_client *c = client(0);
    hx_request r = get("/");
    r.port = ntohs(a.sin_port);
    r.connect_timeout_ns = 100000000;
    outcome o = fetch(c, r);
    CHECK(o.done == 1 && o.last.error == HX_ERR_CONNECT_TIMEOUT);
    finish_client(c);
    close(silent);
}

static void test_a_slow_response_times_out_after_connecting(void) {
    hx_client *c = client(0);
    hx_request r = get("/slow");
    r.timeout_ns = 100000000;
    outcome o = fetch(c, r);
    CHECK(o.done == 1 && o.last.error == HX_ERR_TIMEOUT);
    finish_client(c);
}

static void test_nothing_listening_fails_the_request(void) {
    hx_client *c = client(0);
    hx_request r = get("/");
    r.port = 1;
    r.connect_timeout_ns = 500000000;
    outcome o = fetch(c, r);
    CHECK(o.done == 1 && o.last.error != HX_OK);
    finish_client(c);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    port = h3_server_start(CERT, KEY);
    if (port < 0) {
        printf("could not start the server\n");
        return 1;
    }
    test_a_request_asking_for_h3_uses_it();
    test_a_missing_page_reports_its_status();
    test_requests_started_together_share_one_connection();
    test_refused_requests_retry_together_on_one_new_connection();
    test_the_next_request_reuses_the_connection();
    test_a_body_bigger_than_the_read_buffer_arrives_whole();
    test_a_new_connection_resumes_the_tls_session();
    test_the_first_connection_does_not_resume();
    test_another_tls_cache_does_not_resume();
    test_a_slow_response_still_arrives();
    test_cancelling_one_stream_leaves_the_others();
    test_freeing_the_client_during_the_handshake_is_safe();
    test_freeing_the_client_with_streams_open_is_safe();
    test_an_upload_of_unknown_size_goes_over_h3();
    test_an_upload_bigger_than_the_stream_window_arrives_whole();
    test_an_upload_of_known_size_goes_over_h3();
    test_an_empty_upload_goes_over_h3();
    test_an_upload_asks_for_each_piece();
    test_a_post_with_a_fixed_body_sends_it();
    test_a_server_hanging_up_fails_the_request();
    test_an_untrusted_certificate_fails_verification();
    test_nothing_listening_fails_the_request();
    test_a_server_that_never_answers_times_out_connecting();
    test_a_slow_response_times_out_after_connecting();
    h3_server_stop();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
