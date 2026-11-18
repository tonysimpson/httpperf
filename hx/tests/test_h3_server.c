#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "h3_server.h"

#define CERT "build/hx/tls-cert.pem"
#define KEY "build/hx/tls-key.pem"
#define CURL ".deps/bin/curl --http3-only -sk --max-time 10"

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
    char out[256];
    size_t len;
    int status;
} result;

static result curl(const char *args) {
    char command[4096];
    snprintf(command, sizeof(command), CURL " %s 2>/dev/null", args);
    for (char *at; (at = strstr(command, "URL")) != NULL;) {
        char rest[4096];
        snprintf(rest, sizeof(rest), "https://127.0.0.1:%d%s", port, at + 3);
        snprintf(at, sizeof(command) - (size_t)(at - command), "%s", rest);
    }
    result r = {0};
    FILE *f = popen(command, "r");
    if (f == NULL) {
        r.status = -1;
        return r;
    }
    r.len = fread(r.out, 1, sizeof(r.out) - 1, f);
    int status = pclose(f);
    r.status = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    return r;
}

static void test_the_root_answers_ok(void) {
    result r = curl("-D - URL/");
    CHECK(r.status == 0);
    CHECK(strstr(r.out, "HTTP/3 200") != NULL && strstr(r.out, "content-type: text/plain") != NULL);
    CHECK(r.len >= 2 && memcmp(r.out + r.len - 2, "ok", 2) == 0);
}

static void test_an_unknown_path_is_not_found(void) {
    result r = curl("-w ' %{http_code}' URL/nowhere");
    CHECK(r.status == 0 && strcmp(r.out, "not found 404") == 0);
}

static void test_big_sends_the_whole_body(void) {
    result r = curl("-o /dev/null -w '%{size_download}' URL/big");
    CHECK(r.status == 0 && strcmp(r.out, "100000") == 0);
}

static void test_count_reports_the_uploaded_bytes(void) {
    char name[] = "/tmp/h3_upload_XXXXXX";
    int fd = mkstemp(name);
    static char data[500000];
    memset(data, 'u', sizeof(data));
    CHECK(fd >= 0 && write(fd, data, sizeof(data)) == (ssize_t)sizeof(data));
    close(fd);
    char args[256];
    snprintf(args, sizeof(args), "--data-binary @%s URL/count", name);
    result r = curl(args);
    unlink(name);
    CHECK(r.status == 0 && strcmp(r.out, "500000") == 0);
}

static void test_an_empty_upload_counts_zero(void) {
    result r = curl("-X POST URL/count");
    CHECK(r.status == 0 && strcmp(r.out, "0") == 0);
}

static void test_two_urls_share_one_connection(void) {
    int before = h3_server_connections();
    result r = curl("URL/ URL/");
    CHECK(r.status == 0 && strcmp(r.out, "okok") == 0);
    CHECK(h3_server_connections() == before + 1);
}

static void test_many_parallel_requests_all_answer(void) {
    char args[1024] = "--parallel --parallel-max 50";
    for (int i = 0; i < 40; i++) {
        strcat(args, " URL/");
    }
    result r = curl(args);
    CHECK(r.status == 0 && r.len == 80);
}

static void test_separate_clients_at_once_all_answer(void) {
    char command[256];
    snprintf(command, sizeof(command), CURL " -o /dev/null -w '%%{size_download}\\n' https://127.0.0.1:%d/big", port);
    FILE *clients[4];
    for (int i = 0; i < 4; i++) {
        clients[i] = popen(command, "r");
    }
    for (int i = 0; i < 4; i++) {
        char out[32] = {0};
        CHECK(clients[i] != NULL && fgets(out, sizeof(out), clients[i]) != NULL && strcmp(out, "100000\n") == 0);
        CHECK(clients[i] != NULL && pclose(clients[i]) == 0);
    }
}

static void test_slow_answers_after_a_delay(void) {
    result r = curl("-w ' %{time_total}' URL/slow");
    CHECK(r.status == 0 && strncmp(r.out, "ok ", 3) == 0 && atof(r.out + 3) >= 0.3);
}

static void test_a_refused_connection_leaves_the_server_working(void) {
    int before = h3_server_connections();
    h3_server_refuse_next_connection();
    curl("URL/refuse");
    CHECK(h3_server_connections() > before);
    result again = curl("URL/refuse");
    CHECK(again.status == 0 && strcmp(again.out, "ok") == 0);
    result r = curl("URL/");
    CHECK(r.status == 0 && strcmp(r.out, "ok") == 0);
}

static void test_a_hang_up_fails_the_client_and_leaves_the_server_working(void) {
    result hung = curl("URL/hang-up");
    CHECK(hung.status != 0);
    result r = curl("URL/");
    CHECK(r.status == 0 && strcmp(r.out, "ok") == 0);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    port = h3_server_start(CERT, KEY);
    if (port <= 0) {
        printf("can't start the h3 server\n");
        return 1;
    }
    test_the_root_answers_ok();
    test_an_unknown_path_is_not_found();
    test_big_sends_the_whole_body();
    test_count_reports_the_uploaded_bytes();
    test_an_empty_upload_counts_zero();
    test_two_urls_share_one_connection();
    test_many_parallel_requests_all_answer();
    test_separate_clients_at_once_all_answer();
    test_slow_answers_after_a_delay();
    test_a_refused_connection_leaves_the_server_working();
    test_a_hang_up_fails_the_client_and_leaves_the_server_working();
    h3_server_stop();
    printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures != 0;
}
