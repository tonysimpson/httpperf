#ifndef HX_H
#define HX_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef int hx_socket;

enum { HX_READ = 1, HX_WRITE = 2 };

typedef struct {
    void (*watch)(void *ctx, hx_socket s, int events);
    void (*set_timer)(void *ctx, int64_t deadline_ns);
    int64_t (*now)(void *ctx);
    void *ctx;
} hx_io;

typedef struct {
    size_t max_idle_per_host;
    int64_t idle_timeout_ns;
    size_t read_size;
    int64_t dns_ttl_ns;
} hx_config;

typedef struct {
    const char *method;
    const char *host;
    uint16_t port;
    const char *authority;
    const char *target;
    const char *const *headers;
    size_t header_count;
    const char *body;
    size_t body_len;
    bool fresh_connection;
    bool close_after;
    uint64_t pool;
    int64_t idle_timeout_ns;
    int64_t timeout_ns;
    int64_t connect_timeout_ns;
    bool decode;
} hx_request;

typedef enum { HX_EV_HEADERS = 1, HX_EV_BODY, HX_EV_DONE } hx_event_type;

typedef enum {
    HX_OK = 0,
    HX_ERR_DNS,
    HX_ERR_CONNECT,
    HX_ERR_SEND,
    HX_ERR_RECEIVE,
    HX_ERR_PROTOCOL,
    HX_ERR_TIMEOUT,
    HX_ERR_CANCELLED,
    HX_ERR_INVALID,
    HX_ERR_NOMEM,
    HX_ERR_DECODE,
    HX_ERR_UNKNOWN_ENCODING,
} hx_error;

enum {
    HX_T_START,
    HX_T_DNS,
    HX_T_CONNECT,
    HX_T_TLS,
    HX_T_FIRST_SENT,
    HX_T_LAST_SENT,
    HX_T_FIRST_BYTE,
    HX_T_DONE,
    HX_T_COUNT,
};

typedef struct {
    hx_event_type type;
    void *user;
    const char *data;
    size_t len;
    int status;
    int version;
    hx_error error;
    int sys_errno;
    bool reused;
    bool connection_closed;
    uint64_t connection_id;
    int64_t times[HX_T_COUNT];
    uint64_t bytes_sent;
    uint64_t bytes_received;
    char primary_ip[46];
} hx_event;

typedef struct hx_client hx_client;
typedef struct hx_req hx_req;

hx_client *hx_client_new(const hx_config *cfg, hx_io io);
void hx_client_on_socket(hx_client *c, hx_socket s, int events);
void hx_client_on_timer(hx_client *c);
hx_req *hx_request_start(hx_client *c, const hx_request *r, void *user);
void hx_request_cancel(hx_req *q);
int hx_client_events(hx_client *c, hx_event *out, int max);
size_t hx_client_open_connections(const hx_client *c);
void hx_client_free(hx_client *c);

#endif
