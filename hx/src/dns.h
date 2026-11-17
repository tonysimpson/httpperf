#ifndef HX_DNS_H
#define HX_DNS_H

#include <netdb.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct hx_dns hx_dns;

typedef struct {
    uint64_t id;
    int error;
    struct addrinfo *addrs;
} hx_dns_result;

hx_dns *hx_dns_new(void);
int hx_dns_fd(const hx_dns *d);
int hx_dns_lookup(hx_dns *d, const char *host, uint16_t port, uint64_t id);
int hx_dns_results(hx_dns *d, hx_dns_result *out, int max);
void hx_dns_free(hx_dns *d);

#endif
