#include "dns.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define WORKERS 4

typedef struct job {
    struct job *next;
    char host[256];
    char port[8];
    uint64_t id;
    int error;
    struct addrinfo *addrs;
} job;

struct hx_dns {
    pthread_mutex_t lock;
    pthread_cond_t wake;
    job *todo, *todo_tail;
    job *done, *done_tail;
    int pipe[2];
    int workers;
    int idle;
    bool closed;
    atomic_int refs;
};

static void release(hx_dns *d) {
    if (atomic_fetch_sub(&d->refs, 1) != 1) {
        return;
    }
    for (job *j = d->done, *next; j != NULL; j = next) {
        next = j->next;
        if (j->addrs != NULL) {
            freeaddrinfo(j->addrs);
        }
        free(j);
    }
    for (job *j = d->todo, *next; j != NULL; j = next) {
        next = j->next;
        free(j);
    }
    close(d->pipe[0]);
    close(d->pipe[1]);
    pthread_mutex_destroy(&d->lock);
    pthread_cond_destroy(&d->wake);
    free(d);
}

static void *worker(void *arg) {
    hx_dns *d = arg;
    pthread_mutex_lock(&d->lock);
    for (;;) {
        while (d->todo == NULL && !d->closed) {
            d->idle++;
            pthread_cond_wait(&d->wake, &d->lock);
            d->idle--;
        }
        if (d->closed) {
            break;
        }
        job *j = d->todo;
        d->todo = j->next;
        if (d->todo == NULL) {
            d->todo_tail = NULL;
        }
        pthread_mutex_unlock(&d->lock);
        struct addrinfo hints = {.ai_socktype = SOCK_STREAM, .ai_flags = AI_ADDRCONFIG | AI_NUMERICSERV};
        j->error = getaddrinfo(j->host, j->port, &hints, &j->addrs);
        j->next = NULL;
        pthread_mutex_lock(&d->lock);
        bool was_empty = d->done == NULL;
        if (d->done_tail != NULL) {
            d->done_tail->next = j;
        } else {
            d->done = j;
        }
        d->done_tail = j;
        if (was_empty && !d->closed) {
            char byte = 0;
            while (write(d->pipe[1], &byte, 1) == -1 && errno == EINTR) {
            }
        }
    }
    pthread_mutex_unlock(&d->lock);
    release(d);
    return NULL;
}

hx_dns *hx_dns_new(void) {
    hx_dns *d = calloc(1, sizeof(hx_dns));
    if (d == NULL) {
        return NULL;
    }
    if (pipe(d->pipe) == -1) {
        free(d);
        return NULL;
    }
    for (int i = 0; i < 2; i++) {
        fcntl(d->pipe[i], F_SETFL, fcntl(d->pipe[i], F_GETFL) | O_NONBLOCK);
        fcntl(d->pipe[i], F_SETFD, FD_CLOEXEC);
    }
    pthread_mutex_init(&d->lock, NULL);
    pthread_cond_init(&d->wake, NULL);
    atomic_init(&d->refs, 1);
    return d;
}

int hx_dns_fd(const hx_dns *d) { return d->pipe[0]; }

int hx_dns_lookup(hx_dns *d, const char *host, uint16_t port, uint64_t id) {
    job *j = calloc(1, sizeof(job));
    if (j == NULL) {
        return -1;
    }
    snprintf(j->host, sizeof(j->host), "%s", host);
    snprintf(j->port, sizeof(j->port), "%u", port);
    j->id = id;
    pthread_mutex_lock(&d->lock);
    if (d->todo_tail != NULL) {
        d->todo_tail->next = j;
    } else {
        d->todo = j;
    }
    d->todo_tail = j;
    bool start = d->idle == 0 && d->workers < WORKERS;
    if (start) {
        d->workers++;
        atomic_fetch_add(&d->refs, 1);
    }
    pthread_cond_signal(&d->wake);
    pthread_mutex_unlock(&d->lock);
    if (start) {
        pthread_t t;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, 256 * 1024);
        if (pthread_create(&t, &attr, worker, d) != 0) {
            pthread_mutex_lock(&d->lock);
            d->workers--;
            pthread_mutex_unlock(&d->lock);
            atomic_fetch_sub(&d->refs, 1);
        }
        pthread_attr_destroy(&attr);
    }
    return 0;
}

int hx_dns_results(hx_dns *d, hx_dns_result *out, int max) {
    char buf[64];
    while (read(d->pipe[0], buf, sizeof(buf)) > 0) {
    }
    int count = 0;
    pthread_mutex_lock(&d->lock);
    while (count < max && d->done != NULL) {
        job *j = d->done;
        d->done = j->next;
        if (d->done == NULL) {
            d->done_tail = NULL;
        }
        out[count++] = (hx_dns_result){j->id, j->error, j->addrs};
        free(j);
    }
    bool more = d->done != NULL;
    pthread_mutex_unlock(&d->lock);
    if (more) {
        char byte = 0;
        while (write(d->pipe[1], &byte, 1) == -1 && errno == EINTR) {
        }
    }
    return count;
}

void hx_dns_free(hx_dns *d) {
    pthread_mutex_lock(&d->lock);
    d->closed = true;
    pthread_cond_broadcast(&d->wake);
    pthread_mutex_unlock(&d->lock);
    release(d);
}
