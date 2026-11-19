#ifndef HX_COMPAT_H
#define HX_COMPAT_H

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define HX_ARC4RANDOM 1
#else
#include <sys/random.h>
#endif

static inline int hx_random_bytes(void *buf, size_t len) {
#ifdef HX_ARC4RANDOM
    arc4random_buf(buf, len);
    return 0;
#else
    return getrandom(buf, len, 0) == (ssize_t)len ? 0 : -1;
#endif
}

static inline int hx_open_socket(int domain, int type, int protocol) {
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    return socket(domain, type | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol);
#else
    int fd = socket(domain, type, protocol);
    if (fd < 0) {
        return fd;
    }
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    return fd;
#endif
}

#endif
