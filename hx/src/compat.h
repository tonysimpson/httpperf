#ifndef HX_COMPAT_H
#define HX_COMPAT_H

/* Portable stand-ins for the Linux-only calls hx uses, so it also builds on
   macOS and the BSDs. */

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/random.h>
#endif

/* Fills buf with len cryptographically random bytes. Returns 0, or -1 on
   failure. */
static inline int hx_random_bytes(void *buf, size_t len) {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    arc4random_buf(buf, len);
    return 0;
#else
    return getrandom(buf, len, 0) == (ssize_t)len ? 0 : -1;
#endif
}

/* socket(), returning a non-blocking, close-on-exec socket that never raises
   SIGPIPE. Linux does the first two in one call with SOCK_NONBLOCK and
   SOCK_CLOEXEC; elsewhere they're set with fcntl afterwards. */
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
#ifdef SO_NOSIGPIPE
    /* MSG_NOSIGNAL covers hx's own send() calls, but not OpenSSL's writes */
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    return fd;
#endif
}

#endif
