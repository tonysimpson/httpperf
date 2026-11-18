#ifndef HX_COOKIE_H
#define HX_COOKIE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "buf.h"

typedef struct hx_jar hx_jar;

hx_jar *hx_jar_new(void);
void hx_jar_free(hx_jar *j);
size_t hx_jar_count(const hx_jar *j);
bool hx_jar_set_cookie(hx_jar *j, const char *value, size_t len, const char *host, const char *path, bool secure,
                       int64_t now);
bool hx_jar_add_line(hx_jar *j, const char *line, int64_t now);
void hx_jar_clear(hx_jar *j, bool session_only);
bool hx_jar_header(hx_jar *j, const char *host, const char *path, bool secure, int64_t now, hx_buf *out);
bool hx_jar_lines(hx_jar *j, int64_t now, hx_buf *out);
int64_t hx_cookie_date(const char *s, size_t len);

#endif
