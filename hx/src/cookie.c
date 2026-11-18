#include "cookie.h"

#include <stdio.h>
#include <strings.h>

#define MAX_NAME_VALUE 4096
#define MAX_SENT 150

typedef struct {
    char *name, *value, *domain, *path;
    bool tailmatch, secure, http_only;
    int64_t expires;
    uint64_t created;
} cookie;

struct hx_jar {
    cookie *items;
    size_t len, cap;
    uint64_t next_created;
};

hx_jar *hx_jar_new(void) { return calloc(1, sizeof(hx_jar)); }

static void cookie_free(cookie *c) {
    free(c->name);
    free(c->value);
    free(c->domain);
    free(c->path);
}

static void remove_at(hx_jar *j, size_t i) {
    cookie_free(&j->items[i]);
    j->items[i] = j->items[--j->len];
}

void hx_jar_free(hx_jar *j) {
    if (j == NULL) {
        return;
    }
    for (size_t i = 0; i < j->len; i++) {
        cookie_free(&j->items[i]);
    }
    free(j->items);
    free(j);
}

size_t hx_jar_count(const hx_jar *j) { return j->len; }

static char *dup(const char *s, size_t len) {
    char *d = malloc(len + 1);
    if (d != NULL) {
        memcpy(d, s, len);
        d[len] = '\0';
    }
    return d;
}

static bool is_space(char c) { return c == ' ' || c == '\t'; }

static void trim(const char **s, size_t *len) {
    while (*len > 0 && is_space(**s)) {
        (*s)++;
        (*len)--;
    }
    while (*len > 0 && is_space((*s)[*len - 1])) {
        (*len)--;
    }
}

static bool clean(const char *s, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if ((c < 0x20 && c != '\t') || c == 0x7f) {
            return false;
        }
    }
    return true;
}

static bool domain_match(const char *host, const char *domain, bool tailmatch) {
    if (strcasecmp(host, domain) == 0) {
        return true;
    }
    size_t h = strlen(host), d = strlen(domain);
    return tailmatch && h > d && host[h - d - 1] == '.' && strcasecmp(host + h - d, domain) == 0;
}

static bool path_match(const char *request, const char *path) {
    size_t n = strlen(path);
    if (strncmp(request, path, n) != 0) {
        return false;
    }
    return request[n] == '\0' || path[n - 1] == '/' || request[n] == '/' || request[n] == '?';
}

static bool is_ip(const char *host) {
    if (strchr(host, ':') != NULL) {
        return true;
    }
    for (const char *p = host; *p; p++) {
        if ((*p < '0' || *p > '9') && *p != '.') {
            return false;
        }
    }
    return true;
}

static void lower(char *s) {
    for (; *s; s++) {
        if (*s >= 'A' && *s <= 'Z') {
            *s = (char)(*s - 'A' + 'a');
        }
    }
}

static bool store(hx_jar *j, cookie *c, int64_t now) {
    for (size_t i = 0; i < j->len; i++) {
        cookie *old = &j->items[i];
        if (strcmp(old->name, c->name) == 0 && strcasecmp(old->domain, c->domain) == 0 &&
            old->tailmatch == c->tailmatch && strcmp(old->path, c->path) == 0) {
            c->created = old->created;
            remove_at(j, i);
            break;
        }
    }
    if (c->expires != 0 && c->expires <= now) {
        cookie_free(c);
        return true;
    }
    if (j->len == j->cap) {
        size_t cap = j->cap ? j->cap * 2 : 16;
        cookie *grown = realloc(j->items, cap * sizeof(cookie));
        if (grown == NULL) {
            cookie_free(c);
            return false;
        }
        j->items = grown;
        j->cap = cap;
    }
    if (c->created == 0) {
        c->created = ++j->next_created;
    }
    j->items[j->len++] = *c;
    return true;
}

static char *default_path(const char *path) {
    const char *end = path;
    for (const char *p = path; *p && *p != '?'; p++) {
        if (*p == '/') {
            end = p;
        }
    }
    return path[0] != '/' || end == path ? dup("/", 1) : dup(path, (size_t)(end - path));
}

static bool attr_is(const char *name, size_t len, const char *want) {
    return len == strlen(want) && strncasecmp(name, want, len) == 0;
}

static bool parse_max_age(const char *s, size_t len, int64_t now, int64_t *expires) {
    size_t i = s[0] == '-' ? 1 : 0;
    if (i == len) {
        return false;
    }
    int64_t n = 0;
    for (; i < len; i++) {
        if (s[i] < '0' || s[i] > '9') {
            return false;
        }
        n = n > (INT64_MAX - 9) / 10 ? INT64_MAX / 2 : n * 10 + (s[i] - '0');
    }
    *expires = s[0] == '-' || n == 0 ? 1 : now + n;
    return true;
}

bool hx_jar_set_cookie(hx_jar *j, const char *value, size_t len, const char *host, const char *path, bool secure,
                       int64_t now) {
    const char *end = value + len;
    const char *semi = memchr(value, ';', len);
    const char *pair_end = semi != NULL ? semi : end;
    const char *eq = memchr(value, '=', (size_t)(pair_end - value));
    if (eq == NULL) {
        return false;
    }
    const char *name = value, *val = eq + 1;
    size_t name_len = (size_t)(eq - value), val_len = (size_t)(pair_end - val);
    trim(&name, &name_len);
    trim(&val, &val_len);
    if (name_len == 0 || name_len + val_len > MAX_NAME_VALUE || !clean(name, name_len) || !clean(val, val_len)) {
        return false;
    }
    cookie c = {0};
    bool have_max_age = false, have_domain = false;
    const char *domain = NULL, *cpath = NULL;
    size_t domain_len = 0, cpath_len = 0;
    for (const char *p = semi; p != NULL && p < end;) {
        p++;
        const char *next = memchr(p, ';', (size_t)(end - p));
        const char *stop = next != NULL ? next : end;
        const char *aeq = memchr(p, '=', (size_t)(stop - p));
        const char *an = p, *av = aeq != NULL ? aeq + 1 : stop;
        size_t an_len = (size_t)((aeq != NULL ? aeq : stop) - p), av_len = (size_t)(stop - av);
        trim(&an, &an_len);
        trim(&av, &av_len);
        if (attr_is(an, an_len, "secure")) {
            c.secure = true;
        } else if (attr_is(an, an_len, "httponly")) {
            c.http_only = true;
        } else if (attr_is(an, an_len, "max-age") && av_len > 0) {
            have_max_age = parse_max_age(av, av_len, now, &c.expires) || have_max_age;
        } else if (attr_is(an, an_len, "expires") && !have_max_age && av_len > 0) {
            int64_t t = hx_cookie_date(av, av_len);
            if (t != -1) {
                c.expires = t > 0 ? t : 1;
            }
        } else if (attr_is(an, an_len, "domain") && av_len > 0) {
            while (av_len > 0 && av[0] == '.') {
                av++;
                av_len--;
            }
            if (av_len > 0) {
                domain = av;
                domain_len = av_len;
                have_domain = true;
            }
        } else if (attr_is(an, an_len, "path") && av_len > 0 && av[0] == '/') {
            cpath = av;
            cpath_len = av_len;
        }
        p = next;
    }
    if (c.secure && !secure) {
        return false;
    }
    if (name_len >= 9 && strncmp(name, "__Secure-", 9) == 0 && !c.secure) {
        return false;
    }
    if (name_len >= 7 && strncmp(name, "__Host-", 7) == 0 &&
        (!c.secure || have_domain || cpath == NULL || cpath_len != 1)) {
        return false;
    }
    c.name = dup(name, name_len);
    c.value = dup(val, val_len);
    c.domain = have_domain ? dup(domain, domain_len) : dup(host, strlen(host));
    c.path = cpath != NULL ? dup(cpath, cpath_len) : default_path(path);
    if (c.name == NULL || c.value == NULL || c.domain == NULL || c.path == NULL) {
        cookie_free(&c);
        return false;
    }
    lower(c.domain);
    if (have_domain) {
        if (is_ip(host) ? strcasecmp(host, c.domain) != 0 : !domain_match(host, c.domain, true)) {
            cookie_free(&c);
            return false;
        }
        c.tailmatch = !is_ip(host);
    }
    return store(j, &c, now);
}

static bool field(const char **p, const char **start, size_t *len) {
    if (*p == NULL) {
        return false;
    }
    *start = *p;
    const char *tab = strchr(*p, '\t');
    *len = tab != NULL ? (size_t)(tab - *p) : strlen(*p);
    *p = tab != NULL ? tab + 1 : NULL;
    return true;
}

bool hx_jar_add_line(hx_jar *j, const char *line, int64_t now) {
    cookie c = {0};
    if (strncmp(line, "#HttpOnly_", 10) == 0) {
        c.http_only = true;
        line += 10;
    } else if (line[0] == '#') {
        return false;
    }
    const char *p = line, *f[7];
    size_t n[7];
    int count = 0;
    while (count < 7 && field(&p, &f[count], &n[count])) {
        count++;
    }
    if (count == 6) {
        f[6] = "";
        n[6] = 0;
    } else if (count != 7 || p != NULL) {
        return false;
    }
    while (n[0] > 0 && f[0][0] == '.') {
        f[0]++;
        n[0]--;
    }
    if (n[5] == 0 || n[2] == 0) {
        return false;
    }
    c.tailmatch = n[1] == 4 && strncasecmp(f[1], "TRUE", 4) == 0;
    c.secure = n[3] == 4 && strncasecmp(f[3], "TRUE", 4) == 0;
    char *end = NULL;
    char number[24];
    snprintf(number, sizeof(number), "%.*s", (int)(n[4] < 23 ? n[4] : 23), f[4]);
    c.expires = strtoll(number, &end, 10);
    if (end == number || *end != '\0') {
        return false;
    }
    c.domain = dup(f[0], n[0]);
    c.path = dup(f[2], n[2]);
    c.name = dup(f[5], n[5]);
    c.value = dup(f[6], n[6]);
    if (c.name == NULL || c.value == NULL || c.domain == NULL || c.path == NULL) {
        cookie_free(&c);
        return false;
    }
    lower(c.domain);
    return store(j, &c, now);
}

void hx_jar_clear(hx_jar *j, bool session_only) {
    for (size_t i = j->len; i-- > 0;) {
        if (!session_only || j->items[i].expires == 0) {
            remove_at(j, i);
        }
    }
}

static void drop_expired(hx_jar *j, int64_t now) {
    for (size_t i = j->len; i-- > 0;) {
        if (j->items[i].expires != 0 && j->items[i].expires <= now) {
            remove_at(j, i);
        }
    }
}

static int send_order(const void *a, const void *b) {
    const cookie *x = *(const cookie *const *)a, *y = *(const cookie *const *)b;
    size_t xl = strlen(x->path), yl = strlen(y->path);
    if (xl != yl) {
        return xl > yl ? -1 : 1;
    }
    xl = strlen(x->domain);
    yl = strlen(y->domain);
    if (xl != yl) {
        return xl > yl ? -1 : 1;
    }
    xl = strlen(x->name);
    yl = strlen(y->name);
    if (xl != yl) {
        return xl > yl ? -1 : 1;
    }
    return x->created < y->created ? -1 : x->created > y->created;
}

bool hx_jar_header(hx_jar *j, const char *host, const char *path, bool secure, int64_t now, hx_buf *out) {
    drop_expired(j, now);
    cookie *chosen[MAX_SENT];
    size_t count = 0;
    for (size_t i = 0; i < j->len && count < MAX_SENT; i++) {
        cookie *c = &j->items[i];
        if ((!c->secure || secure) && domain_match(host, c->domain, c->tailmatch) && path_match(path, c->path)) {
            chosen[count++] = c;
        }
    }
    if (count == 0) {
        return false;
    }
    qsort(chosen, count, sizeof(cookie *), send_order);
    bool ok = true;
    for (size_t i = 0; ok && i < count; i++) {
        ok = (i == 0 || hx_buf_append(out, "; ", 2)) && hx_buf_append_str(out, chosen[i]->name) &&
             hx_buf_append(out, "=", 1) && hx_buf_append_str(out, chosen[i]->value);
    }
    return ok;
}

bool hx_jar_lines(hx_jar *j, int64_t now, hx_buf *out) {
    drop_expired(j, now);
    cookie **order = malloc((j->len ? j->len : 1) * sizeof(cookie *));
    if (order == NULL) {
        return false;
    }
    for (size_t i = 0; i < j->len; i++) {
        order[i] = &j->items[i];
    }
    for (size_t i = 1; i < j->len; i++) {
        for (size_t k = i; k > 0 && order[k - 1]->created > order[k]->created; k--) {
            cookie *t = order[k];
            order[k] = order[k - 1];
            order[k - 1] = t;
        }
    }
    bool ok = true;
    for (size_t i = 0; ok && i < j->len; i++) {
        cookie *c = order[i];
        char expires[24];
        snprintf(expires, sizeof(expires), "%lld", (long long)c->expires);
        ok = hx_buf_append_str(out, c->http_only ? "#HttpOnly_" : "") &&
             hx_buf_append_str(out, c->tailmatch ? "." : "") && hx_buf_append_str(out, c->domain) &&
             hx_buf_append_str(out, c->tailmatch ? "\tTRUE\t" : "\tFALSE\t") && hx_buf_append_str(out, c->path) &&
             hx_buf_append_str(out, c->secure ? "\tTRUE\t" : "\tFALSE\t") && hx_buf_append_str(out, expires) &&
             hx_buf_append(out, "\t", 1) && hx_buf_append_str(out, c->name) && hx_buf_append(out, "\t", 1) &&
             hx_buf_append_str(out, c->value) && hx_buf_append(out, "\n", 1);
    }
    free(order);
    return ok;
}

static bool delimiter(unsigned char c) {
    return c == 0x09 || (c >= 0x20 && c <= 0x2f) || (c >= 0x3b && c <= 0x40) || (c >= 0x5b && c <= 0x60) ||
           (c >= 0x7b && c <= 0x7e);
}

static int digits(const char *s, size_t len, size_t min, size_t max, int *value) {
    size_t i = 0;
    int v = 0;
    while (i < len && i < max && s[i] >= '0' && s[i] <= '9') {
        v = v * 10 + (s[i] - '0');
        i++;
    }
    if (i < min || (i < len && s[i] >= '0' && s[i] <= '9')) {
        return -1;
    }
    *value = v;
    return (int)i;
}

static int64_t days_from_civil(int64_t y, int m, int d) {
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    int64_t yoe = y - era * 400;
    int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

int64_t hx_cookie_date(const char *s, size_t len) {
    static const char *const months[] = {"jan", "feb", "mar", "apr", "may", "jun",
                                         "jul", "aug", "sep", "oct", "nov", "dec"};
    int hour = -1, minute = 0, second = 0, day = -1, month = -1, year = -1;
    size_t i = 0;
    while (i < len) {
        while (i < len && delimiter((unsigned char)s[i])) {
            i++;
        }
        size_t start = i;
        while (i < len && !delimiter((unsigned char)s[i])) {
            i++;
        }
        const char *t = s + start;
        size_t n = i - start;
        if (n == 0) {
            continue;
        }
        int a, b, c, used;
        if (hour < 0 && (used = digits(t, n, 1, 2, &a)) > 0 && (size_t)used < n && t[used] == ':') {
            int u2 = digits(t + used + 1, n - used - 1, 1, 2, &b);
            if (u2 > 0 && (size_t)(used + 1 + u2) < n && t[used + 1 + u2] == ':' &&
                digits(t + used + 2 + u2, n - used - 2 - u2, 1, 2, &c) > 0) {
                hour = a;
                minute = b;
                second = c;
                continue;
            }
        }
        if (day < 0 && digits(t, n, 1, 2, &a) > 0) {
            day = a;
            continue;
        }
        if (month < 0 && n >= 3) {
            for (int m = 0; m < 12; m++) {
                if (strncasecmp(t, months[m], 3) == 0) {
                    month = m + 1;
                    break;
                }
            }
            if (month > 0) {
                continue;
            }
        }
        if (year < 0 && digits(t, n, 2, 4, &a) > 0) {
            year = a;
        }
    }
    if (year >= 70 && year <= 99) {
        year += 1900;
    } else if (year >= 0 && year <= 69) {
        year += 2000;
    }
    if (hour < 0 || day < 1 || day > 31 || month < 1 || year < 1601 || hour > 23 || minute > 59 || second > 59) {
        return -1;
    }
    return days_from_civil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second;
}
