#ifndef HX_TESTS_H3_SERVER_H
#define HX_TESTS_H3_SERVER_H

int h3_server_start(const char *cert, const char *key);
int h3_server_connections(void);
void h3_server_refuse_next_connection(void);
void h3_server_stop(void);

#endif
