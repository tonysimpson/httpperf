#ifndef HX_NG_H
#define HX_NG_H

#include <nghttp2/nghttp2.h>

#if NGHTTP2_VERSION_NUM < 0x013d00
typedef ssize_t nghttp2_ssize;
#define nghttp2_session_mem_send2 nghttp2_session_mem_send
#define nghttp2_session_mem_recv2 nghttp2_session_mem_recv
#define nghttp2_submit_request2 nghttp2_submit_request
#define nghttp2_submit_response2 nghttp2_submit_response
#define nghttp2_data_provider2 nghttp2_data_provider
#endif

#endif
