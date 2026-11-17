#!/bin/sh
# Builds AWS-LC, nghttp2, nghttp3, ngtcp2 and libcurl into $PREFIX (default /usr/local) inside the cibuildwheel containers.
set -eux

PREFIX=${PREFIX:-/usr/local}

AWSLC_VERSION=5.12.0
AWSLC_SHA256=52c7f98ffd296d267cc015ad4653d1ffae399db65b14421da4f5f7efc44becd2
NGHTTP2_VERSION=1.70.0
NGHTTP2_SHA256=e05cb1388eaca3830aded4ccf20044b6e1ac1a61411dcca11b0437c4285c8bc2
NGHTTP3_VERSION=1.19.0
NGHTTP3_SHA256=228e17f5cc3d4de7bfced9912bd09de30e7f45741cc47503cf88d3fa9209e155
NGTCP2_VERSION=1.26.0
NGTCP2_SHA256=7804aa0fa1c39942ba322a90805824c37231f7675b08d0c3cc4332858a7b085e
CURL_VERSION=8.23.0
CURL_SHA256=e3de77cfa6367083c776a26647fc9f633b538c06c3baa73d931a252ac8f72b49

if command -v apk >/dev/null; then
    apk add --no-cache zlib-dev xz patch
else
    dnf install -y zlib-devel xz patch
fi

cd "$(mktemp -d)"
curl -fsSLo aws-lc.tar.gz "https://github.com/aws/aws-lc/archive/refs/tags/v$AWSLC_VERSION.tar.gz"
echo "$AWSLC_SHA256  aws-lc.tar.gz" | sha256sum -c -
curl -fsSLo nghttp2.tar.xz "https://github.com/nghttp2/nghttp2/releases/download/v$NGHTTP2_VERSION/nghttp2-$NGHTTP2_VERSION.tar.xz"
echo "$NGHTTP2_SHA256  nghttp2.tar.xz" | sha256sum -c -
curl -fsSLo nghttp3.tar.xz "https://github.com/ngtcp2/nghttp3/releases/download/v$NGHTTP3_VERSION/nghttp3-$NGHTTP3_VERSION.tar.xz"
echo "$NGHTTP3_SHA256  nghttp3.tar.xz" | sha256sum -c -
curl -fsSLo ngtcp2.tar.xz "https://github.com/ngtcp2/ngtcp2/releases/download/v$NGTCP2_VERSION/ngtcp2-$NGTCP2_VERSION.tar.xz"
echo "$NGTCP2_SHA256  ngtcp2.tar.xz" | sha256sum -c -
curl -fsSLo curl.tar.gz "https://curl.se/download/curl-$CURL_VERSION.tar.gz"
echo "$CURL_SHA256  curl.tar.gz" | sha256sum -c -

tar xf aws-lc.tar.gz
cmake -S "aws-lc-$AWSLC_VERSION" -B aws-lc-build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_INSTALL_LIBDIR=lib -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=OFF -DBUILD_TOOL=OFF \
    -DDISABLE_GO=ON -DDISABLE_PERL=ON \
    -DENABLE_PRE_SONAME_BUILD=OFF -DENABLE_SYMBOL_VERSIONING=ON -DSYMBOL_VERSION_NAMESPACE=HTTPPERF
cmake --build aws-lc-build -j"$(nproc)"
cmake --install aws-lc-build

tar xf nghttp2.tar.xz
(cd "nghttp2-$NGHTTP2_VERSION" &&
    ./configure --prefix="$PREFIX" --enable-lib-only --disable-static &&
    make -j"$(nproc)" && make install)

tar xf nghttp3.tar.xz
(cd "nghttp3-$NGHTTP3_VERSION" &&
    ./configure --prefix="$PREFIX" --enable-lib-only --disable-static &&
    make -j"$(nproc)" && make install)

tar xf ngtcp2.tar.xz
(cd "ngtcp2-$NGTCP2_VERSION" &&
    ./configure --prefix="$PREFIX" --enable-lib-only --disable-static --with-boringssl \
        CFLAGS="${CFLAGS:--g -O2} -fPIC" CXXFLAGS="${CXXFLAGS:--g -O2} -fPIC" \
        BORINGSSL_CFLAGS="-I$PREFIX/include" BORINGSSL_LIBS="-L$PREFIX/lib -lssl -lcrypto" &&
    make -j"$(nproc)" && make install)

tar xf curl.tar.gz
# Backport of curl commit e234f3789 ("ws: mask outgoing payload in chunks", 2026-10-22, curl/curl#23567):
# 8.23 masks WebSocket sends a byte at a time, capping them at about 0.3 GB/s. The fix is in curl 8.24,
# so remove this patch when CURL_VERSION is bumped to 8.24 or later.
(cd "curl-$CURL_VERSION" && patch -p1 <<'PATCH'
--- a/lib/ws.c
+++ b/lib/ws.c
@@ -998,37 +998,78 @@ static CURLcode ws_enc_write_head(struct Curl_easy *data,
   return ws_enc_add_frame(data, enc, flags, payload_len, out);
 }
 
+/*
+ * Mask and append payload to the bufq in chunks.
+ *
+ * @unittest 2607
+ */
+UNITTEST CURLcode ws_enc_mask_write(const uint8_t *mask, unsigned int *pxori,
+                                    const uint8_t *buf, size_t len,
+                                    struct bufq *out, size_t *pnwritten);
+UNITTEST CURLcode ws_enc_mask_write(const uint8_t *mask, unsigned int *pxori,
+                                    const uint8_t *buf, size_t len,
+                                    struct bufq *out, size_t *pnwritten)
+{
+  CURLcode result;
+  size_t i, done = 0;
+
+  *pnwritten = 0;
+  while(done < len) {
+    uint8_t tmp[1024];
+    uint8_t mask8[8];
+    uint64_t m;
+    size_t n = CURLMIN(len - done, sizeof(tmp));
+    size_t nwritten;
+
+    for(i = 0; i < 8; i++)
+      mask8[i] = mask[(*pxori + i) & 3];
+    memcpy(&m, mask8, 8);
+    for(i = 0; i + 8 <= n; i += 8) {
+      uint64_t v;
+      memcpy(&v, &buf[done + i], 8);
+      v ^= m;
+      memcpy(&tmp[i], &v, 8);
+    }
+    for(; i < n; i++)
+      tmp[i] = buf[done + i] ^ mask8[i & 7];
+
+    result = Curl_bufq_write(out, tmp, n, &nwritten);
+    if(result) {
+      if((result != CURLE_AGAIN) || !done)
+        return result;
+      break;
+    }
+    done += nwritten;
+    *pxori = (*pxori + (unsigned int)nwritten) & 3;
+    if(nwritten < n)
+      break;
+  }
+  *pnwritten = done;
+  return CURLE_OK;
+}
+
 static CURLcode ws_enc_write_payload(struct ws_encoder *enc,
                                      struct Curl_easy *data,
                                      const uint8_t *buf, size_t buflen,
                                      struct bufq *out, size_t *pnwritten)
 {
   CURLcode result;
-  size_t i, len, n, remain;
+  size_t len, remain;
 
   *pnwritten = 0;
   if(Curl_bufq_is_full(out))
     return CURLE_AGAIN;
 
-  /* not the most performant way to do this */
   len = buflen;
   remain = curlx_sotouz_range(enc->payload_remain, 0, SIZE_MAX);
   if(remain < len)
     len = remain;
 
-  for(i = 0; i < len; ++i) {
-    uint8_t c = buf[i] ^ enc->mask[enc->xori];
-    result = Curl_bufq_write(out, &c, 1, &n);
-    if(result) {
-      if((result != CURLE_AGAIN) || !i)
-        return result;
-      break;
-    }
-    enc->xori++;
-    enc->xori &= 3;
-  }
-  *pnwritten = i;
-  enc->payload_remain -= (curl_off_t)i;
+  result = ws_enc_mask_write(enc->mask, &enc->xori, buf, len, out,
+                             pnwritten);
+  if(result)
+    return result;
+  enc->payload_remain -= (curl_off_t)*pnwritten;
   ws_enc_info(enc, data, "buffered");
   return CURLE_OK;
 }
PATCH
)
(cd "curl-$CURL_VERSION" &&
    PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig" ./configure --prefix="$PREFIX" --disable-static \
        --with-openssl="$PREFIX" --with-nghttp2 --with-ngtcp2 --with-nghttp3 --with-zlib \
        --without-libpsl --without-libidn2 --without-brotli --without-zstd \
        --without-ca-bundle --without-ca-path \
        --disable-ldap --disable-docs &&
    make -j"$(nproc)" && make install)

if [ -d /etc/ld.so.conf.d ]; then
    echo "$PREFIX/lib" > /etc/ld.so.conf.d/httpperf-curl.conf
    ldconfig
fi
"$PREFIX/bin/curl" --version
