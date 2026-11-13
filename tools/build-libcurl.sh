#!/bin/sh
# Builds nghttp2, nghttp3, ngtcp2 and libcurl into /usr/local inside the cibuildwheel containers.
set -eux

NGHTTP2_VERSION=1.70.0
NGHTTP2_SHA256=e05cb1388eaca3830aded4ccf20044b6e1ac1a61411dcca11b0437c4285c8bc2
NGHTTP3_VERSION=1.19.0
NGHTTP3_SHA256=228e17f5cc3d4de7bfced9912bd09de30e7f45741cc47503cf88d3fa9209e155
NGTCP2_VERSION=1.26.0
NGTCP2_SHA256=7804aa0fa1c39942ba322a90805824c37231f7675b08d0c3cc4332858a7b085e
CURL_VERSION=8.23.0
CURL_SHA256=e3de77cfa6367083c776a26647fc9f633b538c06c3baa73d931a252ac8f72b49

if command -v apk >/dev/null; then
    apk add --no-cache openssl-dev zlib-dev xz
else
    dnf install -y openssl-devel zlib-devel xz
fi

cd "$(mktemp -d)"
curl -fsSLo nghttp2.tar.xz "https://github.com/nghttp2/nghttp2/releases/download/v$NGHTTP2_VERSION/nghttp2-$NGHTTP2_VERSION.tar.xz"
echo "$NGHTTP2_SHA256  nghttp2.tar.xz" | sha256sum -c -
curl -fsSLo nghttp3.tar.xz "https://github.com/ngtcp2/nghttp3/releases/download/v$NGHTTP3_VERSION/nghttp3-$NGHTTP3_VERSION.tar.xz"
echo "$NGHTTP3_SHA256  nghttp3.tar.xz" | sha256sum -c -
curl -fsSLo ngtcp2.tar.xz "https://github.com/ngtcp2/ngtcp2/releases/download/v$NGTCP2_VERSION/ngtcp2-$NGTCP2_VERSION.tar.xz"
echo "$NGTCP2_SHA256  ngtcp2.tar.xz" | sha256sum -c -
curl -fsSLo curl.tar.gz "https://curl.se/download/curl-$CURL_VERSION.tar.gz"
echo "$CURL_SHA256  curl.tar.gz" | sha256sum -c -

tar xf nghttp2.tar.xz
(cd "nghttp2-$NGHTTP2_VERSION" &&
    ./configure --prefix=/usr/local --enable-lib-only --disable-static &&
    make -j"$(nproc)" && make install)

tar xf nghttp3.tar.xz
(cd "nghttp3-$NGHTTP3_VERSION" &&
    ./configure --prefix=/usr/local --enable-lib-only --disable-static &&
    make -j"$(nproc)" && make install)

tar xf ngtcp2.tar.xz
(cd "ngtcp2-$NGTCP2_VERSION" &&
    ./configure --prefix=/usr/local --enable-lib-only --disable-static --with-openssl &&
    make -j"$(nproc)" && make install)

tar xf curl.tar.gz
(cd "curl-$CURL_VERSION" &&
    PKG_CONFIG_PATH=/usr/local/lib/pkgconfig ./configure --prefix=/usr/local --disable-static \
        --with-openssl --with-nghttp2 --with-ngtcp2 --with-nghttp3 --with-zlib \
        --without-libpsl --without-libidn2 --without-brotli --without-zstd \
        --without-ca-bundle --without-ca-path \
        --disable-ldap --disable-docs &&
    make -j"$(nproc)" && make install)

if [ -d /etc/ld.so.conf.d ]; then
    echo /usr/local/lib > /etc/ld.so.conf.d/usr-local.conf
    ldconfig
fi
/usr/local/bin/curl --version
