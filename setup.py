import os
import shlex

from setuptools import Extension, setup


def has_http3():
    dirs = [a[2:] for a in shlex.split(os.environ.get("LDFLAGS", "")) if a.startswith("-L")]
    dirs += ["/usr/local/lib", "/usr/local/lib64"]
    return any(os.path.exists(os.path.join(d, "libngtcp2_crypto_boringssl.a")) for d in dirs)


hx_libraries = ["z", "ssl", "crypto", "nghttp2"]
hx_macros = []
if has_http3():
    hx_libraries = ["ngtcp2_crypto_boringssl", "ngtcp2", "nghttp3"] + hx_libraries
    hx_macros = [("HX_H3", "1")]

compile_args = [
    "-Wall",
    "-Wextra",
    "-Wno-unused-parameter",
    "-Wno-missing-field-initializers",
    "-Wno-cast-function-type",
]
if os.environ.get("HTTPPERF_PROFILE"):
    compile_args += [
        "-g",
        "-fno-omit-frame-pointer",
        "-finstrument-functions",
        "-finstrument-functions-exclude-file-list=src/ae/,/usr/include/,include/python",
    ]

setup(
    ext_modules=[
        Extension(
            "httpperf._httpperf",
            sources=["src/httpperf.c", "src/ae/ae.c", "src/ae/zmalloc.c"],
            libraries=["curl"],
            extra_compile_args=compile_args,
        ),
        Extension(
            "httpperf._httpperf_hx",
            sources=[
                "src/hx/module.c",
                "src/ae/ae.c",
                "src/ae/zmalloc.c",
                "hx/src/client.c",
                "hx/src/cookie.c",
                "hx/src/decode.c",
                "hx/src/dns.c",
                "hx/src/h1.c",
                "hx/src/h3.c",
                "hx/src/tls.c",
                "hx/src/ws.c",
                "hx/vendor/llhttp/llhttp.c",
                "hx/vendor/llhttp/api.c",
                "hx/vendor/llhttp/http.c",
            ],
            libraries=hx_libraries,
            define_macros=hx_macros,
            extra_compile_args=compile_args,
        ),
    ]
)
