import os
import shlex
import subprocess
import sysconfig
import tempfile

from setuptools import Extension, setup

OPENSSL_HTTP3 = ["ngtcp2_crypto_ossl", "ngtcp2", "nghttp3"]


def has_boringssl_http3():
    dirs = [a[2:] for a in shlex.split(os.environ.get("LDFLAGS", "")) if a.startswith("-L")]
    dirs += ["/usr/local/lib", "/usr/local/lib64"]
    return any(os.path.exists(os.path.join(d, "libngtcp2_crypto_boringssl.a")) for d in dirs)


def has_openssl_http3():
    cc = shlex.split(os.environ.get("CC") or sysconfig.get_config_var("CC") or "cc")
    flags = shlex.split(os.environ.get("CFLAGS", ""))
    libs = shlex.split(os.environ.get("LDFLAGS", "")) + ["-l" + name for name in OPENSSL_HTTP3 + ["ssl", "crypto"]]
    with tempfile.TemporaryDirectory() as tmp:
        main = os.path.join(tmp, "main.c")
        with open(main, "w") as f:
            f.write("int hx_h3_available(void);\nint main(void) { return !hx_h3_available(); }\n")
        command = cc + flags + ["-DHX_H3", "-DHX_H3_OSSL", "hx/src/h3.c", main, "-o", os.path.join(tmp, "probe")] + libs
        try:
            return subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        except OSError:
            return False


hx_libraries = ["z", "ssl", "crypto", "nghttp2"]
hx_macros = []
if has_boringssl_http3():
    hx_libraries = ["ngtcp2_crypto_boringssl", "ngtcp2", "nghttp3"] + hx_libraries
    hx_macros = [("HX_H3", "1")]
elif has_openssl_http3():
    hx_libraries = OPENSSL_HTTP3 + hx_libraries
    hx_macros = [("HX_H3", "1"), ("HX_H3_OSSL", "1")]

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
