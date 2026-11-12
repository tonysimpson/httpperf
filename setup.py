import os

from setuptools import Extension, setup

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
        )
    ]
)
