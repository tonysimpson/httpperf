from __future__ import absolute_import, division, print_function

import os

from setuptools import setup
from setuptools.extension import Extension

with open(os.path.join("acurl", "version.py")) as f:
    exec(f.read())


compile_args = [
    "-Wall",
    "-Wextra",
    "-Wno-unused-parameter",
    "-Wno-missing-field-initializers",
    "-Wno-cast-function-type",
]
if os.environ.get("ACURL_PROFILE"):
    compile_args += [
        "-g",
        "-fno-omit-frame-pointer",
        "-finstrument-functions",
        "-finstrument-functions-exclude-file-list=src/ae/,/usr/include/,include/python",
    ]

# Building without nanoconfig
cpy_extension = Extension(
    "acurl._acurl",
    sources=["src/acurl.c", "src/ae/ae.c", "src/ae/zmalloc.c"],
    libraries=["curl"],
    extra_compile_args=compile_args,
    # extra_compile_args=['-g', '-fno-omit-frame-pointer', '-O0'], # used for performance/debug
)


install_requires = ["ujson"]


setup(
    name="acurl",
    version=__version__,
    packages=["acurl"],
    ext_modules=[cpy_extension],
    install_requires=install_requires,
    description="An async Curl library.",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    python_requires=">=3.8",
    author="Tony Simpson",
    author_email="agjasimpson@gmail.com",
    license="MIT",
)
