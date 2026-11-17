#!/bin/sh
# Runs the CI test jobs locally with Podman: the manylinux_2_34 image and the bundled curl for each
# Python version, and "system" for Ubuntu 24.04's libcurl (CI's system-curl job, Python 3.13).
# Usage: tools/ci-local/run.sh [python version or system ...]; PYTEST_ARGS and JOBS (default 4) are optional.
set -eu

cd "$(dirname "$0")/../.."
LOGS=.ci-local

if [ "${1:-}" = "--one" ]; then
    if [ "$2" = system ]; then
        set -- "$1" "$2" httpperf-ci-system 3.13 "" ""
    else
        set -- "$1" "$2" httpperf-ci "$2" -I/opt/curl/include "-L/opt/curl/lib -Wl,-rpath,/opt/curl/lib"
    fi
    if podman run --rm -v "$PWD:/src:ro,Z" -v httpperf-ci-uv:/root/.cache/uv \
        -e PY_VERSION="$4" -e PYTEST_ARGS="${PYTEST_ARGS:-}" -e CFLAGS="$5" -e LDFLAGS="$6" \
        "$3" sh -c '
            set -e
            mkdir /work
            tar -C /src --exclude="./.venv*" --exclude=./.tools --exclude=./.ci-local --exclude=./build \
                --exclude="*.so" --exclude=__pycache__ -cf - . | tar -C /work -xf -
            cd /work
            make venv PY_VERSION=$PY_VERSION > /dev/null
            HTTPPERF_BACKEND=hx .venv/bin/python -m pytest -q -p no:cacheprovider $PYTEST_ARGS > /tmp/hx.log || { cat /tmp/hx.log; exit 1; }
            .venv/bin/python -m pytest -p no:cacheprovider $PYTEST_ARGS
        ' > "$LOGS/$2.log" 2>&1; then
        echo "$2: $(tail -n 1 "$LOGS/$2.log" | tr -d '=')"
    else
        echo "$2: FAILED, see $LOGS/$2.log: $(tail -n 1 "$LOGS/$2.log" | tr -d '=')"
    fi
    exit 0
fi

VERSIONS=${*:-3.8 3.9 3.10 3.11 3.12 3.13 3.14 3.14t 3.15 3.15t system}
mkdir -p "$LOGS"
cp tools/build-libcurl.sh tools/ci-local/build-libcurl.sh
podman build -t httpperf-ci tools/ci-local > "$LOGS/build.log" 2>&1 || { rm -f tools/ci-local/build-libcurl.sh; echo "image build failed, see $LOGS/build.log"; exit 1; }
rm tools/ci-local/build-libcurl.sh
podman build -t httpperf-ci-system -f tools/ci-local/Containerfile.system tools/ci-local >> "$LOGS/build.log" 2>&1 || { echo "image build failed, see $LOGS/build.log"; exit 1; }
printf '%s\n' $VERSIONS | xargs -P "${JOBS:-4}" -n 1 "$0" --one
