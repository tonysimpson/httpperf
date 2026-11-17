import subprocess
import sys

import pytest

import httpperf
from httpperf import _backend


def backend_in_subprocess(name):
    code = "import httpperf._backend as b; print(b.name, b.ext.__name__)"
    return subprocess.run([sys.executable, "-c", code], env={"HTTPPERF_BACKEND": name}, capture_output=True, text=True)


@pytest.mark.parametrize("name, module", [("curl", "httpperf._httpperf"), ("hx", "httpperf._httpperf_hx")])
def test_the_backend_comes_from_the_environment(name, module):
    assert backend_in_subprocess(name).stdout.split() == [name, module]


def test_an_unknown_backend_is_an_error():
    assert "HTTPPERF_BACKEND should be curl or hx" in backend_in_subprocess("nope").stderr


async def test_a_closed_session_refuses_requests():
    session = httpperf.Session()
    session.close()
    with pytest.raises(RuntimeError, match="session is closed"):
        await session.get("http://127.0.0.1:9/")


def test_the_version_names_the_backend(capsys):
    from httpperf._cli import main

    main(["--version"])
    assert ("libcurl" if _backend.name == "curl" else "(hx)") in capsys.readouterr().out
