import json
import sys

import pytest

from httpperf.__main__ import Histogram, duration, main

QUICK = ["-t", "1", "-w", "100ms", "-d", "300ms"]


def run(capsys, *args):
    code = main([*QUICK, "--json", *args])
    out = capsys.readouterr().out
    return code, json.loads(out)["steps"] if out else None


def script(tmp_path, source):
    path = tmp_path / "script.py"
    path.write_text(source)
    return str(path)


@pytest.mark.parametrize("text, seconds", [("500ms", 0.5), ("10s", 10), ("2m", 120), ("3", 3)])
def test_durations(text, seconds):
    assert duration(text) == seconds


def test_histogram_percentiles_are_within_one_percent():
    h = Histogram()
    for us in range(1, 100001):
        h.record(us / 1e6)
    assert h.percentile(99) == pytest.approx(0.099, rel=0.01)


def test_a_run_against_a_working_server_succeeds(capsys, server):
    assert main([*QUICK, server.url]) == 0


def test_requests_are_counted(capsys, server):
    _, steps = run(capsys, server.url)
    assert steps[0]["requests"] > 0


def test_each_connection_count_is_a_step(capsys, server):
    _, steps = run(capsys, "-c", "1,2,4", server.url)
    assert [step["connections"] for step in steps] == [1, 2, 4]


def test_a_fixed_rate_is_kept(capsys, server):
    _, steps = run(capsys, "--rate", "200", "-d", "1s", server.url)
    assert steps[0]["rps"] == pytest.approx(200, rel=0.05)


def test_an_error_status_is_an_error(capsys, httpbin):
    _, steps = run(capsys, httpbin.url + "/status/503")
    assert {e["error"] for e in steps[0]["error_kinds"]} == {"HTTP 503"}


def test_errors_set_the_exit_status(capsys, httpbin):
    assert run(capsys, httpbin.url + "/status/503")[0] == 1


def test_an_exception_in_a_script_is_an_error(capsys, server, tmp_path):
    path = script(tmp_path, "async def request(session, url):\n    raise ValueError('nope')\n")
    _, steps = run(capsys, "-s", path, server.url)
    assert {e["error"] for e in steps[0]["error_kinds"]} == {"ValueError: nope"}


def test_arguments_after_a_double_dash_reach_the_script(capsys, server, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", sys.argv)
    path = script(tmp_path, "import sys\n\nasync def request(session, url):\n    raise ValueError(sys.argv[1])\n")
    _, steps = run(capsys, "-s", path, server.url, "--", "hello")
    assert {e["error"] for e in steps[0]["error_kinds"]} == {"ValueError: hello"}


def test_a_script_without_a_request_coroutine_is_refused(capsys, server, tmp_path):
    assert main(["-s", script(tmp_path, "x = 1\n"), server.url]) == 2


def test_an_unreachable_server_exits_with_two(capsys):
    assert main([*QUICK, "http://127.0.0.1:1/"]) == 2


def test_requests_that_pile_up_are_over_capacity(capsys, server, tmp_path):
    path = script(tmp_path, "import asyncio\n\nasync def request(session, url):\n    await asyncio.sleep(2)\n")
    _, steps = run(capsys, "-s", path, "--rate", "50", "-d", "2s", server.url)
    assert steps[0]["over_capacity"]


def test_a_connection_list_with_a_rate_is_refused(capsys, server):
    assert main(["--rate", "100", "-c", "1,2", server.url]) == 2


def test_the_report_names_the_best_step(capsys, server):
    main([*QUICK, "-c", "1,2", server.url])
    assert "best: " in capsys.readouterr().out
