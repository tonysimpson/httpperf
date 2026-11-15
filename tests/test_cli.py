import json
import sys

import pytest

from httpperf._cli import Histogram, best_step, duration, main, notes

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


def test_a_fixed_rate_reports_send_lag(capsys, server):
    _, steps = run(capsys, "--rate", "200", server.url)
    assert steps[0]["send_lag"]["count"] > 0


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


def test_an_unreachable_server_is_an_error(capsys):
    assert main([*QUICK, "http://127.0.0.1:1/"]) == 1


def test_requests_that_pile_up_are_over_capacity(capsys, server, tmp_path):
    path = script(tmp_path, "import asyncio\n\nasync def request(session, url):\n    await asyncio.sleep(2)\n")
    _, steps = run(capsys, "-s", path, "--rate", "50", "-d", "2s", server.url)
    assert steps[0]["over_capacity"]


def test_a_connection_list_with_a_rate_is_refused(capsys, server):
    assert main(["--rate", "100", "-c", "1,2", server.url]) == 2


def test_the_header_is_printed_first(capsys, server):
    main([*QUICK, server.url])
    assert capsys.readouterr().out.startswith("Running 0.3s test @ " + server.url)


def test_the_report_names_the_best_step(capsys, server):
    main([*QUICK, "-c", "1,2", server.url])
    assert "Best: " in capsys.readouterr().out


LAGGING = {"client_lag": {"p50": 0.009, "p99": 0.009}}


def step(connections=10, rate=None, rps=1000.0, **overrides):
    result = {
        "connections": connections,
        "rate": rate,
        "rps": rps,
        "requests": int(rps),
        "errors": 0,
        "error_kinds": [],
        "rps_variation": 0.0,
        "over_capacity": False,
        "latency": {"p50": 0.01, "p99": 0.01},
        "client_lag": {"p50": 0.0, "p99": 0.0},
        "send_lag": None,
        "connections_closed_measuring": 0,
        "connections_used": connections,
        "http_versions": {"HTTP/1.1": 1},
    }
    result.update(overrides)
    return result


def test_the_best_step_is_the_fewest_connections_near_the_most_req_s():
    assert best_step([step(10, rps=500), step(50, rps=990), step(200, rps=1000)]) == 1


def test_a_step_with_many_errors_cannot_be_best():
    assert best_step([step(10, rps=900), step(50, rps=1000, errors=100)]) == 0


def test_the_best_rate_is_the_highest_that_kept_up():
    assert best_step([step(rate=1000, rps=1000), step(rate=2000, rps=2000), step(rate=4000, rps=3000)]) == 1


def test_a_warning_on_several_steps_is_one_line():
    steps = [step(10, **LAGGING), step(50, **LAGGING)]
    assert notes(steps, 1) == [
        "httpperf was the limit at 10 and 50 connections, not the server, "
        "so the server can do at least 1000 requests/sec."
    ]


def test_a_best_last_step_suggests_more_connections():
    assert notes([step(10, rps=500), step(50)], 1) == [
        "Requests/sec was still rising at the last step, so try more connections."
    ]


def test_time_waiting_on_python_means_httpperf_was_the_limit():
    lagging = step(latency={"p50": 0.01, "p99": 0.01}, client_lag={"p50": 0.006, "p99": 0.006})
    assert notes([lagging], None)[0].startswith("httpperf was the limit")


def test_idle_connections_are_called_out_when_httpperf_was_the_limit():
    found = notes([step(10000, connections_used=461, **LAGGING)], None)
    assert "Only 461 of 10000 connections were in use, because httpperf was the limit." in found
