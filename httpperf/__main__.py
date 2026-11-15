import argparse
import asyncio
import inspect
import json
import math
import os
import resource
import runpy
import sys
import threading
import time
import traceback

import httpperf
from httpperf import _httpperf

try:
    import uvloop
except ImportError:
    uvloop = None

PERCENTILES = (50, 75, 90, 99, 99.9, 99.99)
HTTP_VERSIONS = {"1.1": "HTTP/1.1", "2": "HTTP/2", "3": "HTTP/3"}


class Usage(Exception):
    pass


class Histogram:
    __slots__ = ("counts", "n", "total", "max")

    def __init__(self):
        self.counts = [0] * 4800
        self.n = 0
        self.total = 0.0
        self.max = 0.0

    def record(self, seconds):
        us = int(seconds * 1e6)
        if us < 256:
            i = us if us > 0 else 0
        else:
            shift = us.bit_length() - 8
            i = (shift << 7) + (us >> shift)
        self.counts[i] += 1
        self.n += 1
        self.total += seconds
        if seconds > self.max:
            self.max = seconds

    def merge(self, other):
        self.counts = [a + b for a, b in zip(self.counts, other.counts)]
        self.n += other.n
        self.total += other.total
        self.max = max(self.max, other.max)

    def percentile(self, p):
        target = max(1, math.ceil(p / 100 * self.n))
        seen = 0
        for i, count in enumerate(self.counts):
            seen += count
            if seen >= target:
                return min(bucket_middle(i) / 1e6, self.max)
        return self.max

    def summary(self):
        if not self.n:
            return None
        result = {"count": self.n, "mean": self.total / self.n}
        result.update(("p%g" % p, self.percentile(p)) for p in PERCENTILES)
        result["max"] = self.max
        return result


def bucket_middle(i):
    if i < 256:
        return i + 0.5
    shift = (i >> 7) - 1
    return ((i - (shift << 7)) << shift) + (1 << shift) / 2


class Record:
    def __init__(self, plan):
        self.on = False
        self.over_capacity = False
        self.latency = Histogram()
        self.first_byte = Histogram()
        self.download = Histogram()
        self.client_lag = Histogram()
        self.queue_time = Histogram()
        self.send_lag = Histogram()
        self.connect = Histogram()
        self.tls = Histogram()
        self.per_second = [0] * max(1, math.ceil(plan.duration))
        self.t_meas = plan.t_meas
        self.errors = {}
        self.stats = []

    def done(self, now):
        i = int(now - self.t_meas)
        if i < len(self.per_second):
            self.per_second[i] += 1

    def error(self, error):
        key, message = error
        entry = self.errors.get(key)
        if entry is None:
            self.errors[key] = [1, message]
        else:
            entry[0] += 1

    def sample(self, engine):
        self.stats.append(engine.stats())


class Plan:
    def __init__(self, connections, rate, threads, duration, warmup):
        self.connections = connections
        self.rate = rate
        self.threads = threads
        self.duration = duration
        self.t_start = time.monotonic()
        self.t_meas = self.t_start + warmup
        self.t_end = self.t_meas + duration


def share(total, threads, index):
    return total // threads + (1 if index < total % threads else 0)


async def sleep_until(t):
    await asyncio.sleep(max(0.0, t - time.monotonic()))


class Worker:
    def __init__(self, index, args, request):
        self.index = index
        self.request = request
        self.url = args.url
        self.rec = None
        self.loop = uvloop.new_event_loop() if uvloop else asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, name="httpperf-cli-%d" % index, daemon=True)
        self.thread.start()
        cap = share(args.connections[0], args.threads, index) if args.rate else 0
        self.call(self.open(cap, args)).result()

    def call(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    async def open(self, cap, args):
        self.engine = httpperf.Engine(max_total_connections=cap)
        self.session = httpperf.Session(engine=self.engine, http_version=HTTP_VERSIONS[args.http], timeout=args.timeout)
        self.session.set_response_callback(self.on_response)

    async def close(self):
        self.session.close()
        self.engine.close()

    def shutdown(self):
        self.call(self.close()).result()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        self.loop.close()

    def on_response(self, r):
        rec = self.rec
        if rec is not None and rec.on:
            first_byte = r.starttransfer_time
            total = r.total_time
            queue_time = r.queue_time
            rec.first_byte.record(first_byte)
            rec.download.record(total - first_byte)
            rec.queue_time.record(queue_time)
            rec.client_lag.record(r.end_time - r.start_time - total - queue_time)
            connect = r.connect_time
            if connect:
                rec.connect.record(connect)
                tls = r.appconnect_time
                if tls:
                    rec.tls.record(tls - connect)

    async def attempt(self):
        try:
            r = await self.request(self.session, self.url)
        except httpperf.Timeout:
            return "timeout", "timeout"
        except httpperf.RequestError as e:
            return ("curl", e.code), str(e)
        except Exception as e:
            message = "%s: %s" % (type(e).__name__, e)
            return message, message
        if isinstance(r, httpperf.Response) and r.status_code >= 400:
            message = "HTTP %d" % r.status_code
            return message, message
        return None

    async def window(self, plan, rec):
        await sleep_until(plan.t_start)
        rec.sample(self.engine)
        await sleep_until(plan.t_meas)
        rec.sample(self.engine)
        rec.on = True
        await sleep_until(plan.t_end)
        rec.on = False
        rec.sample(self.engine)

    async def step(self, plan):
        rec = self.rec = Record(plan)
        window = asyncio.ensure_future(self.window(plan, rec))
        try:
            if plan.rate:
                await self.scheduled(plan, rec)
            else:
                await asyncio.gather(
                    *(self.lane(plan, rec) for _ in range(share(plan.connections, plan.threads, self.index)))
                )
            await window
        finally:
            window.cancel()
        rec.sample(self.engine)
        return rec

    async def lane(self, plan, rec):
        t_end = plan.t_end
        monotonic = time.monotonic
        while True:
            sent = monotonic()
            if sent >= t_end:
                return
            error = await self.attempt()
            if error is not None:
                await asyncio.sleep(0)
            if rec.on:
                done = monotonic()
                rec.done(done)
                if error is None:
                    rec.latency.record(done - sent)
                else:
                    rec.error(error)

    async def scheduled(self, plan, rec):
        pending = set()
        try:
            await self.schedule(plan, rec, pending)
        finally:
            for task in list(pending):
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def schedule(self, plan, rec, pending):
        loop = asyncio.get_running_loop()
        interval = plan.threads / plan.rate
        first = plan.t_start + self.index / plan.rate
        limit = max(1.0, plan.rate / plan.threads)
        i = 0
        while True:
            now = time.monotonic()
            due = first + i * interval
            while due <= now and due < plan.t_end:
                task = loop.create_task(self.timed(due, plan, rec))
                pending.add(task)
                task.add_done_callback(pending.discard)
                i += 1
                due = first + i * interval
            if due >= plan.t_end:
                break
            if len(pending) > limit:
                rec.over_capacity = True
                return
            await asyncio.sleep(max(0.0, due - time.monotonic()))
        await asyncio.gather(*pending, return_exceptions=True)

    async def timed(self, due, plan, rec):
        sent = time.monotonic()
        error = await self.attempt()
        done = time.monotonic()
        if rec.on:
            rec.done(done)
        if plan.t_meas <= due < plan.t_end:
            rec.send_lag.record(sent - due)
            if error is None:
                rec.latency.record(done - sent)
            else:
                rec.error(error)


def summarise(plan, recs, cpu, threads_cpu, cores_allowed):
    def merged(name):
        h = Histogram()
        for rec in recs:
            h.merge(getattr(rec, name))
        return h.summary()

    def diff(key, a, b):
        return sum(rec.stats[b][key] - rec.stats[a][key] for rec in recs)

    per_second = [sum(counts) for counts in zip(*(rec.per_second for rec in recs))]
    full_seconds = per_second[: int(plan.duration)] or per_second
    mean = sum(full_seconds) / len(full_seconds)
    cv = (math.sqrt(sum((x - mean) ** 2 for x in full_seconds) / len(full_seconds)) / mean) if mean else 0.0
    requests = sum(per_second)
    errors = {}
    for rec in recs:
        for key, (count, message) in rec.errors.items():
            entry = errors.setdefault(key, [0, message])
            entry[0] += count
    versions = {}
    for rec in recs:
        for name, count in rec.stats[2]["http_versions"].items():
            versions[name] = versions.get(name, 0) + count - rec.stats[1]["http_versions"][name]
    return {
        "connections": plan.connections,
        "rate": plan.rate,
        "duration": plan.duration,
        "requests": requests,
        "rps": requests / plan.duration,
        "rps_variation": cv,
        "errors": sum(count for count, _ in errors.values()),
        "error_kinds": sorted(({"error": m, "count": c} for c, m in errors.values()), key=lambda e: -e["count"]),
        "over_capacity": any(rec.over_capacity for rec in recs),
        "latency": merged("latency"),
        "first_byte": merged("first_byte"),
        "download": merged("download"),
        "client_lag": merged("client_lag"),
        "queue_time": merged("queue_time"),
        "send_lag": merged("send_lag"),
        "connect": merged("connect"),
        "tls": merged("tls"),
        "received_per_s": diff("bytes_received", 1, 2) / plan.duration,
        "sent_per_s": diff("bytes_sent", 1, 2) / plan.duration,
        "connections_opened": diff("connections", 0, 3),
        "connections_closed_measuring": diff("connections", 1, 2) - diff("connections_open", 1, 2),
        "connections_open": sum(rec.stats[3]["connections_open"] for rec in recs),
        "tls_handshakes": diff("tls_handshakes", 0, 3),
        "http_versions": {name: count for name, count in versions.items() if count},
        "cpu_cores": cpu / plan.duration,
        "cores_allowed": cores_allowed,
        "cpu_per_request": cpu / requests if requests else None,
        "busiest_thread": threads_cpu / plan.duration,
    }


def cpu_time():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def threads_cpu_time():
    ticks = os.sysconf("SC_CLK_TCK")
    times = {}
    try:
        for tid in os.listdir("/proc/self/task"):
            with open("/proc/self/task/%s/stat" % tid) as f:
                fields = f.read().rsplit(")", 1)[1].split()
            times[tid] = (int(fields[11]) + int(fields[12])) / ticks
    except OSError:
        pass
    return times


def busiest(before, after):
    return max((after[tid] - before[tid] for tid in after if tid in before), default=0.0)


def sleep_until_wall(t):
    delay = t - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def run_step(workers, plan, cores_allowed):
    futures = [w.call(w.step(plan)) for w in workers]
    try:
        sleep_until_wall(plan.t_meas)
        cpu0 = cpu_time()
        threads0 = threads_cpu_time()
        sleep_until_wall(plan.t_end)
        cpu1 = cpu_time()
        threads1 = threads_cpu_time()
        recs = [f.result() for f in futures]
    except BaseException:
        for f in futures:
            f.cancel()
        raise
    return summarise(plan, recs, cpu1 - cpu0, busiest(threads0, threads1), cores_allowed)


def kept_up(step):
    return step["rate"] is None or (not step["over_capacity"] and step["rps"] >= 0.99 * step["rate"])


def few_errors(step):
    return step["errors"] <= 0.01 * max(1, step["requests"])


def best_step(steps):
    candidates = [i for i, step in enumerate(steps) if kept_up(step) and few_errors(step)]
    if not candidates:
        return None
    if steps[0]["rate"] is None:
        return max(candidates, key=lambda i: steps[i]["rps"])
    return max(candidates, key=lambda i: steps[i]["rate"])


def client_saturated(step):
    latency = step["latency"]
    lag = step["client_lag"]
    if step["cpu_cores"] > 0.9 * step["cores_allowed"]:
        return "client saturated: used %.1f of %d cores, so the result describes the client, not the server" % (
            step["cpu_cores"],
            step["cores_allowed"],
        )
    if step["busiest_thread"] > 0.9:
        return (
            "client saturated: a client thread was %.0f%% busy, so the result describes the client, not the server"
            % (100 * step["busiest_thread"])
        )
    if latency and lag and lag["p99"] > 0.25 * latency["p99"]:
        return "client saturated: client lag p99 %s is over a quarter of latency p99 %s" % (
            fmt_time(lag["p99"]),
            fmt_time(latency["p99"]),
        )
    return None


def warnings_for(step):
    found = []
    saturated = client_saturated(step)
    if saturated:
        found.append(saturated)
    if not few_errors(step):
        found.append(
            "errors: %s of %s requests failed (%s)" % (fmt_int(step["errors"]), fmt_int(step["requests"]), kinds(step))
        )
    if step["rps_variation"] > 0.1:
        found.append("unsteady: req/s varied %.0f%% from second to second" % (100 * step["rps_variation"]))
    if step["connections_closed_measuring"]:
        found.append(
            "connection churn: %s connections closed while measuring, so latency includes opening new ones"
            % fmt_int(step["connections_closed_measuring"])
        )
    if step["over_capacity"]:
        found.append("over capacity: requests piled up, so the step stopped early")
    elif step["rate"] is not None and step["rps"] < 0.99 * step["rate"]:
        found.append("behind schedule: achieved %s of %s req/s" % (fmt_int(round(step["rps"])), fmt_int(step["rate"])))
    elif step["send_lag"] and step["send_lag"]["p99"] > 0.002:
        found.append(
            "behind schedule: send lag p99 %s, so the client sent requests late" % fmt_time(step["send_lag"]["p99"])
        )
    return found


def kinds(step):
    return ", ".join("%s × %s" % (fmt_int(e["count"]), e["error"]) for e in step["error_kinds"][:3])


def fmt_int(n):
    return "{:,}".format(n)


def fmt_time(seconds):
    if seconds is None:
        return "-"
    ms = seconds * 1000
    if ms < 10:
        return "%.2fms" % ms
    if ms < 100:
        return "%.1fms" % ms
    if ms < 1000:
        return "%.0fms" % ms
    return "%.2fs" % seconds


def fmt_rate(bytes_per_s):
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if bytes_per_s >= scale:
            return "%.3g %s/s" % (bytes_per_s / scale, unit)
    return "%.0f B/s" % bytes_per_s


def percentiles(summary, names):
    if summary is None:
        return "-"
    return "  ".join("%s %s" % (name, fmt_time(summary[name])) for name in names)


def step_label(step):
    if step["rate"] is not None:
        return "%s req/s" % fmt_int(step["rate"])
    return "%s connections" % fmt_int(step["connections"])


def table(steps, best):
    rate_mode = steps[0]["rate"] is not None
    head = ["rate", "achieved"] if rate_mode else ["connections", "req/s"]
    head += ["±/s", "p50", "p99", "p99.9", "max", "errors"]
    rows = [head]
    for step in steps:
        latency = step["latency"] or {}
        rps = "-" if step["over_capacity"] else fmt_int(round(step["rps"]))
        first = [fmt_int(step["rate"]), rps] if rate_mode else [fmt_int(step["connections"]), rps]
        rows.append(
            first
            + ["-" if step["over_capacity"] else "%.0f%%" % (100 * step["rps_variation"])]
            + [fmt_time(latency.get(name)) for name in ("p50", "p99", "p99.9", "max")]
            + [fmt_int(step["errors"])]
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(head))]
    lines = []
    for n, row in enumerate(rows):
        line = "  " + "  ".join(cell.rjust(width) for cell, width in zip(row, widths))
        if n - 1 == best:
            line += "  ← best"
        elif n and steps[n - 1]["over_capacity"]:
            line += "  over capacity"
        lines.append(line)
    return lines


def details(step):
    lines = []
    latency = step["latency"]
    lines.append("  latency      " + percentiles(latency, ("p50", "p90", "p99", "p99.9", "max")))
    lines.append("    first byte " + percentiles(step["first_byte"], ("p50", "p99")))
    lines.append("    download   " + percentiles(step["download"], ("p50", "p99")))
    if step["connect"]:
        lines.append("    connect    " + percentiles(step["connect"], ("p50", "p99")))
    if step["tls"]:
        lines.append("    TLS        " + percentiles(step["tls"], ("p50", "p99")))
    errors = "%s errors" % fmt_int(step["errors"])
    if step["error_kinds"]:
        errors += " (%s)" % kinds(step)
    lines.append("  requests     %s in %gs, %s" % (fmt_int(step["requests"]), step["duration"], errors))
    lines.append(
        "  transfer     received %s, sent %s" % (fmt_rate(step["received_per_s"]), fmt_rate(step["sent_per_s"]))
    )
    versions = ", ".join(sorted(step["http_versions"])) or "-"
    lines.append(
        "  connections  %s opened, %s open at the end, %s, %s TLS handshakes"
        % (
            fmt_int(step["connections_opened"]),
            fmt_int(step["connections_open"]),
            versions,
            fmt_int(step["tls_handshakes"]),
        )
    )
    per_request = step["cpu_per_request"]
    lines.append(
        "  client       %.1f of %d cores, %s CPU per request"
        % (step["cpu_cores"], step["cores_allowed"], "-" if per_request is None else "%.0fµs" % (per_request * 1e6))
    )
    lag = "               client lag p99 %s, connection wait p99 %s" % (
        fmt_time(step["client_lag"] and step["client_lag"]["p99"]),
        fmt_time(step["queue_time"] and step["queue_time"]["p99"]),
    )
    if step["send_lag"]:
        lag += ", send lag p99 %s" % fmt_time(step["send_lag"]["p99"])
    lines.append(lag)
    return lines


def header(args):
    v = _httpperf.libcurl_version_num
    lines = [
        "httpperf %s (libcurl %d.%d.%d), %d threads, %s"
        % (httpperf.__version__, v >> 16, (v >> 8) & 255, v & 255, args.threads, args.url)
    ]
    timing = "%gs per step after %gs warm-up" % (args.duration, args.warmup)
    if args.rate:
        lines.append("fixed rate, up to %s connections, %s" % (fmt_int(args.connections[0]), timing))
    else:
        lines.append("flat out, %s" % timing)
    return lines


def report(args, steps, out):
    best = best_step(steps)
    lines = header(args) + [""]
    if len(steps) > 1:
        lines += table(steps, best) + [""]
        if best is not None and steps[best]["rate"] is not None:
            lines.append("best: %s, the highest rate that kept up" % step_label(steps[best]))
        elif best is not None:
            lines.append("best: %s, %s req/s" % (step_label(steps[best]), fmt_int(round(steps[best]["rps"]))))
    shown = best if best is not None else len(steps) - 1
    if len(steps) == 1 or best is not None:
        lines += details(steps[shown])
    notes = []
    for step in steps:
        for warning in warnings_for(step):
            notes.append(("%s: %s" % (step_label(step), warning)) if len(steps) > 1 else warning)
    if len(steps) > 1 and steps[0]["rate"] is None and best == len(steps) - 1 and not client_saturated(steps[best]):
        notes.append("peak not reached: the best step was the last, so try more connections")
    if notes:
        lines.append("")
        lines += ["warning: " + note for note in notes]
    out.write("\n".join(lines) + "\n")


def duration(text):
    try:
        for suffix, scale in (("ms", 1e-3), ("s", 1), ("m", 60), ("h", 3600)):
            if text.endswith(suffix):
                value = float(text[: -len(suffix)]) * scale
                break
        else:
            value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("not a duration: %r (try 500ms, 10s or 2m)" % text) from None
    if value <= 0:
        raise argparse.ArgumentTypeError("must be more than zero: %r" % text)
    return value


def counts(text):
    try:
        values = [int(part) for part in text.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError("not a number or list of numbers: %r" % text) from None
    if any(v <= 0 for v in values):
        raise argparse.ArgumentTypeError("must be more than zero: %r" % text)
    return values


def parser():
    p = argparse.ArgumentParser(
        prog="httpperf",
        description="Load test an HTTP server.",
        epilog="Arguments after -- are passed to the script in sys.argv. See docs/cli.md.",
    )
    p.add_argument("url")
    p.add_argument(
        "-c",
        "--connections",
        type=counts,
        metavar="N[,N...]",
        help="requests in flight, or with --rate the connection cap",
    )
    p.add_argument(
        "-r", "--rate", type=counts, metavar="N[,N...]", help="requests per second in total, sent on a fixed schedule"
    )
    p.add_argument("-t", "--threads", type=int, metavar="N", default=2, help="client threads (default 2)")
    p.add_argument(
        "-d", "--duration", type=duration, metavar="T", default=10.0, help="measured time per step (default 10s)"
    )
    p.add_argument(
        "-w", "--warmup", type=duration, metavar="T", default=1.0, help="warm-up before each step (default 1s)"
    )
    p.add_argument("-H", "--header", action="append", default=[], help='"Name: value", repeatable')
    p.add_argument("-X", "--method", default="GET")
    p.add_argument("--body", metavar="DATA", help="request body, or @file")
    p.add_argument("--http", choices=sorted(HTTP_VERSIONS), default="2", help="HTTP version to ask for (default 2)")
    p.add_argument("--timeout", type=duration, metavar="T", default=2.0, help="request timeout (default 2s)")
    p.add_argument("-s", "--script", metavar="FILE", help="Python file defining async def request(session, url)")
    p.add_argument("--json", action="store_true", help="print the results as JSON")
    return p


def parse(argv):
    extra = []
    if "--" in argv:
        at = argv.index("--")
        argv, extra = argv[:at], argv[at + 1 :]
    args = parser().parse_args(argv)
    if args.threads < 1:
        raise Usage("--threads must be at least 1")
    if args.connections is None:
        args.connections = [100] if args.rate else [10]
    if args.rate and len(args.connections) > 1:
        raise Usage("with --rate, -c is the connection cap and takes one number")
    if args.script and (args.header or args.body is not None or args.method != "GET"):
        raise Usage("-H, -X and --body apply to the default request, not to scripts")
    args.extra = extra
    return args


def default_request(args):
    method = args.method.upper()
    headers = tuple(args.header) or None
    body = args.body
    if body is not None and body.startswith("@"):
        with open(body[1:], "rb") as f:
            body = f.read()
    elif body is not None:
        body = body.encode()

    async def request(session, url):
        return await session.request(method, url, headers_list=headers, data=body, body_to=httpperf.DISCARD)

    return request


def load_script(path, extra):
    sys.argv = [path] + extra
    namespace = runpy.run_path(path, run_name="__httpperf__")
    request = namespace.get("request")
    if not inspect.iscoroutinefunction(request):
        raise Usage("%s must define async def request(session, url)" % path)
    return request


def plans(args):
    if args.rate:
        for rate in args.rate:
            yield args.connections[0], rate
    else:
        for connections in args.connections:
            yield connections, None


def cores():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    try:
        args = parse(argv)
        request = load_script(args.script, args.extra) if args.script else default_request(args)
    except Usage as e:
        print("httpperf: %s" % e, file=sys.stderr)
        return 2
    except SystemExit as e:
        return e.code
    except Exception:
        traceback.print_exc()
        return 2
    workers = [Worker(i, args, request) for i in range(args.threads)]
    steps = []
    interrupted = False
    try:
        error = workers[0].call(workers[0].attempt()).result()
        if error is not None and (error[0] == "timeout" or isinstance(error[0], tuple)):
            print("httpperf: the first request failed: %s" % error[1], file=sys.stderr)
            return 2
        cores_allowed = cores()
        for connections, rate in plans(args):
            plan = Plan(connections, rate, args.threads, args.duration, args.warmup)
            steps.append(run_step(workers, plan, cores_allowed))
            if steps[-1]["over_capacity"]:
                break
    except KeyboardInterrupt:
        interrupted = True
    finally:
        for w in workers:
            w.shutdown()
    if steps:
        if args.json:
            json.dump({"steps": steps, "best": best_step(steps)}, sys.stdout, indent=2)
            sys.stdout.write("\n")
        else:
            report(args, steps, sys.stdout)
    if interrupted:
        return 130
    return 1 if any(step["errors"] or step["over_capacity"] for step in steps) else 0


if __name__ == "__main__":
    sys.exit(main())
