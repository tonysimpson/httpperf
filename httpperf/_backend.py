import importlib
import os

MODULES = {"curl": "httpperf._httpperf", "hx": "httpperf._httpperf_hx"}

name = os.environ.get("HTTPPERF_BACKEND", "curl")
if name not in MODULES:
    raise ImportError(f"HTTPPERF_BACKEND should be curl or hx, not {name!r}")
ext = importlib.import_module(MODULES[name])
