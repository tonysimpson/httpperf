PY_VERSION ?= 3.13
VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
C_SOURCES := src/httpperf.c src/hx/module.c
INSTALLED := $(VENV)/.installed
UV ?= $(or $(shell command -v uv),.tools/bin/uv)
GROUPS := --group dev $(if $(filter %t,$(PY_VERSION)),,--group http3)

.PHONY: venv test ci-local lint lint-fix clean distclean release

venv: $(INSTALLED)

$(INSTALLED): setup.py pyproject.toml $(wildcard src/*.c src/hx/*.c src/ae/*.c src/ae/*.h) | $(UV)
	test -x $(VENV)/bin/python || $(UV) venv -p $(PY_VERSION) $(VENV)
	$(UV) pip install --python $(VENV)/bin/python $(GROUPS) --reinstall-package httpperf -e .
	touch $@

.tools/bin/uv:
	python3 -m venv .tools
	.tools/bin/pip install uv

test: $(INSTALLED)
	$(PYTHON) -m pytest
	HTTPPERF_BACKEND=hx $(PYTHON) -m pytest -q

ci-local:
	tools/ci-local/run.sh $(PYTHONS)

lint: $(INSTALLED)
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	$(VENV)/bin/clang-format --dry-run --Werror $(C_SOURCES)

lint-fix: $(INSTALLED)
	$(PYTHON) -m ruff format .
	$(VENV)/bin/clang-format -i $(C_SOURCES)
	$(PYTHON) -m ruff check --fix .

clean:
	rm -rf build dist *.egg-info httpperf/_httpperf*.so .pytest_cache .ruff_cache uftrace.data massif.out* *.zst $(INSTALLED)
	find httpperf tests -name __pycache__ -prune -exec rm -rf {} +

distclean: clean
	rm -rf $(VENV) .tools

release:
	git diff --quiet HEAD || { echo "commit your changes first"; exit 1; }
	git fetch -q origin main && test "$$(git rev-parse HEAD)" = "$$(git rev-parse origin/main)" || { echo "HEAD is not origin/main"; exit 1; }
	v=$$(sed -n 's/^__version__ = "\(.*\)"/\1/p' httpperf/version.py); \
	git tag -a v$$v -m "httpperf $$v" && git push origin v$$v
