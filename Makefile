PY_VERSION ?= 3.13
VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
C_SOURCES := src/acurl.c
INSTALLED := $(VENV)/.installed

.PHONY: venv test lint lint-fix clean distclean

venv: $(INSTALLED)

$(INSTALLED): setup.py pyproject.toml $(wildcard src/*.c src/ae/*.c src/ae/*.h)
	test -x $(VENV)/bin/python || uv venv -p $(PY_VERSION) $(VENV)
	uv pip install --python $(VENV)/bin/python --group dev --reinstall-package acurl -e .
	touch $@

test: $(INSTALLED)
	$(PYTHON) -m pytest

lint: $(INSTALLED)
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	$(VENV)/bin/clang-format --dry-run --Werror $(C_SOURCES)

lint-fix: $(INSTALLED)
	$(PYTHON) -m ruff format .
	$(VENV)/bin/clang-format -i $(C_SOURCES)
	$(PYTHON) -m ruff check --fix .

clean:
	rm -rf build dist *.egg-info acurl/_acurl*.so .pytest_cache .ruff_cache uftrace.data massif.out* *.zst $(INSTALLED)
	find acurl tests -name __pycache__ -prune -exec rm -rf {} +

distclean: clean
	rm -rf $(VENV)
