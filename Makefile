PYTHON ?= python
C_SOURCES := src/acurl.c

.PHONY: lint lint-fix

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	clang-format --dry-run --Werror $(C_SOURCES)

lint-fix:
	$(PYTHON) -m ruff format .
	clang-format -i $(C_SOURCES)
	$(PYTHON) -m ruff check --fix .
