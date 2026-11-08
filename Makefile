# Export a large database table to a single Parquet file.
# Run `make help` to list every command.
#
# Quick start:
#   make setup                 # create .venv and install dependencies
#   make env                   # create .env from .env_template
#   $EDITOR .env               # set database credentials and table
#   make check                 # validate connection and query
#   make run                   # export (prompts for the output path)

SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
VENV   ?= .venv
PY     := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
MAIN   := main.py

# Optional overrides, e.g. `make export OUT=/tmp/db.parquet TABLE=other_table`
OUT         ?=
LIMIT       ?=
CHUNK_SIZE  ?=
COMPRESSION ?=
TABLE       ?=

BASE_FLAGS := $(if $(TABLE),--table $(TABLE)) \
              $(if $(CHUNK_SIZE),--chunk-size $(CHUNK_SIZE)) \
              $(if $(COMPRESSION),--compression $(COMPRESSION))
LIMIT_FLAG := $(if $(LIMIT),--limit $(LIMIT))

define require_setup
if [ ! -x "$(PY)" ]; then printf 'Missing %s. Run "make setup" first.\n' '$(VENV)' >&2; exit 1; fi
if [ ! -f .env ]; then printf 'Missing .env. Run "make env" and edit it first.\n' >&2; exit 1; fi
endef

.PHONY: help setup install env check doctor run export sample clean distclean

help: ## Show this help
	@printf 'Usage:\n  make <target> [VAR=value ...]\n\nTargets:\n'
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nVariables:\n'
	@printf '  OUT=path         Destination .parquet file (export and sample)\n'
	@printf '  LIMIT=n          Export only n rows (run and export)\n'
	@printf '  TABLE=name       Override DB_TABLE for a single run\n'
	@printf '  CHUNK_SIZE=n     Override DB_CHUNK_SIZE for a single run\n'
	@printf '  COMPRESSION=x    Override PARQUET_COMPRESSION for a single run\n'
	@printf '  VENV=dir         Virtualenv location (default: .venv)\n'
	@printf '\nExamples:\n'
	@printf '  make run\n'
	@printf '  make export OUT=/var/backups/vicidial/vicidial_log_archive.parquet\n'
	@printf '  make export OUT=/tmp/archive.parquet CHUNK_SIZE=250000 COMPRESSION=zstd\n'
	@printf '  make sample OUT=/tmp/archive_sample.parquet LIMIT=1000\n'

# --------------------------------------------------------------------------- #
# Environment
# --------------------------------------------------------------------------- #
setup: ## Create .venv and install the dependencies
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	@$(PY) -m pip install --upgrade pip
	@$(PIP) install -r requirements.txt
	@printf '\nEnvironment ready. Next: make env, then edit .env\n'

install: ## Reinstall the dependencies in the existing environment
	@$(PIP) install -r requirements.txt

env: ## Create .env from .env_template (never overwrites an existing .env)
	@if [ -f .env ]; then \
		echo '.env already exists - leaving it untouched.'; \
	else \
		cp .env_template .env; \
		echo '.env created. Edit it with your database credentials.'; \
	fi

# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
check: ## Validate .env, dependencies, connection and query
	@$(call require_setup)
	@$(PY) $(MAIN) --dry-run $(BASE_FLAGS)

doctor: ## Show tool versions and whether .env exists
	@if [ ! -x "$(PY)" ]; then printf 'Missing %s. Run "make setup" first.\n' '$(VENV)' >&2; exit 1; fi
	@$(PY) -c 'import sys, pandas, pyarrow, sqlalchemy, pymysql, dotenv; \
print("python      :", sys.version.split()[0]); \
print("pandas      :", pandas.__version__); \
print("pyarrow     :", pyarrow.__version__); \
print("sqlalchemy  :", sqlalchemy.__version__); \
print("pymysql     :", pymysql.__version__); \
print("dotenv      :", getattr(dotenv, "__version__", "installed"))'
	@if [ -f .env ]; then echo '.env        : present'; else echo '.env        : MISSING (run make env)'; fi

# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
run: ## Interactive export (asks for the destination path)
	@$(call require_setup)
	@$(PY) $(MAIN) $(OUT) $(BASE_FLAGS) $(LIMIT_FLAG)

export: ## Non-interactive export; requires OUT=/path/file.parquet
	@$(call require_setup)
	@if [ -z "$(OUT)" ]; then \
		printf 'OUT is required, e.g. make export OUT=/var/backups/vicidial/archive.parquet\n' >&2; \
		exit 1; \
	fi
	@$(PY) $(MAIN) "$(OUT)" --force $(BASE_FLAGS) $(LIMIT_FLAG)

sample: ## Small test export (OUT required, LIMIT defaults to 1000)
	@$(call require_setup)
	@if [ -z "$(OUT)" ]; then \
		printf 'OUT is required, e.g. make sample OUT=/tmp/archive_sample.parquet\n' >&2; \
		exit 1; \
	fi
	@$(PY) $(MAIN) "$(OUT)" --force --limit $(or $(LIMIT),1000) $(BASE_FLAGS)

# --------------------------------------------------------------------------- #
# Maintenance
# --------------------------------------------------------------------------- #
clean: ## Remove caches and stale .part files
	@find . -type d -name __pycache__ -prune -exec rm -rf {} +
	@find . -maxdepth 1 -type f -name '*.part' -delete
	@printf 'Cleaned caches and partial files.\n'

distclean: clean ## Remove the virtualenv too (.env and exports are kept)
	@rm -rf $(VENV)
	@printf 'Removed %s. Run "make setup" to recreate it.\n' '$(VENV)'
