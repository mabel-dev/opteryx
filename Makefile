# Modernized Makefile for Opteryx
# Use bash shell for consistency across environments
SHELL := /bin/bash

# Variables
PYTHON := PYTHON_GIL=0 python
UV := $(PYTHON) -m uv
PIP := $(UV) pip
PYTEST := $(PYTHON) -m pytest
COVERAGE := $(PYTHON) -m coverage
MYPY := $(PYTHON) -m mypy

# Parallel job count for compilation
JOBS := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Directories
SRC_DIR := opteryx
TEST_DIR := tests
BUILD_DIR := build
DIST_DIR := dist

# Colors for output (using echo -e for proper ANSI handling)
define print_green
	@echo -e "\033[0;32m$(1)\033[0m"
endef

define print_blue
	@echo -e "\033[0;34m$(1)\033[0m"
endef

define print_yellow
	@echo -e "\033[1;33m$(1)\033[0m"
endef

define print_red
	@echo -e "\033[0;31m$(1)\033[0m"
endef

.PHONY: help lint format check test test-quick test-battery coverage mypy compile clean install update dev-install all

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	$(call print_green,"Opteryx Development Makefile")
	$(call print_blue,"Available targets:")
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[1;33m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# === LINTING AND FORMATTING ===

lint: ## Run all linting tools
	$(call print_blue,"Installing linting tools...")
	@$(PIP) install --quiet --upgrade pycln isort ruff yamllint cython-lint
	$(call print_blue,"Removing whitespace in pyx files...")
	@$(PYTHON) dev/fix_cython_whitespace.py
	$(call print_blue,"Running Cython lint...")
	@cython-lint $(SRC_DIR)/compiled/**/*.pyx || true
	$(call print_blue,"Running Ruff checks...")
	@$(PYTHON) -m ruff check --fix --exit-zero
	$(call print_blue,"Cleaning unused imports...")
	@$(PYTHON) -m pycln .
	$(call print_blue,"Sorting imports...")
	@$(PYTHON) -m isort .
	$(call print_blue,"Formatting code...")
	@$(PYTHON) -m ruff format $(SRC_DIR)
	$(call print_green,"Linting complete!")

format: ## Format code only
	$(call print_blue,"Formatting code...")
	@$(PYTHON) -m ruff format $(SRC_DIR)
	@$(PYTHON) -m isort .

check: ## Check code without fixing
	$(call print_blue,"Checking code style...")
	@$(PYTHON) -m ruff check
	@$(PYTHON) -m isort --check-only .

# === DEPENDENCIES ===

install: ## Install package in development mode
	$(call print_blue,"Installing package...")
	@$(PIP) install -e .

dev-install: ## Install development dependencies
	$(call print_blue,"Installing development dependencies...")
	@$(PIP) install --upgrade pip uv
	@$(PIP) install --upgrade -r tests/requirements.txt

update: ## Update all dependencies
	$(call print_blue,"Updating dependencies...")
	@$(PYTHON) -m pip install --upgrade pip uv
	@$(UV) pip install --upgrade -r tests/requirements.txt
	@$(UV) pip install --upgrade -r pyproject.toml

# === TESTING ===

test: dev-install ## Run full test suite
	$(call print_blue,"Running full test suite...")
	@$(PIP) install --upgrade pytest pytest-xdist
	@clear
	@MANUAL_TEST=1 $(PYTEST) -n auto --color=yes

test-quick: ## Run quick test (alias: t)
	@clear
	@$(PYTHON) tests/integration/sql_battery/run_shapes_battery.py

b:
	@clear
	@$(PYTHON) scratch/brace.py

clickbench:
	@clear
	@$(PYTHON) tests/performance/clickbench/clickbench.py

# Aliases for backward compatibility
t: test-quick

coverage: ## Generate test coverage report
	$(call print_blue,"Running coverage analysis...")
	@$(PIP) install --upgrade coverage pytest
	@clear
	@MANUAL_TEST=1 $(COVERAGE) run -m pytest --color=yes
	@$(COVERAGE) report --include=$(SRC_DIR)/** --fail-under=80 -m
	@$(COVERAGE) html --include=$(SRC_DIR)/**
	$(call print_green,"Coverage report generated in htmlcov/")

# === TYPE CHECKING ===

mypy: ## Run type checking
	$(call print_blue,"Running type checking...")
	@$(PIP) install --upgrade mypy
	@clear
	@$(MYPY) --ignore-missing-imports --python-version 3.11 --no-strict-optional --check-untyped-defs $(SRC_DIR)

# === COMPILATION ===

compile: clean ## Compile Cython extensions
	$(call print_blue,"Compiling Cython extensions...")
	@$(PIP) install --upgrade pip uv numpy cython setuptools setuptools_rust
	@$(PYTHON) setup.py clean
	@$(PYTHON) setup.py build_ext --inplace -j $(JOBS)
	$(call print_green,"Compilation complete!")

compile-quick: ## Quick compilation (alias: c)
	@$(PYTHON) setup.py build_ext --inplace

# Alias for backward compatibility
c: compile-quick

# === CLEANUP ===

clean: ## Clean build artifacts
	$(call print_blue,"Cleaning build artifacts...")
	@find . -name '*.so' -delete
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.egg-info' -type d -exec rm -rf {} + 2>/dev/null || true
	@rm -rf $(BUILD_DIR) $(DIST_DIR) .coverage htmlcov/ .pytest_cache/
	$(call print_green,"Cleanup complete!")

distclean: clean ## Deep clean including compiled extensions
	$(call print_blue,"Deep cleaning...")
	@find . -name '*.so' -delete
	@find . -name '*.c' -path '*/opteryx/compiled/*' -delete

# === CONVENIENCE TARGETS ===

all: clean dev-install lint mypy test compile ## Run complete development workflow

check-all: lint mypy test coverage ## Run all checks without compilation
