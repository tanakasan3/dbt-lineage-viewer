.PHONY: help install install-pipx dev clean test serve

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

help:
	@echo "dbt-lineage-viewer"
	@echo ""
	@echo "Usage:"
	@echo "  make dev          Create venv and install in editable mode"
	@echo "  make install      Install in existing venv (or global)"
	@echo "  make install-pipx Install via pipx (isolated)"
	@echo "  make serve        Run dev server (requires DBT_PROJECT_PATH)"
	@echo "  make clean        Remove venv and build artifacts"
	@echo ""
	@echo "Examples:"
	@echo "  make dev"
	@echo "  make serve DBT_PROJECT_PATH=~/my-dbt-project"

# Create venv and install in editable mode
dev: $(VENV)/bin/activate
	@echo "✓ Dev environment ready. Activate with: source $(VENV)/bin/activate"

$(VENV)/bin/activate:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]" 2>/dev/null || $(PIP) install -e .
	@touch $(VENV)/bin/activate

# Install in current environment (no venv)
install:
	pip install -e .

# Install via pipx (isolated)
install-pipx:
	pipx install -e . --force

# Run dev server
serve: $(VENV)/bin/activate
ifndef DBT_PROJECT_PATH
	@echo "Error: DBT_PROJECT_PATH not set"
	@echo "Usage: make serve DBT_PROJECT_PATH=/path/to/dbt/project"
	@exit 1
endif
	$(VENV)/bin/dbt-lineage serve $(DBT_PROJECT_PATH)

# Clean up
clean:
	rm -rf $(VENV)
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
