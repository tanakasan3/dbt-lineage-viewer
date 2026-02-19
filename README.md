# dbt-lineage-viewer

Interactive DBT lineage explorer with search and filtering. Parses `manifest.json` and renders an interactive DAG with Cytoscape.js + ELK layout.

## Installation

```bash
pip install git+https://github.com/tanakasan3/dbt-lineage-viewer.git
```

Or for development:

```bash
git clone https://github.com/tanakasan3/dbt-lineage-viewer.git
cd dbt-lineage-viewer

# Option 1: Makefile (creates venv automatically)
make dev
source .venv/bin/activate

# Option 2: pipx (isolated install)
make install-pipx

# Option 3: Manual venv
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Usage

### Quick Start

Point it at a DBT project directory (must have `target/manifest.json`):

```bash
# Generate manifest first (if not already present)
cd /path/to/dbt/project
dbt parse  # or dbt compile

# Launch viewer
dbt-lineage serve /path/to/dbt/project
```

### Options

```bash
dbt-lineage serve [OPTIONS] DBT_PROJECT_PATH

Options:
  -p, --port INTEGER      Port to run server on (default: 8142)
  -h, --host TEXT         Host to bind to (default: 127.0.0.1)
  --manifest PATH         Path to manifest.json (default: target/manifest.json)
  --open / --no-open      Auto-open browser (default: --open)
  --help                  Show this message and exit.
```

### Examples

```bash
# Serve with custom port
dbt-lineage serve ./my_dbt_project --port 3000

# Use specific manifest file
dbt-lineage serve . --manifest ./compiled/manifest.json

# Don't auto-open browser
dbt-lineage serve . --no-open
```

## Features

- **Search**: Filter nodes by name with real-time highlighting
- **Lineage Focus**: Click a node to highlight upstream/downstream dependencies
- **Depth Control**: Slider to limit lineage depth (1-hop, 2-hop, etc.)
- **Layer Coloring**: Sources (green), staging (blue), intermediate (yellow), outputs (red)
- **SQL Preview**: Click a node to view its SQL and metadata
- **ELK Layout**: Hierarchical left-to-right layout for clear lineage flow

## How It Works

1. Reads `manifest.json` from your DBT project's `target/` directory
2. Extracts nodes (models, sources, seeds, snapshots) and their dependencies
3. Serves a web UI with an interactive DAG visualization
4. All processing happens locally - no data leaves your machine

## Requirements

- Python 3.10+
- A DBT project with compiled `manifest.json`

## License

MIT
