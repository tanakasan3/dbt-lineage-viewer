#!/usr/bin/env python3
"""
Generate a mock manifest.json from a DBT project by parsing SQL files.
This is a fallback when DBT isn't available.
"""

import json
import re
import sys
from pathlib import Path
from datetime import datetime


def extract_refs(sql: str) -> list[str]:
    """Extract model names from ref() calls."""
    pattern = r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}"
    return re.findall(pattern, sql)


def extract_sources(sql: str) -> list[tuple[str, str]]:
    """Extract (source_name, table_name) from source() calls."""
    pattern = r"\{\{\s*source\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}"
    return re.findall(pattern, sql)


def generate_manifest(models_dir: Path, project_name: str = "lana_dw") -> dict:
    """Generate a manifest-like structure from SQL files."""
    nodes = {}
    sources = {}
    
    # Track all source references to create source nodes
    all_sources: set[tuple[str, str]] = set()
    
    # First pass: collect all models and their refs
    model_files = list(models_dir.rglob("*.sql"))
    
    for sql_file in model_files:
        model_name = sql_file.stem
        rel_path = sql_file.relative_to(models_dir)
        sql_content = sql_file.read_text()
        
        # Extract dependencies
        refs = extract_refs(sql_content)
        srcs = extract_sources(sql_content)
        all_sources.update(srcs)
        
        # Build depends_on
        depends_on_nodes = []
        for ref_name in refs:
            depends_on_nodes.append(f"model.{project_name}.{ref_name}")
        for src_name, tbl_name in srcs:
            depends_on_nodes.append(f"source.{project_name}.{src_name}.{tbl_name}")
        
        node_id = f"model.{project_name}.{model_name}"
        nodes[node_id] = {
            "name": model_name,
            "resource_type": "model",
            "package_name": project_name,
            "path": str(rel_path),
            "original_file_path": f"models/{rel_path}",
            "database": "analytics",
            "schema": "public",
            "raw_code": sql_content,
            "compiled_code": sql_content,  # Same as raw for mock
            "depends_on": {
                "nodes": depends_on_nodes,
                "macros": [],
            },
            "config": {
                "materialized": "view",
            },
            "tags": [],
            "columns": {},
            "description": "",
        }
    
    # Create source nodes
    for src_name, tbl_name in all_sources:
        source_id = f"source.{project_name}.{src_name}.{tbl_name}"
        sources[source_id] = {
            "name": tbl_name,
            "source_name": src_name,
            "resource_type": "source",
            "package_name": project_name,
            "path": "",
            "database": src_name,
            "schema": "raw",
            "description": f"Source table from {src_name}",
        }
    
    # Check for seeds
    seeds_dir = models_dir.parent / "seeds"
    if seeds_dir.exists():
        for seed_file in seeds_dir.rglob("*.csv"):
            seed_name = seed_file.stem
            node_id = f"seed.{project_name}.{seed_name}"
            nodes[node_id] = {
                "name": seed_name,
                "resource_type": "seed",
                "package_name": project_name,
                "path": str(seed_file.relative_to(seeds_dir.parent)),
                "original_file_path": str(seed_file.relative_to(seeds_dir.parent)),
                "database": "analytics",
                "schema": "seeds",
                "depends_on": {"nodes": [], "macros": []},
                "config": {},
                "tags": [],
                "columns": {},
                "description": "",
            }
    
    manifest = {
        "metadata": {
            "dbt_version": "1.x.x (mock)",
            "project_name": project_name,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        },
        "nodes": nodes,
        "sources": sources,
        "exposures": {},
        "metrics": {},
    }
    
    return manifest


def main():
    if len(sys.argv) < 2:
        print("Usage: generate_mock_manifest.py <dbt_project_path> [output_path]")
        sys.exit(1)
    
    project_path = Path(sys.argv[1])
    models_dir = project_path / "models"
    
    if not models_dir.exists():
        print(f"Models directory not found: {models_dir}")
        sys.exit(1)
    
    # Try to get project name from dbt_project.yml
    project_name = "dbt_project"
    dbt_project_file = project_path / "dbt_project.yml"
    if dbt_project_file.exists():
        import yaml
        with open(dbt_project_file) as f:
            config = yaml.safe_load(f)
            project_name = config.get("name", project_name)
    
    manifest = generate_manifest(models_dir, project_name)
    
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else project_path / "target" / "manifest.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Generated manifest with {len(manifest['nodes'])} nodes and {len(manifest['sources'])} sources")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
