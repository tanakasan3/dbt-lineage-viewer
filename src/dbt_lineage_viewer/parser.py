"""Parse DBT manifest.json and extract lineage graph."""

import json
from pathlib import Path
from typing import Any


def parse_manifest(manifest_path: Path) -> dict[str, Any]:
    """
    Parse manifest.json and return a graph structure for Cytoscape.
    
    Returns:
        {
            "nodes": [{"data": {"id": "...", "label": "...", "type": "...", ...}}],
            "edges": [{"data": {"source": "...", "target": "..."}}],
            "metadata": {...}
        }
    """
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    nodes = []
    edges = []
    node_ids = set()
    
    # Extract nodes from manifest
    # DBT manifest has: nodes, sources, exposures, metrics, seeds, snapshots
    
    # Process sources
    for source_key, source in manifest.get("sources", {}).items():
        node_id = source_key
        node_ids.add(node_id)
        nodes.append({
            "data": {
                "id": node_id,
                "label": f"{source.get('source_name', '')}.{source.get('name', '')}",
                "shortLabel": source.get("name", source_key),
                "type": "source",
                "resourceType": source.get("resource_type", "source"),
                "package": source.get("package_name", ""),
                "database": source.get("database", ""),
                "schema": source.get("schema", ""),
                "description": source.get("description", ""),
                "path": source.get("path", ""),
            }
        })
    
    # Process models, seeds, snapshots, tests
    for node_key, node in manifest.get("nodes", {}).items():
        resource_type = node.get("resource_type", "")
        
        # Skip tests - they clutter the graph
        if resource_type == "test":
            continue
        
        node_id = node_key
        node_ids.add(node_id)
        
        # Determine node type for styling
        node_type = _classify_node_type(node)
        
        nodes.append({
            "data": {
                "id": node_id,
                "label": node.get("name", node_key),
                "shortLabel": node.get("name", node_key),
                "type": node_type,
                "resourceType": resource_type,
                "package": node.get("package_name", ""),
                "database": node.get("database", ""),
                "schema": node.get("schema", ""),
                "description": node.get("description", ""),
                "path": node.get("original_file_path", node.get("path", "")),
                "rawCode": node.get("raw_code", node.get("raw_sql", "")),
                "compiledCode": node.get("compiled_code", node.get("compiled_sql", "")),
                "materialized": node.get("config", {}).get("materialized", ""),
                "tags": node.get("tags", []),
                "columns": _extract_columns(node),
            }
        })
        
        # Add edges from depends_on
        for dep in node.get("depends_on", {}).get("nodes", []):
            edges.append({
                "data": {
                    "source": dep,
                    "target": node_id,
                }
            })
    
    # Process exposures (downstream consumers)
    for exp_key, exposure in manifest.get("exposures", {}).items():
        node_id = exp_key
        node_ids.add(node_id)
        nodes.append({
            "data": {
                "id": node_id,
                "label": exposure.get("name", exp_key),
                "shortLabel": exposure.get("name", exp_key),
                "type": "exposure",
                "resourceType": "exposure",
                "package": exposure.get("package_name", ""),
                "description": exposure.get("description", ""),
                "exposureType": exposure.get("type", ""),
                "owner": exposure.get("owner", {}).get("name", ""),
                "url": exposure.get("url", ""),
            }
        })
        
        for dep in exposure.get("depends_on", {}).get("nodes", []):
            edges.append({
                "data": {
                    "source": dep,
                    "target": node_id,
                }
            })
    
    # Filter edges to only include existing nodes
    edges = [e for e in edges if e["data"]["source"] in node_ids and e["data"]["target"] in node_ids]
    
    # Build metadata
    metadata = {
        "dbtVersion": manifest.get("metadata", {}).get("dbt_version", "unknown"),
        "projectName": manifest.get("metadata", {}).get("project_name", "unknown"),
        "generatedAt": manifest.get("metadata", {}).get("generated_at", ""),
        "nodeCount": len(nodes),
        "edgeCount": len(edges),
    }
    
    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": metadata,
    }


def _classify_node_type(node: dict) -> str:
    """Classify node into type for styling: source, staging, intermediate, mart, output."""
    resource_type = node.get("resource_type", "")
    name = node.get("name", "").lower()
    path = node.get("original_file_path", node.get("path", "")).lower()
    
    if resource_type == "seed":
        return "seed"
    
    if resource_type == "snapshot":
        return "snapshot"
    
    # Check path-based classification
    if "staging" in path or "stg_" in name or name.startswith("stg"):
        return "staging"
    
    if "intermediate" in path or "int_" in name or name.startswith("int"):
        return "intermediate"
    
    if "mart" in path or "dim_" in name or "fct_" in name or "fact_" in name:
        return "mart"
    
    if "output" in path or "report" in name:
        return "output"
    
    if "static" in path:
        return "static"
    
    return "model"


def _extract_columns(node: dict) -> list[dict]:
    """Extract column information from node."""
    columns = []
    for col_name, col_info in node.get("columns", {}).items():
        columns.append({
            "name": col_name,
            "description": col_info.get("description", ""),
            "dataType": col_info.get("data_type", ""),
            "tags": col_info.get("tags", []),
        })
    return columns
