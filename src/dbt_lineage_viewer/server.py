"""FastAPI server for DBT lineage viewer."""

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from .parser import parse_manifest

# Will be set by CLI or env var
_graph_data: dict[str, Any] | None = None
_manifest_path: Path | None = None

app = FastAPI(title="DBT Lineage Viewer")


@app.on_event("startup")
async def startup_event():
    """Initialize on startup if manifest path is set via env."""
    global _graph_data, _manifest_path
    manifest_env = os.environ.get("DBT_LINEAGE_MANIFEST")
    if manifest_env and _graph_data is None:
        _manifest_path = Path(manifest_env)
        if _manifest_path.exists():
            _graph_data = parse_manifest(_manifest_path)


def init_app(manifest_path: Path) -> None:
    """Initialize the app with manifest data."""
    global _graph_data, _manifest_path
    _manifest_path = manifest_path
    _graph_data = parse_manifest(manifest_path)


@app.get("/")
async def index() -> HTMLResponse:
    """Serve the main HTML page."""
    static_dir = Path(__file__).parent / "static"
    index_path = static_dir / "index.html"
    
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="index.html not found")
    
    return HTMLResponse(content=index_path.read_text())


@app.get("/app.js")
async def app_js() -> FileResponse:
    """Serve the JavaScript."""
    static_dir = Path(__file__).parent / "static"
    return FileResponse(static_dir / "app.js", media_type="application/javascript")


@app.get("/style.css")
async def style_css() -> FileResponse:
    """Serve the CSS."""
    static_dir = Path(__file__).parent / "static"
    return FileResponse(static_dir / "style.css", media_type="text/css")


@app.get("/api/graph")
async def get_graph() -> dict[str, Any]:
    """Return the full graph data for Cytoscape."""
    if _graph_data is None:
        raise HTTPException(status_code=500, detail="Graph not initialized")
    return _graph_data


@app.get("/api/node/{node_id:path}")
async def get_node(node_id: str) -> dict[str, Any]:
    """Return details for a specific node."""
    if _graph_data is None:
        raise HTTPException(status_code=500, detail="Graph not initialized")
    
    for node in _graph_data["nodes"]:
        if node["data"]["id"] == node_id:
            return node["data"]
    
    raise HTTPException(status_code=404, detail=f"Node {node_id} not found")


@app.get("/api/lineage/{node_id:path}")
async def get_lineage(node_id: str, depth: int = 10) -> dict[str, Any]:
    """
    Return upstream and downstream lineage for a node.
    
    Args:
        node_id: The node to get lineage for
        depth: Maximum depth to traverse (default 10)
    
    Returns:
        {
            "upstream": ["node_id", ...],
            "downstream": ["node_id", ...],
        }
    """
    if _graph_data is None:
        raise HTTPException(status_code=500, detail="Graph not initialized")
    
    # Build adjacency lists
    upstream_adj: dict[str, list[str]] = {}  # node -> its parents (what it depends on)
    downstream_adj: dict[str, list[str]] = {}  # node -> its children (what depends on it)
    
    for edge in _graph_data["edges"]:
        source = edge["data"]["source"]
        target = edge["data"]["target"]
        
        # source -> target means target depends on source
        # so source is upstream of target, target is downstream of source
        downstream_adj.setdefault(source, []).append(target)
        upstream_adj.setdefault(target, []).append(source)
    
    # BFS to find all upstream nodes
    upstream = set()
    queue = [(node_id, 0)]
    visited = {node_id}
    
    while queue:
        current, d = queue.pop(0)
        if d >= depth:
            continue
        for parent in upstream_adj.get(current, []):
            if parent not in visited:
                visited.add(parent)
                upstream.add(parent)
                queue.append((parent, d + 1))
    
    # BFS to find all downstream nodes
    downstream = set()
    queue = [(node_id, 0)]
    visited = {node_id}
    
    while queue:
        current, d = queue.pop(0)
        if d >= depth:
            continue
        for child in downstream_adj.get(current, []):
            if child not in visited:
                visited.add(child)
                downstream.add(child)
                queue.append((child, d + 1))
    
    return {
        "upstream": list(upstream),
        "downstream": list(downstream),
    }


@app.post("/api/reload")
async def reload_manifest() -> dict[str, Any]:
    """Reload the manifest file."""
    global _graph_data
    
    if _manifest_path is None:
        raise HTTPException(status_code=500, detail="Manifest path not set")
    
    if not _manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"Manifest not found: {_manifest_path}")
    
    _graph_data = parse_manifest(_manifest_path)
    
    return {
        "status": "reloaded",
        "metadata": _graph_data["metadata"],
    }
