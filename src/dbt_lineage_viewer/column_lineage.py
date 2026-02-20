"""Extract column-level lineage from SQL using sqlglot."""

import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import build_scope


def strip_jinja(sql: str) -> str:
    """
    Strip Jinja templates from SQL, replacing refs/sources with table names.
    
    Handles:
    - {{ ref("model_name") }} -> model_name
    - {{ ref('model_name') }} -> model_name
    - {{ source("source", "table") }} -> source__table
    - {{ config(...) }} -> (removed)
    - {# comment #} -> (removed)
    """
    # Remove Jinja comments
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)
    
    # Remove config blocks
    sql = re.sub(r'\{\{\s*config\s*\([^)]*\)\s*\}\}', '', sql, flags=re.DOTALL)
    
    # Replace ref() with the model name
    # Handles: {{ ref("name") }}, {{ ref('name') }}, {{ ref("name", v=1) }}
    def replace_ref(match):
        content = match.group(1)
        # Extract first string argument
        str_match = re.search(r'''["']([^"']+)["']''', content)
        if str_match:
            return str_match.group(1)
        return 'unknown_ref'
    
    sql = re.sub(r'\{\{\s*ref\s*\(([^)]+)\)\s*\}\}', replace_ref, sql)
    
    # Replace source() with source__table
    def replace_source(match):
        content = match.group(1)
        # Extract both string arguments
        strings = re.findall(r'''["']([^"']+)["']''', content)
        if len(strings) >= 2:
            return f"{strings[0]}__{strings[1]}"
        elif len(strings) == 1:
            return strings[0]
        return 'unknown_source'
    
    sql = re.sub(r'\{\{\s*source\s*\(([^)]+)\)\s*\}\}', replace_source, sql)
    
    # Remove any remaining {{ ... }} blocks
    sql = re.sub(r'\{\{[^}]*\}\}', '', sql)
    
    return sql


@dataclass
class ColumnSource:
    """A source column that contributes to a target column."""
    table: str  # Table/model name or alias
    column: str  # Column name
    resolved_table: str | None = None  # Resolved actual table name
    transformation: str | None = None  # e.g., "SUM", "CONCAT", etc.


@dataclass
class ColumnLineage:
    """Lineage information for a single output column."""
    column: str  # Output column name
    sources: list[ColumnSource] = field(default_factory=list)
    expression: str | None = None  # The SQL expression if derived
    is_derived: bool = False  # True if computed (not direct reference)


def detect_dialect(sql: str) -> str:
    """
    Auto-detect SQL dialect from syntax hints.
    
    Returns dialect name for sqlglot.
    """
    # BigQuery indicators
    if '`' in sql:  # Backtick quoting
        return "bigquery"
    if "SAFE_OFFSET" in sql.upper() or "SAFE_DIVIDE" in sql.upper():
        return "bigquery"
    if "STRUCT<" in sql.upper() or "ARRAY<" in sql.upper():
        return "bigquery"
    
    # Snowflake indicators
    if "FLATTEN(" in sql.upper():
        return "snowflake"
    if "$$" in sql:  # Dollar quoting often in Snowflake
        return "snowflake"
    
    # Default to postgres (most compatible)
    return "postgres"


def extract_column_lineage(
    sql: str,
    dialect: str | None = None
) -> dict[str, ColumnLineage]:
    """
    Parse SQL and extract column-level lineage.
    
    Args:
        sql: The compiled SQL to parse (may contain Jinja)
        dialect: SQL dialect (postgres, bigquery, snowflake, etc.)
                 If None, auto-detects from SQL syntax.
    
    Returns:
        Dict mapping output column names to their lineage info
    """
    # Strip Jinja templates first
    sql = strip_jinja(sql)
    
    # Auto-detect dialect if not specified
    if dialect is None:
        dialect = detect_dialect(sql)
    
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        # If parsing fails with detected dialect, try postgres as fallback
        if dialect != "postgres":
            try:
                parsed = sqlglot.parse_one(sql, dialect="postgres")
            except Exception:
                return {}
        else:
            return {}
    
    if not isinstance(parsed, exp.Select):
        # Handle CTEs - get the final SELECT
        if isinstance(parsed, exp.Union):
            # For UNION, analyze the first select
            parsed = parsed.this
        elif hasattr(parsed, 'this') and isinstance(parsed.this, exp.Select):
            parsed = parsed.this
        else:
            return {}
    
    # Build scope for resolving table references
    try:
        root_scope = build_scope(parsed)
    except Exception:
        root_scope = None
    
    # Extract table aliases from FROM/JOIN
    table_aliases = _extract_table_aliases(parsed)
    
    # Analyze each SELECT expression
    result: dict[str, ColumnLineage] = {}
    
    for select_expr in parsed.expressions:
        lineage = _analyze_select_expression(select_expr, table_aliases, root_scope)
        if lineage:
            result[lineage.column] = lineage
    
    return result


def _extract_table_aliases(select: exp.Select) -> dict[str, str]:
    """Extract table alias -> actual table name mappings."""
    aliases = {}
    
    # FROM clause
    from_clause = select.find(exp.From)
    if from_clause:
        _extract_table_from_expression(from_clause.this, aliases)
    
    # JOIN clauses
    for join in select.find_all(exp.Join):
        _extract_table_from_expression(join.this, aliases)
    
    return aliases


def _extract_table_from_expression(expr: exp.Expression, aliases: dict[str, str]) -> None:
    """Extract table name and alias from a table expression."""
    if isinstance(expr, exp.Table):
        table_name = expr.name
        alias = expr.alias
        if alias:
            aliases[alias] = table_name
        else:
            aliases[table_name] = table_name
    elif isinstance(expr, exp.Subquery):
        alias = expr.alias
        if alias:
            # For subqueries, the alias points to the subquery itself
            aliases[alias] = f"({alias})"
    elif hasattr(expr, 'this'):
        _extract_table_from_expression(expr.this, aliases)


def _analyze_select_expression(
    expr: exp.Expression,
    table_aliases: dict[str, str],
    scope: Any
) -> ColumnLineage | None:
    """Analyze a single SELECT expression to determine its sources."""
    
    # Get the output column name
    if isinstance(expr, exp.Alias):
        output_name = expr.alias
        inner_expr = expr.this
    elif isinstance(expr, exp.Column):
        output_name = expr.name
        inner_expr = expr
    else:
        # For complex expressions without alias, try to get a name
        if hasattr(expr, 'alias') and expr.alias:
            output_name = expr.alias
        else:
            output_name = expr.sql()[:50]  # Truncate long expressions
        inner_expr = expr
    
    # Find all column references in the expression
    sources = []
    columns = list(inner_expr.find_all(exp.Column))
    
    # Check if this is a direct column reference or derived
    is_derived = not (isinstance(inner_expr, exp.Column) and len(columns) == 1)
    
    # Track transformations
    transformations = set()
    for func in inner_expr.find_all(exp.Func):
        transformations.add(func.key.upper())
    
    for col in columns:
        table_ref = col.table if col.table else None
        col_name = col.name
        
        # Resolve table alias to actual table
        resolved_table = None
        if table_ref:
            resolved_table = table_aliases.get(table_ref, table_ref)
        elif len(table_aliases) == 1:
            # If only one table, assume it's from there
            resolved_table = list(table_aliases.values())[0]
        elif len(table_aliases) > 1:
            # Multiple tables - mark as ambiguous but include all possible sources
            # This will be resolved later when matching against upstream models
            resolved_table = None  # Will try to resolve via upstream model matching
        
        transformation = ", ".join(sorted(transformations)) if transformations else None
        
        sources.append(ColumnSource(
            table=table_ref or "?",
            column=col_name,
            resolved_table=resolved_table,
            transformation=transformation,
        ))
    
    # Handle star expressions
    if isinstance(inner_expr, exp.Star):
        return ColumnLineage(
            column="*",
            sources=[ColumnSource(table="*", column="*", resolved_table="*")],
            expression="*",
            is_derived=False,
        )
    
    return ColumnLineage(
        column=output_name,
        sources=sources,
        expression=inner_expr.sql() if is_derived else None,
        is_derived=is_derived,
    )


def analyze_model_columns(
    compiled_sql: str,
    upstream_models: list[str],
    dialect: str | None = None
) -> dict[str, Any]:
    """
    Analyze a model's SQL to extract column lineage with upstream model resolution.
    
    Args:
        compiled_sql: The compiled SQL of the model
        upstream_models: List of upstream model names (from depends_on)
        dialect: SQL dialect
    
    Returns:
        Dict with column lineage info suitable for API response
    """
    lineage = extract_column_lineage(compiled_sql, dialect)
    
    # Build a lookup of short names to full model IDs
    model_lookup = {}
    for model in upstream_models:
        model_short = model.split(".")[-1].lower()
        model_lookup[model_short] = model
    
    # Convert to API-friendly format
    result = {}
    for col_name, col_lineage in lineage.items():
        sources = []
        for src in col_lineage.sources:
            matched_model = None
            resolved = src.resolved_table
            
            if resolved:
                # Direct match on resolved table
                resolved_lower = resolved.lower()
                if resolved_lower in model_lookup:
                    matched_model = model_lookup[resolved_lower]
                else:
                    # Try partial match
                    for model_short, model_full in model_lookup.items():
                        if resolved_lower in model_short or model_short in resolved_lower:
                            matched_model = model_full
                            break
            
            # If no table alias (resolved_table is None), try to match by checking 
            # all upstream models - assign to all possible sources
            if not resolved and src.table == "?":
                # Column without table qualifier - could come from any joined table
                # For now, include all upstream models as potential sources
                # The trace will resolve this by checking which model actually has the column
                for model in upstream_models:
                    sources.append({
                        "table": src.table,
                        "column": src.column,
                        "resolvedTable": model.split(".")[-1],
                        "upstreamModel": model,
                        "transformation": src.transformation,
                        "ambiguous": True,
                    })
                continue  # Skip the normal append below
            
            sources.append({
                "table": src.table,
                "column": src.column,
                "resolvedTable": resolved,
                "upstreamModel": matched_model,
                "transformation": src.transformation,
            })
        
        result[col_name] = {
            "column": col_name,
            "sources": sources,
            "expression": col_lineage.expression,
            "isDerived": col_lineage.is_derived,
        }
    
    return result


def trace_column_upstream(
    target_column: str,
    model_lineage: dict[str, Any],
    all_models: dict[str, dict],  # node_id -> {compiledCode, depends_on, ...}
    visited: set[str] | None = None,
    max_depth: int = 10,
    dialect: str | None = None
) -> list[dict]:
    """
    Trace a column upstream through multiple models.
    
    Args:
        target_column: Column name to trace
        model_lineage: Column lineage for the current model
        all_models: Dict of all models with their SQL and dependencies
        visited: Set of visited (model, column) pairs to prevent cycles
        max_depth: Maximum depth to traverse
        dialect: SQL dialect
    
    Returns:
        List of upstream column references with their paths
    """
    if visited is None:
        visited = set()
    
    if max_depth <= 0:
        return []
    
    result = []
    col_info = model_lineage.get(target_column)
    
    if not col_info:
        return []
    
    for source in col_info.get("sources", []):
        upstream_model = source.get("upstreamModel")
        source_column = source.get("column")
        is_ambiguous = source.get("ambiguous", False)
        
        if not source_column:
            continue
        
        # If no upstream model but we have a column name, try to find it in any upstream
        if not upstream_model and source_column:
            # This shouldn't happen now with our ambiguous handling, but just in case
            continue
        
        visit_key = f"{upstream_model}:{source_column}"
        if visit_key in visited:
            continue
        
        # For ambiguous sources, verify the column exists in the upstream model
        if is_ambiguous:
            upstream_data = all_models.get(upstream_model, {})
            upstream_sql = upstream_data.get("compiledCode") or upstream_data.get("rawCode")
            if upstream_sql:
                upstream_deps = upstream_data.get("depends_on", [])
                upstream_lineage = analyze_model_columns(upstream_sql, upstream_deps, dialect)
                # Check if this column is actually output by this model
                if source_column not in upstream_lineage:
                    continue  # Column not in this model, skip
        
        visited.add(visit_key)
        
        result.append({
            "model": upstream_model,
            "column": source_column,
            "table": source.get("table"),
            "transformation": source.get("transformation"),
            "depth": max_depth,
            "ambiguous": is_ambiguous,
        })
        
        # Recursively trace upstream
        upstream_data = all_models.get(upstream_model, {})
        upstream_sql = upstream_data.get("compiledCode") or upstream_data.get("rawCode")
        
        if upstream_sql:
            upstream_deps = upstream_data.get("depends_on", [])
            upstream_lineage = analyze_model_columns(upstream_sql, upstream_deps, dialect)
            
            deeper = trace_column_upstream(
                source_column,
                upstream_lineage,
                all_models,
                visited,
                max_depth - 1,
                dialect
            )
            result.extend(deeper)
    
    return result
