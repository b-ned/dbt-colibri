"""
Lightweight factory for generating synthetic dbt manifest + catalog test data
across all supported dialects, without requiring dbt-core.

Uses SQLGlot's dialect awareness to produce correctly-cased identifiers so
that tests exercise the same code paths as real dbt artifacts would.

Dialect conventions (as observed via SQLGlot):
  - Snowflake / Oracle:  unquoted → UPPERCASE, quoted → preserves case
  - Postgres / Redshift / Trino / DuckDB / BigQuery / ClickHouse / TSQL / Databricks:
                          unquoted → lowercase, quoted → see below

Quoted column case-sensitivity (SQLGlot qualify behaviour):
  Preserves case: snowflake, postgres, oracle, clickhouse
  Lowercases:     bigquery, duckdb, redshift, trino, tsql, databricks
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import patch

import sqlglot


# ---------------------------------------------------------------------------
# Dialect metadata
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DialectInfo:
    """How a SQL dialect treats identifiers."""
    name: str
    # "upper" or "lower" – how unquoted identifiers appear in the catalog.
    unquoted_case: str
    # Whether the dialect preserves original casing for quoted identifiers.
    quoted_preserves_case: bool
    # The adapter_type value written into the dbt manifest metadata.
    adapter_type: str
    # If the adapter_type differs from the SQLGlot dialect name.
    sqlglot_dialect: Optional[str] = None


DIALECTS: dict[str, DialectInfo] = {
    "snowflake": DialectInfo("snowflake", "upper", True, "snowflake"),
    "postgres": DialectInfo("postgres", "lower", True, "postgres"),
    "oracle": DialectInfo("oracle", "upper", True, "oracle"),
    "clickhouse": DialectInfo("clickhouse", "lower", True, "clickhouse"),
    "bigquery": DialectInfo("bigquery", "lower", False, "bigquery"),
    "duckdb": DialectInfo("duckdb", "lower", False, "duckdb"),
    "redshift": DialectInfo("redshift", "lower", False, "redshift"),
    "trino": DialectInfo("trino", "lower", False, "trino"),
    "tsql": DialectInfo("tsql", "lower", False, "sqlserver", sqlglot_dialect="tsql"),
    "databricks": DialectInfo("databricks", "lower", False, "databricks"),
}

# Dialects where ``quote: true`` in dbt meaningfully preserves casing.
CASE_SENSITIVE_QUOTE_DIALECTS = frozenset(
    name for name, info in DIALECTS.items() if info.quoted_preserves_case
)


# ---------------------------------------------------------------------------
# Column definition
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    """Describes a column in both manifest and catalog."""
    name: str
    data_type: str = "VARCHAR"
    quote: bool = False
    description: str = ""


# ---------------------------------------------------------------------------
# Helper: apply dialect casing rules
# ---------------------------------------------------------------------------

def catalog_column_name(col: ColumnDef, dialect_info: DialectInfo) -> str:
    """Return the column name as it would appear in a catalog for *dialect_info*.

    - Unquoted columns follow the dialect's default casing rule.
    - Quoted columns preserve their original name on case-sensitive dialects,
      and are lowercased on case-insensitive dialects.
    """
    if col.quote and dialect_info.quoted_preserves_case:
        return col.name  # e.g. "quotedCol" stays as-is on Snowflake
    if dialect_info.unquoted_case == "upper":
        return col.name.upper()
    return col.name.lower()


def catalog_table_name(name: str, dialect_info: DialectInfo) -> str:
    """Return a table/schema/database name as it appears in the catalog."""
    if dialect_info.unquoted_case == "upper":
        return name.upper()
    return name.lower()


def compiled_column_ref(col: ColumnDef, dialect_info: DialectInfo) -> str:
    """Return the column reference as it would appear in compiled SQL."""
    if col.quote:
        if dialect_info.name == "bigquery":
            return f"`{col.name}`"
        elif dialect_info.name == "tsql":
            return f"[{col.name}]"
        else:
            return f'"{col.name}"'
    # Unquoted — use dialect casing
    if dialect_info.unquoted_case == "upper":
        return col.name.upper()
    return col.name.lower()


# ---------------------------------------------------------------------------
# Factory: build manifest + catalog dicts
# ---------------------------------------------------------------------------

def build_test_artifacts(
    dialect: str,
    source_columns: list[ColumnDef],
    model_columns: Optional[list[ColumnDef]] = None,
    *,
    project_name: str = "test_project",
    source_name: str = "raw",
    source_table: str = "source_table",
    model_name: str = "my_model",
    database: str = "test_db",
    source_schema: str = "raw_schema",
    model_schema: str = "public",
) -> tuple[dict, dict]:
    """Build a minimal but valid manifest + catalog pair.

    The model is a simple ``SELECT <columns> FROM <source_table>`` so that
    lineage should map each model column back to the corresponding source column.

    Parameters
    ----------
    dialect : str
        One of the keys in ``DIALECTS``.
    source_columns : list[ColumnDef]
        Columns on the source (and by default, also on the model).
    model_columns : list[ColumnDef] | None
        Columns on the model. Defaults to *source_columns*.

    Returns
    -------
    (manifest_dict, catalog_dict)
    """
    info = DIALECTS[dialect]
    adapter_type = info.adapter_type
    sqlglot_dialect = info.sqlglot_dialect or info.name

    if model_columns is None:
        model_columns = copy.deepcopy(source_columns)

    # Catalog-cased names
    cat_db = catalog_table_name(database, info)
    cat_src_schema = catalog_table_name(source_schema, info)
    cat_model_schema = catalog_table_name(model_schema, info)
    cat_src_table = catalog_table_name(source_table, info)
    cat_model_table = catalog_table_name(model_name, info)

    # Build SQL column refs for compiled code
    col_refs = ", ".join(compiled_column_ref(c, info) for c in model_columns)
    if info.name == "clickhouse":
        table_ref = f"{cat_src_schema}.{cat_src_table}"
    elif info.name == "oracle":
        table_ref = f"{cat_src_schema}.{cat_src_table}"
    else:
        table_ref = f"{cat_db}.{cat_src_schema}.{cat_src_table}"
    compiled_sql = f"SELECT {col_refs} FROM {table_ref}"

    source_id = f"source.{project_name}.{source_name}.{source_table}"
    model_id = f"model.{project_name}.{model_name}"

    # -- Manifest --------------------------------------------------------
    def _manifest_columns(cols):
        out = {}
        for c in cols:
            entry = {
                "name": c.name,
                "description": c.description,
                "data_type": c.data_type,
                "tags": [],
            }
            if c.quote:
                entry["quote"] = True
            out[c.name] = entry
        return out

    def _relation_name(db, schema, table, info):
            """Build a dialect-appropriate relation_name."""
            if info.name == "clickhouse":
                s = catalog_table_name(schema, info)
                t = catalog_table_name(table, info)
                return f"`{s}`.`{t}`"
            elif info.name == "bigquery":
                d = catalog_table_name(db, info)
                s = catalog_table_name(schema, info)
                t = catalog_table_name(table, info)
                return f"`{d}`.`{s}`.`{t}`"
            elif info.name == "oracle":
                s = catalog_table_name(schema, info)
                t = catalog_table_name(table, info)
                return f"{s}.{t}"
            else:
                d = catalog_table_name(db, info)
                s = catalog_table_name(schema, info)
                t = catalog_table_name(table, info)
                return f'"{d}"."{s}"."{t}"'

    manifest = {
        "metadata": {
            "adapter_type": adapter_type,
            "dbt_version": "1.11.6",
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "invocation_id": "test-fixture",
            "project_name": project_name,
        },
        "nodes": {
            model_id: {
                "path": f"models/{model_name}.sql",
                "original_file_path": f"models/{model_name}.sql",
                "resource_type": "model",
                "compiled_code": compiled_sql,
                "raw_code": compiled_sql,
                "depends_on": {"nodes": [source_id]},
                "database": database,
                "schema": model_schema,
                "name": model_name,
                "alias": model_name,
                "columns": _manifest_columns(model_columns),
                "relation_name": _relation_name(database, model_schema, model_name, info),
                "config": {"materialized": "table"},
                "refs": [],
                "tags": [],
                "fqn": [project_name, model_name],
            },
        },
        "sources": {
            source_id: {
                "path": "models/sources.yml",
                "original_file_path": "models/sources.yml",
                "resource_type": "source",
                "database": database,
                "schema": source_schema,
                "name": source_table,
                "identifier": source_table,
                "columns": _manifest_columns(source_columns),
                "relation_name": _relation_name(database, source_schema, source_table, info),
                "config": {"materialized": None},
                "fqn": [project_name, source_name, source_table],
                "tags": [],
            },
        },
        "exposures": {},
        "parent_map": {
            model_id: [source_id],
            source_id: [],
        },
        "child_map": {
            source_id: [model_id],
            model_id: [],
        },
    }

    # -- Catalog ---------------------------------------------------------
    def _catalog_columns(cols, info):
        out = {}
        for i, c in enumerate(cols, 1):
            cat_name = catalog_column_name(c, info)
            out[cat_name] = {
                "type": c.data_type,
                "name": cat_name,
                "index": i,
                "comment": None,
            }
        return out

    catalog = {
        "nodes": {
            model_id: {
                "unique_id": model_id,
                "metadata": {
                    "database": cat_db,
                    "schema": cat_model_schema,
                    "name": cat_model_table,
                    "type": "table",
                },
                "columns": _catalog_columns(model_columns, info),
            },
        },
        "sources": {
            source_id: {
                "unique_id": source_id,
                "metadata": {
                    "database": cat_db,
                    "schema": cat_src_schema,
                    "name": cat_src_table,
                    "type": "table",
                },
                "columns": _catalog_columns(source_columns, info),
            },
        },
    }

    return manifest, catalog


def make_extractor(dialect: str, source_columns: list[ColumnDef], **kwargs):
    """Convenience: build artifacts and return a ready DbtColumnLineageExtractor."""
    from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor

    manifest, catalog = build_test_artifacts(dialect, source_columns, **kwargs)
    with patch("dbt_colibri.utils.json_utils.read_json") as mock:
        mock.side_effect = [manifest, catalog]
        return DbtColumnLineageExtractor(
            manifest_path="dummy", catalog_path="dummy"
        )
