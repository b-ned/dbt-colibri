"""Tests for structural lineage (WHERE/HAVING/JOIN ON column detection)."""

import pytest
import json
import os
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator


def _make_manifest(nodes, sources=None, parent_map=None, child_map=None, adapter_type="snowflake"):
    """Helper to build a minimal manifest dict."""
    return {
        "metadata": {"adapter_type": adapter_type, "dbt_version": "1.9.0"},
        "nodes": nodes,
        "sources": sources or {},
        "parent_map": parent_map or {},
        "child_map": child_map or {},
    }


def _make_catalog(nodes, sources=None):
    """Helper to build a minimal catalog dict."""
    return {"nodes": nodes, "sources": sources or {}}


def _make_node(unique_id, columns, compiled_code, raw_code=None, depends_on=None,
               database="TEST_DB", schema="TEST_SCHEMA", resource_type="model"):
    name = unique_id.split(".")[-1]
    return {
        "unique_id": unique_id,
        "resource_type": resource_type,
        "database": database,
        "schema": schema,
        "name": name,
        "alias": name,
        "relation_name": f"{database}.{schema}.{name}",
        "columns": {c: {} for c in columns},
        "config": {"materialized": "table"},
        "depends_on": {"nodes": depends_on or []},
        "compiled_code": compiled_code,
        "raw_code": raw_code or compiled_code,
        "path": f"{name}.sql",
    }


def _make_catalog_node(unique_id, columns, database="TEST_DB", schema="TEST_SCHEMA"):
    name = unique_id.split(".")[-1]
    return {
        "unique_id": unique_id,
        "metadata": {"database": database, "schema": schema, "name": name},
        "columns": {c: {"type": "VARCHAR"} for c in columns},
    }


def _write_fixtures(tmp_path, manifest, catalog):
    """Write manifest/catalog to tmp_path and return paths."""
    mp = tmp_path / "manifest.json"
    cp = tmp_path / "catalog.json"
    mp.write_text(json.dumps(manifest))
    cp.write_text(json.dumps(catalog))
    return str(mp), str(cp)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _two_table_fixtures():
    """Base fixtures: source_table → model_a (used by multiple tests)."""
    src_id = "model.test.source_table"
    src_cols = ["id", "status", "amount", "category"]

    mdl_id = "model.test.model_a"
    mdl_cols = ["id", "amount"]

    return src_id, src_cols, mdl_id, mdl_cols


# ---------------------------------------------------------------------------
# Test 1: basic WHERE → filter edge
# ---------------------------------------------------------------------------

def test_basic_where_clause_creates_filter_edge(tmp_path):
    src_id, src_cols, mdl_id, mdl_cols = _two_table_fixtures()

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, src_cols, "SELECT id, status, amount, category FROM raw_table"),
            mdl_id: _make_node(
                mdl_id, mdl_cols,
                "SELECT id, amount FROM TEST_DB.TEST_SCHEMA.source_table WHERE status = 'active'",
                raw_code="SELECT id, amount FROM {{ ref('source_table') }} WHERE status = 'active'",
                depends_on=[src_id],
            ),
        },
        parent_map={mdl_id: [src_id], src_id: []},
        child_map={src_id: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, src_cols),
            mdl_id: _make_catalog_node(mdl_id, mdl_cols),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert mdl_id in parents
    assert "__colibri_filter__" in parents[mdl_id]

    filter_edges = parents[mdl_id]["__colibri_filter__"]
    filter_cols = {e["column"] for e in filter_edges}
    assert "status" in filter_cols


# ---------------------------------------------------------------------------
# Test 2: JOIN ON → join edges for both sides
# ---------------------------------------------------------------------------

def test_basic_join_on_creates_join_edges(tmp_path):
    tbl_a = "model.test.table_a"
    tbl_b = "model.test.table_b"
    mdl_id = "model.test.joined_model"

    manifest = _make_manifest(
        nodes={
            tbl_a: _make_node(tbl_a, ["id", "name"], "SELECT id, name FROM raw_a"),
            tbl_b: _make_node(tbl_b, ["id", "value"], "SELECT id, value FROM raw_b"),
            mdl_id: _make_node(
                mdl_id, ["name", "value"],
                "SELECT a.name, b.value FROM TEST_DB.TEST_SCHEMA.table_a a JOIN TEST_DB.TEST_SCHEMA.table_b b ON a.id = b.id",
                raw_code="SELECT a.name, b.value FROM {{ ref('table_a') }} a JOIN {{ ref('table_b') }} b ON a.id = b.id",
                depends_on=[tbl_a, tbl_b],
            ),
        },
        parent_map={mdl_id: [tbl_a, tbl_b], tbl_a: [], tbl_b: []},
        child_map={tbl_a: [mdl_id], tbl_b: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            tbl_a: _make_catalog_node(tbl_a, ["id", "name"]),
            tbl_b: _make_catalog_node(tbl_b, ["id", "value"]),
            mdl_id: _make_catalog_node(mdl_id, ["name", "value"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert "__colibri_join__" in parents[mdl_id]
    join_edges = parents[mdl_id]["__colibri_join__"]
    join_cols = {e["column"] for e in join_edges}
    assert "id" in join_cols

    # Should have edges from both table_a and table_b
    join_nodes = {e["dbt_node"] for e in join_edges}
    assert tbl_a in join_nodes
    assert tbl_b in join_nodes


# ---------------------------------------------------------------------------
# Test 3: CTE with WHERE traces to source
# ---------------------------------------------------------------------------

def test_cte_with_where_traces_to_source(tmp_path):
    src_id = "model.test.source_table"
    mdl_id = "model.test.cte_model"

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, ["id", "status", "amount"], "SELECT id, status, amount FROM raw"),
            mdl_id: _make_node(
                mdl_id, ["id", "amount"],
                """
                WITH base AS (
                    SELECT id, status, amount
                    FROM TEST_DB.TEST_SCHEMA.source_table
                )
                SELECT id, amount FROM base WHERE status = 'active'
                """,
                raw_code="WITH base AS (SELECT id, status, amount FROM {{ ref('source_table') }}) SELECT id, amount FROM base WHERE status = 'active'",
                depends_on=[src_id],
            ),
        },
        parent_map={mdl_id: [src_id], src_id: []},
        child_map={src_id: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, ["id", "status", "amount"]),
            mdl_id: _make_catalog_node(mdl_id, ["id", "amount"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert "__colibri_filter__" in parents[mdl_id]
    filter_edges = parents[mdl_id]["__colibri_filter__"]
    # The status column in WHERE should trace back to source_table
    source_edges = [e for e in filter_edges if e["dbt_node"] == src_id]
    source_cols = {e["column"] for e in source_edges}
    assert "status" in source_cols


# ---------------------------------------------------------------------------
# Test 4: column in SELECT and WHERE emits both edges
# ---------------------------------------------------------------------------

def test_column_in_select_and_where_emits_both_edges(tmp_path):
    src_id = "model.test.source_table"
    mdl_id = "model.test.dual_model"

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, ["id", "status"], "SELECT id, status FROM raw"),
            mdl_id: _make_node(
                mdl_id, ["id", "status"],
                "SELECT id, status FROM TEST_DB.TEST_SCHEMA.source_table WHERE status = 'active'",
                raw_code="SELECT id, status FROM {{ ref('source_table') }} WHERE status = 'active'",
                depends_on=[src_id],
            ),
        },
        parent_map={mdl_id: [src_id], src_id: []},
        child_map={src_id: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, ["id", "status"]),
            mdl_id: _make_catalog_node(mdl_id, ["id", "status"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    # Data edge: status column in SELECT
    assert "status" in parents[mdl_id]
    data_edges = parents[mdl_id]["status"]
    assert any(e["dbt_node"] == src_id and e["column"] == "status" for e in data_edges)

    # Filter edge: status column in WHERE
    assert "__colibri_filter__" in parents[mdl_id]
    filter_edges = parents[mdl_id]["__colibri_filter__"]
    assert any(e["dbt_node"] == src_id and e["column"] == "status" for e in filter_edges)


# ---------------------------------------------------------------------------
# Test 5: multiple JOINs captured
# ---------------------------------------------------------------------------

def test_multiple_joins_captured(tmp_path):
    tbl_a = "model.test.table_a"
    tbl_b = "model.test.table_b"
    tbl_c = "model.test.table_c"
    mdl_id = "model.test.multi_join"

    manifest = _make_manifest(
        nodes={
            tbl_a: _make_node(tbl_a, ["id", "name"], "SELECT id, name FROM raw_a"),
            tbl_b: _make_node(tbl_b, ["id", "val1"], "SELECT id, val1 FROM raw_b"),
            tbl_c: _make_node(tbl_c, ["id", "val2"], "SELECT id, val2 FROM raw_c"),
            mdl_id: _make_node(
                mdl_id, ["name", "val1", "val2"],
                """SELECT a.name, b.val1, c.val2
                   FROM TEST_DB.TEST_SCHEMA.table_a a
                   LEFT JOIN TEST_DB.TEST_SCHEMA.table_b b ON a.id = b.id
                   INNER JOIN TEST_DB.TEST_SCHEMA.table_c c ON a.id = c.id""",
                raw_code="SELECT a.name, b.val1, c.val2 FROM {{ ref('table_a') }} a LEFT JOIN {{ ref('table_b') }} b ON a.id = b.id INNER JOIN {{ ref('table_c') }} c ON a.id = c.id",
                depends_on=[tbl_a, tbl_b, tbl_c],
            ),
        },
        parent_map={mdl_id: [tbl_a, tbl_b, tbl_c], tbl_a: [], tbl_b: [], tbl_c: []},
        child_map={tbl_a: [mdl_id], tbl_b: [mdl_id], tbl_c: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            tbl_a: _make_catalog_node(tbl_a, ["id", "name"]),
            tbl_b: _make_catalog_node(tbl_b, ["id", "val1"]),
            tbl_c: _make_catalog_node(tbl_c, ["id", "val2"]),
            mdl_id: _make_catalog_node(mdl_id, ["name", "val1", "val2"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert "__colibri_join__" in parents[mdl_id]
    join_edges = parents[mdl_id]["__colibri_join__"]
    join_nodes = {e["dbt_node"] for e in join_edges}
    # All three tables contribute to JOIN conditions
    assert tbl_a in join_nodes
    assert tbl_b in join_nodes
    assert tbl_c in join_nodes


# ---------------------------------------------------------------------------
# Test 6: HAVING creates filter edge
# ---------------------------------------------------------------------------

def test_having_creates_filter_edge(tmp_path):
    src_id = "model.test.source_table"
    mdl_id = "model.test.having_model"

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, ["category", "amount"], "SELECT category, amount FROM raw"),
            mdl_id: _make_node(
                mdl_id, ["category"],
                "SELECT category FROM TEST_DB.TEST_SCHEMA.source_table GROUP BY category HAVING SUM(amount) > 100",
                raw_code="SELECT category FROM {{ ref('source_table') }} GROUP BY category HAVING SUM(amount) > 100",
                depends_on=[src_id],
            ),
        },
        parent_map={mdl_id: [src_id], src_id: []},
        child_map={src_id: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, ["category", "amount"]),
            mdl_id: _make_catalog_node(mdl_id, ["category"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert "__colibri_filter__" in parents[mdl_id]
    filter_edges = parents[mdl_id]["__colibri_filter__"]
    filter_cols = {e["column"] for e in filter_edges}
    assert "amount" in filter_cols


# ---------------------------------------------------------------------------
# Test 7: nested CTE chain with WHERE traces to ultimate source
# ---------------------------------------------------------------------------

def test_nested_cte_chain_with_where(tmp_path):
    src_id = "model.test.raw_data"
    mdl_id = "model.test.nested_cte"

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, ["id", "flag", "value"], "SELECT id, flag, value FROM raw"),
            mdl_id: _make_node(
                mdl_id, ["id", "value"],
                """
                WITH step1 AS (
                    SELECT id, flag, value
                    FROM TEST_DB.TEST_SCHEMA.raw_data
                ),
                step2 AS (
                    SELECT id, flag, value
                    FROM step1
                )
                SELECT id, value FROM step2 WHERE flag = true
                """,
                raw_code="WITH step1 AS (SELECT id, flag, value FROM {{ ref('raw_data') }}), step2 AS (SELECT id, flag, value FROM step1) SELECT id, value FROM step2 WHERE flag = true",
                depends_on=[src_id],
            ),
        },
        parent_map={mdl_id: [src_id], src_id: []},
        child_map={src_id: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, ["id", "flag", "value"]),
            mdl_id: _make_catalog_node(mdl_id, ["id", "value"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    assert "__colibri_filter__" in parents[mdl_id]
    filter_edges = parents[mdl_id]["__colibri_filter__"]
    # Should trace all the way back to raw_data.flag
    source_edges = [e for e in filter_edges if e["dbt_node"] == src_id]
    source_cols = {e["column"] for e in source_edges}
    assert "flag" in source_cols


# ---------------------------------------------------------------------------
# Test 8: end-to-end structural edges in report output
# ---------------------------------------------------------------------------

def test_structural_edges_in_report_output(tmp_path):
    src_id = "model.test.source_table"
    tbl_b = "model.test.lookup"
    mdl_id = "model.test.report_model"

    manifest = _make_manifest(
        nodes={
            src_id: _make_node(src_id, ["id", "status", "lookup_id"], "SELECT id, status, lookup_id FROM raw"),
            tbl_b: _make_node(tbl_b, ["id", "label"], "SELECT id, label FROM raw_lookup"),
            mdl_id: _make_node(
                mdl_id, ["id", "label"],
                """SELECT a.id, b.label
                   FROM TEST_DB.TEST_SCHEMA.source_table a
                   JOIN TEST_DB.TEST_SCHEMA.lookup b ON a.lookup_id = b.id
                   WHERE a.status = 'active'""",
                raw_code="SELECT a.id, b.label FROM {{ ref('source_table') }} a JOIN {{ ref('lookup') }} b ON a.lookup_id = b.id WHERE a.status = 'active'",
                depends_on=[src_id, tbl_b],
            ),
        },
        parent_map={mdl_id: [src_id, tbl_b], src_id: [], tbl_b: []},
        child_map={src_id: [mdl_id], tbl_b: [mdl_id], mdl_id: []},
    )
    catalog = _make_catalog(
        nodes={
            src_id: _make_catalog_node(src_id, ["id", "status", "lookup_id"]),
            tbl_b: _make_catalog_node(tbl_b, ["id", "label"]),
            mdl_id: _make_catalog_node(mdl_id, ["id", "label"]),
        }
    )

    mp, cp = _write_fixtures(tmp_path, manifest, catalog)
    extractor = DbtColumnLineageExtractor(mp, cp)
    generator = DbtColibriReportGenerator(extractor)
    report = generator.build_full_lineage()

    edges = report["lineage"]["edges"]

    # Check for filter edges
    filter_edges = [e for e in edges if e.get("edgeType") == "filter"]
    assert len(filter_edges) > 0, "Should have filter edges"
    # Filter edges should have empty targetColumn
    for fe in filter_edges:
        assert fe["targetColumn"] == ""

    # Check for join edges
    join_edges = [e for e in edges if e.get("edgeType") == "join"]
    assert len(join_edges) > 0, "Should have join edges"
    for je in join_edges:
        assert je["targetColumn"] == ""

    # Check that data edges still exist and do NOT have edgeType
    data_edges = [e for e in edges if "edgeType" not in e and e["sourceColumn"] != "" and e["targetColumn"] != ""]
    assert len(data_edges) > 0, "Should still have data edges"


# ---------------------------------------------------------------------------
# Test 9: parametrized integration test with real data
# ---------------------------------------------------------------------------

def test_structural_lineage_with_real_data(dbt_valid_test_data_dir):
    """Verify structural edges appear with real dbt test data across dialects."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid test data available")

    manifest_path = os.path.join(dbt_valid_test_data_dir, "manifest.json")
    catalog_path = os.path.join(dbt_valid_test_data_dir, "catalog.json")

    extractor = DbtColumnLineageExtractor(manifest_path, catalog_path)
    result = extractor.extract_project_lineage()
    parents = result["lineage"]["parents"]

    # Just verify the extraction runs without error and produces valid structure.
    # Not all datasets will have WHERE/JOIN, so we only verify shape.
    for model_id, col_map in parents.items():
        for col_key, sources in col_map.items():
            assert isinstance(sources, list)
            if col_key in ("__colibri_filter__", "__colibri_join__"):
                for entry in sources:
                    assert "column" in entry
                    assert "dbt_node" in entry
                    assert entry.get("lineage_type") in ("filter", "join")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
