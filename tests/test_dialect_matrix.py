from unittest.mock import patch

import pytest

from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator
from dbt_test_factory import (
    ColumnDef,
    DIALECTS,
    build_test_artifacts,
    catalog_table_name,
    compiled_column_ref,
)


MODEL_ID = "model.test_project.my_model"
SOURCE_ID = "source.test_project.raw.source_table"


def _table_ref(dialect):
    info = DIALECTS[dialect]
    db = catalog_table_name("test_db", info)
    schema = catalog_table_name("raw_schema", info)
    table = catalog_table_name("source_table", info)
    if dialect in ("clickhouse", "oracle", "starrocks"):
        return f"{schema}.{table}"
    return f"{db}.{schema}.{table}"


def _col(dialect, name):
    return compiled_column_ref(ColumnDef(name), DIALECTS[dialect])


def _extractor_with_sql(dialect, sql, source_columns, model_columns):
    manifest, catalog = build_test_artifacts(
        dialect,
        source_columns,
        model_columns=model_columns,
    )
    manifest["nodes"][MODEL_ID]["compiled_code"] = sql
    manifest["nodes"][MODEL_ID]["raw_code"] = sql

    with patch("dbt_colibri.utils.json_utils.read_json") as mock:
        mock.side_effect = [manifest, catalog]
        return DbtColumnLineageExtractor("dummy", "dummy")


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_relation_name_resolution_by_dialect(dialect):
    columns = [ColumnDef("id")]
    sql = f"SELECT {_col(dialect, 'id')} FROM {_table_ref(dialect)}"

    extractor = _extractor_with_sql(dialect, sql, columns, columns)
    source_key = _table_ref(dialect).lower()

    assert extractor._table_to_node[source_key]["unique_id"] == SOURCE_ID
    parents = extractor.extract_project_lineage()["lineage"]["parents"][MODEL_ID]
    assert parents["id"] == [
        {"column": "id", "dbt_node": SOURCE_ID, "lineage_type": "pass-through"}
    ]


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_lineage_types_by_dialect(dialect):
    source_columns = [
        ColumnDef("id"),
        ColumnDef("gross_amount"),
        ColumnDef("discount_amount"),
    ]
    model_columns = [
        ColumnDef("id"),
        ColumnDef("customer_id"),
        ColumnDef("net_amount"),
    ]
    sql = (
        f"SELECT {_col(dialect, 'id')}, "
        f"{_col(dialect, 'id')} AS {_col(dialect, 'customer_id')}, "
        f"{_col(dialect, 'gross_amount')} - {_col(dialect, 'discount_amount')} "
        f"AS {_col(dialect, 'net_amount')} "
        f"FROM {_table_ref(dialect)}"
    )

    extractor = _extractor_with_sql(dialect, sql, source_columns, model_columns)
    parents = extractor.extract_project_lineage()["lineage"]["parents"][MODEL_ID]

    assert parents["id"] == [
        {"column": "id", "dbt_node": SOURCE_ID, "lineage_type": "pass-through"}
    ]
    assert parents["customer_id"] == [
        {"column": "id", "dbt_node": SOURCE_ID, "lineage_type": "rename"}
    ]
    assert {
        (edge["column"], edge["lineage_type"])
        for edge in parents["net_amount"]
    } == {
        ("gross_amount", "transformation"),
        ("discount_amount", "transformation"),
    }


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_select_star_expands_by_dialect(dialect):
    columns = [
        ColumnDef("id"),
        ColumnDef("status"),
        ColumnDef("amount"),
    ]
    sql = f"SELECT * FROM {_table_ref(dialect)}"

    extractor = _extractor_with_sql(dialect, sql, columns, columns)
    parents = extractor.extract_project_lineage()["lineage"]["parents"][MODEL_ID]

    for column in ("id", "status", "amount"):
        assert parents[column] == [
            {
                "column": column,
                "dbt_node": SOURCE_ID,
                "lineage_type": "pass-through",
            }
        ]


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_structural_filter_and_having_by_dialect(dialect):
    source_columns = [
        ColumnDef("category"),
        ColumnDef("amount"),
        ColumnDef("status"),
    ]
    model_columns = [ColumnDef("category")]
    sql = (
        f"SELECT {_col(dialect, 'category')} "
        f"FROM {_table_ref(dialect)} "
        f"WHERE {_col(dialect, 'status')} = 'active' "
        f"GROUP BY {_col(dialect, 'category')} "
        f"HAVING SUM({_col(dialect, 'amount')}) > 100"
    )

    extractor = _extractor_with_sql(dialect, sql, source_columns, model_columns)
    parents = extractor.extract_project_lineage()["lineage"]["parents"][MODEL_ID]
    filter_edges = parents["__colibri_filter__"]

    assert {
        (edge["column"], edge["dbt_node"], edge["lineage_type"])
        for edge in filter_edges
    } == {
        ("status", SOURCE_ID, "filter"),
        ("amount", SOURCE_ID, "filter"),
    }


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_report_edges_by_dialect(dialect):
    source_columns = [ColumnDef("id"), ColumnDef("amount")]
    model_columns = [ColumnDef("customer_id"), ColumnDef("amount")]
    sql = (
        f"SELECT {_col(dialect, 'id')} AS {_col(dialect, 'customer_id')}, "
        f"{_col(dialect, 'amount')} "
        f"FROM {_table_ref(dialect)}"
    )

    extractor = _extractor_with_sql(dialect, sql, source_columns, model_columns)
    report = DbtColibriReportGenerator(extractor).build_full_lineage()

    edges = {
        (edge["source"], edge["sourceColumn"], edge["target"], edge["targetColumn"])
        for edge in report["lineage"]["edges"]
        if edge["sourceColumn"] and edge["targetColumn"]
    }
    assert (SOURCE_ID, "id", MODEL_ID, "customer_id") in edges
    assert (SOURCE_ID, "amount", MODEL_ID, "amount") in edges

    model_columns = report["nodes"][MODEL_ID]["columns"]
    assert model_columns["customer_id"]["hasLineage"] is True
    assert model_columns["customer_id"]["lineageType"] == "rename"
    assert model_columns["amount"]["hasLineage"] is True
    assert model_columns["amount"]["lineageType"] == "pass-through"
