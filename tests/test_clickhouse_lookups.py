"""Tests for column lineage through ClickHouse lookup functions (dictGet*, dictHas, joinGet*)."""

import json

import pytest

from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor


FACT_ID = "model.test.orders"
DICT_ID = "model.test.customers_dict"
JOIN_ID = "model.test.rates_join"
MODEL_ID = "model.test.enriched_orders"


def _make_node(unique_id, columns, compiled_code="SELECT 1", config=None, depends_on=None):
    name = unique_id.split(".")[-1]
    return {
        "unique_id": unique_id,
        "resource_type": "model",
        "database": "",
        "schema": "analytics",
        "name": name,
        "alias": name,
        "relation_name": f"`analytics`.`{name}`",
        "columns": {c: {} for c in columns},
        "config": config or {"materialized": "table"},
        "depends_on": {"nodes": depends_on or []},
        "compiled_code": compiled_code,
        "raw_code": compiled_code,
        "path": f"{name}.sql",
    }


def _make_catalog_node(unique_id, columns):
    name = unique_id.split(".")[-1]
    return {
        "unique_id": unique_id,
        "metadata": {"database": "", "schema": "analytics", "name": name},
        "columns": {c: {"type": "String"} for c in columns},
    }


def _extract(
    tmp_path,
    model_sql,
    model_columns,
    adapter_type="clickhouse",
    primary_key="customer_id",
    upstream_in_catalog=True,
):
    """Builds orders + customers_dict + rates_join -> enriched_orders and returns its parents."""
    upstream = {
        FACT_ID: (["order_id", "customer_id", "region_id", "currency"], {"materialized": "table"}),
        DICT_ID: (
            ["customer_id", "region_id", "name", "segment"],
            {"materialized": "dictionary", "primary_key": primary_key},
        ),
        JOIN_ID: (["currency", "rate"], {"materialized": "table", "engine": "Join(ANY, LEFT, currency)"}),
    }
    nodes = {
        node_id: _make_node(node_id, columns, config=config)
        for node_id, (columns, config) in upstream.items()
    }
    nodes[MODEL_ID] = _make_node(MODEL_ID, model_columns, model_sql, depends_on=list(upstream))
    manifest = {
        "metadata": {"adapter_type": adapter_type, "dbt_version": "1.10.0"},
        "nodes": nodes,
        "sources": {},
        "parent_map": {MODEL_ID: list(upstream), **{node_id: [] for node_id in upstream}},
        "child_map": {MODEL_ID: [], **{node_id: [MODEL_ID] for node_id in upstream}},
    }
    catalog = {
        "nodes": {
            node_id: _make_catalog_node(node_id, node["columns"])
            for node_id, node in nodes.items()
            if upstream_in_catalog or node_id == MODEL_ID
        },
        "sources": {},
    }
    manifest_path = tmp_path / "manifest.json"
    catalog_path = tmp_path / "catalog.json"
    manifest_path.write_text(json.dumps(manifest))
    catalog_path.write_text(json.dumps(catalog))
    extractor = DbtColumnLineageExtractor(str(manifest_path), str(catalog_path))
    return extractor.extract_project_lineage()["lineage"]["parents"][MODEL_ID]


def _edges(parents, column):
    return {(edge["dbt_node"], edge["column"]) for edge in parents.get(column, [])}


@pytest.mark.parametrize(
    "dictionary",
    ["analytics.customers_dict", "`analytics`.`customers_dict`", "'analytics.customers_dict'"],
)
def test_dict_get_single_attribute_edge(tmp_path, dictionary):
    sql = (
        f"SELECT order_id, dictGet({dictionary}, 'name', customer_id) AS customer_name "
        "FROM analytics.orders"
    )
    parents = _extract(tmp_path, sql, ["order_id", "customer_name"])

    assert _edges(parents, "customer_name") == {
        (DICT_ID, "name"),
        (FACT_ID, "customer_id"),
    }
    assert parents["customer_name"][0]["lineage_type"] == "transformation"
    assert _edges(parents, "order_id") == {(FACT_ID, "order_id")}


def test_dict_get_tuple_of_attributes_edges(tmp_path):
    sql = (
        "SELECT dictGet('analytics.customers_dict', ('name', 'segment'), customer_id) AS customer "
        "FROM analytics.orders"
    )
    parents = _extract(tmp_path, sql, ["customer"])

    assert _edges(parents, "customer") == {
        (DICT_ID, "name"),
        (DICT_ID, "segment"),
        (FACT_ID, "customer_id"),
    }


@pytest.mark.parametrize(
    "call",
    [
        "dictGetOrNull(analytics.customers_dict, 'segment', customer_id)",
        "dictGetOrDefault(analytics.customers_dict, 'segment', customer_id, 'none')",
        "dictGetString(analytics.customers_dict, 'segment', customer_id)",
        "dictGetStringOrDefault(analytics.customers_dict, 'segment', customer_id, 'none')",
        "if(isNull(customer_id), NULL, dictGetOrNull(analytics.customers_dict, 'segment', assumeNotNull(customer_id)))",
    ],
)
def test_dict_get_variants_attribute_edge(tmp_path, call):
    parents = _extract(tmp_path, f"SELECT {call} AS segment FROM analytics.orders", ["segment"])

    assert _edges(parents, "segment") == {(DICT_ID, "segment"), (FACT_ID, "customer_id")}


def test_dict_has_edge_to_dictionary_key(tmp_path):
    sql = "SELECT dictHas(analytics.customers_dict, customer_id) AS is_known FROM analytics.orders"
    parents = _extract(tmp_path, sql, ["is_known"])

    assert _edges(parents, "is_known") == {(DICT_ID, "customer_id"), (FACT_ID, "customer_id")}


def test_dict_has_edges_to_every_column_of_a_composite_key(tmp_path):
    sql = (
        "SELECT dictHas('analytics.customers_dict', (customer_id, region_id)) AS is_known "
        "FROM analytics.orders"
    )
    parents = _extract(tmp_path, sql, ["is_known"], primary_key="customer_id, region_id")

    assert _edges(parents, "is_known") == {
        (DICT_ID, "customer_id"),
        (DICT_ID, "region_id"),
        (FACT_ID, "customer_id"),
        (FACT_ID, "region_id"),
    }


def test_join_get_attribute_edge(tmp_path):
    sql = "SELECT joinGetOrNull('analytics.rates_join', 'rate', currency) AS rate FROM analytics.orders"
    parents = _extract(tmp_path, sql, ["rate"])

    assert _edges(parents, "rate") == {(JOIN_ID, "rate"), (FACT_ID, "currency")}


def test_dict_get_inside_cte_traces_to_dictionary(tmp_path):
    sql = (
        "WITH named AS ("
        " SELECT order_id, dictGet(analytics.customers_dict, 'name', customer_id) AS customer_name"
        " FROM analytics.orders"
        ") SELECT order_id, customer_name FROM named"
    )
    parents = _extract(tmp_path, sql, ["order_id", "customer_name"])

    assert _edges(parents, "customer_name") == {(DICT_ID, "name"), (FACT_ID, "customer_id")}


def test_dict_has_in_where_creates_filter_edge_to_key(tmp_path):
    sql = (
        "SELECT order_id FROM analytics.orders "
        "WHERE dictHas(analytics.customers_dict, customer_id)"
    )
    parents = _extract(tmp_path, sql, ["order_id"])

    assert _edges(parents, "__colibri_filter__") == {
        (DICT_ID, "customer_id"),
        (FACT_ID, "customer_id"),
    }


@pytest.mark.parametrize(
    "call, upstream_in_catalog",
    [
        # Attribute name not a literal: which attribute is read is unknown.
        ("dictGet(analytics.customers_dict, currency, customer_id)", True),
        # No database: the current database at query time, unknown here.
        ("dictGet('customers_dict', 'name', customer_id)", True),
        # A bare name: with no catalog for the FROM table the optimizer qualifies it as orders.customers_dict.
        ("dictGet(customers_dict, 'name', customer_id)", False),
    ],
)
def test_unresolvable_lookup_adds_no_dictionary_edge(tmp_path, call, upstream_in_catalog):
    sql = f"SELECT {call} AS customer_name FROM analytics.orders"
    parents = _extract(tmp_path, sql, ["customer_name"], upstream_in_catalog=upstream_in_catalog)

    assert {node for node, _ in _edges(parents, "customer_name")} == {FACT_ID}


def test_dict_has_without_primary_key_adds_no_dictionary_edge(tmp_path):
    sql = "SELECT dictHas(analytics.customers_dict, customer_id) AS is_known FROM analytics.orders"
    parents = _extract(tmp_path, sql, ["is_known"], primary_key=None)

    assert _edges(parents, "is_known") == {(FACT_ID, "customer_id")}


def test_lookup_functions_are_clickhouse_only(tmp_path):
    sql = (
        "SELECT dictGet('analytics.customers_dict', 'name', customer_id) AS customer_name "
        "FROM analytics.orders"
    )
    parents = _extract(tmp_path, sql, ["customer_name"], adapter_type="starrocks")

    assert {node for node, _ in _edges(parents, "customer_name")} == {FACT_ID}
