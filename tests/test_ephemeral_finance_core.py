"""Fixture-driven tests for ephemeral-model column lineage support.

These tests run against a real dbt manifest + catalog (duckdb) checked into
``tests/test_data/ephemeral_finance_core/`` that contains six ephemeral models.
Before the ephemeral-aware extractor lands the bug from issue #55 reproduces:

* All six ephemerals come back with empty projection lineage (only
  ``__colibri_join__`` / ``__colibri_filter__`` structural entries).
* No ephemeral has a ``children`` entry — downstream consumers can't trace
  through it.
* Three downstream consumers (``dim_customers``, ``fct_payments``,
  ``cohort_retention``) error during qualification because their compiled SQL
  inlines an ephemeral as a ``__dbt__cte__<name>`` CTE whose source columns
  aren't in the catalog.

Each test below encodes a property that the fixed extractor must satisfy.
"""

from __future__ import annotations

import os

import pytest

from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor


FIXTURE_DIR = os.path.join("tests", "test_data", "ephemeral_finance_core")
MANIFEST = os.path.join(FIXTURE_DIR, "manifest.json")
CATALOG = os.path.join(FIXTURE_DIR, "catalog.json")

EPHEMERAL_IDS = [
    "model.finance_core.int_subscription_plan_changes",
    "model.finance_core.int_customer_cohort_key",
    "model.finance_core.int_invoice_line_items",
    "model.finance_core.int_customer_spine",
    "model.finance_core.int_daily_revenue",
    "model.finance_core.int_payment_method_stats",
]


@pytest.fixture(scope="module")
def lineage_result():
    if not (os.path.exists(MANIFEST) and os.path.exists(CATALOG)):
        pytest.skip("ephemeral_finance_core fixture not present")
    extractor = DbtColumnLineageExtractor(
        manifest_path=MANIFEST,
        catalog_path=CATALOG,
    )
    return extractor.extract_project_lineage()


def _column_parents(parents_map, model_id, column_name):
    """Return the list of parent records for a model.column, or [] if missing.

    Lookup is case-insensitive on the column key because the extractor lowercases
    unquoted identifiers; on duckdb everything is unquoted.
    """
    model_entry = parents_map.get(model_id) or {}
    if column_name in model_entry:
        return model_entry[column_name]
    lower = column_name.lower()
    for k, v in model_entry.items():
        if k.lower() == lower:
            return v
    return []


# ---------------------------------------------------------------------------
# Per-ephemeral projection coverage
# ---------------------------------------------------------------------------

EXPECTED_EPHEMERAL_COLUMNS = {
    "model.finance_core.int_invoice_line_items": {
        "invoice_id", "customer_id", "subscription_id", "plan_id", "plan_name",
        "tier", "subtotal", "tax", "total", "estimated_cost", "estimated_margin",
        "issued_at", "due_at", "is_paid",
    },
    "model.finance_core.int_payment_method_stats": {
        "customer_id", "payment_method", "method_usage_count",
        "method_total_amount", "first_used_at", "last_used_at", "method_rank",
    },
    "model.finance_core.int_customer_spine": {
        "customer_id", "customer_name", "billing_email", "country_code",
        "customer_since", "first_subscription_date", "latest_subscription_date",
        "total_subscriptions", "first_payment_date", "total_payments",
        "has_subscribed", "has_paid",
    },
    "model.finance_core.int_customer_cohort_key": {
        "customer_id", "customer_name", "country_code", "customer_since",
        "first_subscription_date", "has_subscribed", "has_paid",
        "total_subscriptions", "total_payments", "lifetime_invoiced",
        "lifetime_paid", "outstanding_balance", "preferred_payment_method",
        "cohort_month", "customer_health",
    },
    "model.finance_core.int_subscription_plan_changes": {
        # The model does `select *, change_type, mrr_change` from a CTE
        # whose body is `select s.*, p.normalized_monthly_price, ...`.  The
        # algorithm must recurse through the * to produce these columns.
        "subscription_id", "customer_id", "plan_id", "plan_name", "tier",
        "normalized_monthly_price", "started_at", "ended_at", "is_active",
        "previous_plan_id", "previous_mrr", "change_type", "mrr_change",
    },
    "model.finance_core.int_daily_revenue": {
        # `select * from invoice_revenue union all select * from refund_offsets`
        # — the * must be expanded from each CTE's projection.
        "revenue_date", "revenue_source", "transaction_count",
        "gross_revenue", "tax_collected", "total_revenue",
    },
}


@pytest.mark.parametrize("eph_id", list(EXPECTED_EPHEMERAL_COLUMNS))
def test_ephemeral_has_projection_columns(lineage_result, eph_id):
    """Every ephemeral must end up with one parents-map entry per projected column."""
    parents = lineage_result["lineage"]["parents"]
    assert eph_id in parents, f"{eph_id} missing from parents map"

    expected = EXPECTED_EPHEMERAL_COLUMNS[eph_id]
    actual_keys = {k for k in parents[eph_id] if not k.startswith("__colibri_")}
    actual_lower = {k.lower() for k in actual_keys}
    missing = {c for c in expected if c.lower() not in actual_lower}
    assert not missing, (
        f"{eph_id} is missing projection columns: {sorted(missing)}.\n"
        f"Got: {sorted(actual_keys)}"
    )


# ---------------------------------------------------------------------------
# Specific column traces
# ---------------------------------------------------------------------------

def test_pass_through_column_traces_to_source(lineage_result):
    """``int_invoice_line_items.invoice_id`` is a pass-through from ``stg_invoices.invoice_id``."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(parents, "model.finance_core.int_invoice_line_items", "invoice_id")
    assert refs, "no parents for int_invoice_line_items.invoice_id"
    assert any(
        r["dbt_node"] == "model.finance_core.stg_invoices" and r["column"] == "invoice_id"
        for r in refs
    ), f"expected stg_invoices.invoice_id; got {refs}"


def test_computed_column_traces_to_input(lineage_result):
    """``int_invoice_line_items.estimated_cost`` is ``i.subtotal * 0.85`` → stg_invoices.subtotal."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(parents, "model.finance_core.int_invoice_line_items", "estimated_cost")
    assert refs, "no parents for estimated_cost"
    assert any(
        r["dbt_node"] == "model.finance_core.stg_invoices" and r["column"] == "subtotal"
        for r in refs
    ), f"expected stg_invoices.subtotal; got {refs}"


def test_join_column_traces_to_correct_parent(lineage_result):
    """``int_invoice_line_items.plan_name`` comes from ``stg_plans.plan_name`` via join."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(parents, "model.finance_core.int_invoice_line_items", "plan_name")
    assert refs
    assert any(
        r["dbt_node"] == "model.finance_core.stg_plans" and r["column"] == "plan_name"
        for r in refs
    ), f"expected stg_plans.plan_name; got {refs}"


def test_window_function_traces_through(lineage_result):
    """``int_subscription_plan_changes.previous_plan_id`` is ``lag(s.plan_id) over ...``."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(
        parents,
        "model.finance_core.int_subscription_plan_changes",
        "previous_plan_id",
    )
    assert refs
    assert any(
        r["dbt_node"] == "model.finance_core.stg_subscriptions" and r["column"] == "plan_id"
        for r in refs
    ), f"expected stg_subscriptions.plan_id; got {refs}"


def test_union_all_traces_to_both_branches(lineage_result):
    """``int_daily_revenue.total_revenue`` unions invoice_revenue (stg_invoices.total)
    and refund_offsets (stg_refunds.amount)."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(
        parents,
        "model.finance_core.int_daily_revenue",
        "total_revenue",
    )
    assert refs
    parent_nodes = {(r["dbt_node"], r["column"]) for r in refs}
    assert ("model.finance_core.stg_invoices", "total") in parent_nodes, parent_nodes
    assert ("model.finance_core.stg_refunds", "amount") in parent_nodes, parent_nodes


def test_select_star_recurses_into_parent(lineage_result):
    """``int_subscription_plan_changes.normalized_monthly_price`` flows through a
    ``select s.*, p.normalized_monthly_price`` CTE — the * must be expanded so
    ``normalized_monthly_price`` is recognised as a projected column traceable to
    ``stg_plans``."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(
        parents,
        "model.finance_core.int_subscription_plan_changes",
        "normalized_monthly_price",
    )
    assert refs, "select-* recursion failed: column not in lineage"
    assert any(
        r["dbt_node"] == "model.finance_core.stg_plans"
        and r["column"] == "normalized_monthly_price"
        for r in refs
    ), f"expected stg_plans.normalized_monthly_price; got {refs}"


# ---------------------------------------------------------------------------
# Children map: ephemerals must register as parents of their consumers
# ---------------------------------------------------------------------------

def test_ephemeral_appears_in_children_map(lineage_result):
    """Once the algorithm processes ephemerals, every ephemeral with downstream
    consumers should appear in the project-wide ``children`` map."""
    children = lineage_result["lineage"]["children"]
    # int_invoice_line_items is consumed by fct_invoices and int_payment_applications
    assert "model.finance_core.int_invoice_line_items" in children, (
        f"int_invoice_line_items missing from children map: "
        f"{sorted(k for k in children if 'int_' in k)}"
    )
    consumers = {
        ref["dbt_node"]
        for col_refs in children["model.finance_core.int_invoice_line_items"].values()
        for ref in col_refs
    }
    assert "model.finance_core.fct_invoices" in consumers, consumers


# ---------------------------------------------------------------------------
# Ephemeral-of-ephemeral via __dbt__cte__ wrapping
# ---------------------------------------------------------------------------

def test_ephemeral_consumes_ephemeral_via_cte(lineage_result):
    """``int_customer_cohort_key.preferred_payment_method`` reads from the CTE
    ``__dbt__cte__int_payment_method_stats`` whose body is the inlined ephemeral.
    The lineage must terminate at ``int_payment_method_stats.payment_method`` — not
    walk through to ``stg_payments.payment_method`` — so that the ephemeral
    appears in the chain."""
    parents = lineage_result["lineage"]["parents"]
    refs = _column_parents(
        parents,
        "model.finance_core.int_customer_cohort_key",
        "preferred_payment_method",
    )
    assert refs
    parent_nodes = {r["dbt_node"] for r in refs}
    assert "model.finance_core.int_payment_method_stats" in parent_nodes, (
        f"expected ephemeral int_payment_method_stats in lineage; got {parent_nodes}"
    )


def test_consumer_of_ephemeral_resolves_columns(lineage_result):
    """``dim_customers`` inlines ``int_customer_cohort_key`` (which itself inlines
    two further ephemerals).  Before the fix this errors during qualification.

    After the fix, the model must process AND its columns that flow through the
    ephemeral CTE must list the ephemeral as a direct parent (CTE-boundary rule).
    The full path back to ``stg_customers`` is then walked via the ephemeral's
    own lineage entry.
    """
    parents = lineage_result["lineage"]["parents"]
    assert "model.finance_core.dim_customers" in parents, (
        "dim_customers must produce lineage (today it errors out)"
    )
    refs = _column_parents(parents, "model.finance_core.dim_customers", "customer_name")
    assert refs
    assert any(
        r["dbt_node"] == "model.finance_core.int_customer_cohort_key"
        and r["column"] == "customer_name"
        for r in refs
    ), f"expected int_customer_cohort_key.customer_name; got {refs}"


def test_consumer_attaches_to_ephemeral_directly(lineage_result):
    """Following the CTE-boundary rule, a dim_customers column that reads from
    ``__dbt__cte__int_customer_cohort_key`` should list the ephemeral as a
    direct parent rather than walking past it to the underlying source."""
    parents = lineage_result["lineage"]["parents"]
    # outstanding_balance flows: stg_invoices/stg_refunds → int_customer_balances
    # → int_customer_cohort_key → dim_customers, with the cohort_key step inlined
    # as a __dbt__cte__ in dim_customers' SQL.
    refs = _column_parents(parents, "model.finance_core.dim_customers", "outstanding_balance")
    assert refs
    parent_nodes = {r["dbt_node"] for r in refs}
    assert "model.finance_core.int_customer_cohort_key" in parent_nodes, (
        f"expected ephemeral as direct parent; got {parent_nodes}"
    )


# ---------------------------------------------------------------------------
# Errors regression
# ---------------------------------------------------------------------------

def test_no_qualification_errors_on_ephemeral_consumers(lineage_result):
    """``dim_customers`` and ``cohort_retention`` error today because
    ``__dbt__cte__<eph>`` CTEs in their compiled SQL reference tables whose
    columns are unknown to sqlglot.  After ephemeral schema injection both must
    process cleanly.

    ``fct_payments`` is intentionally excluded: it errors due to a stale
    catalog (``int_payment_applications`` selects ``plan_name``/``tier`` from
    an upstream ephemeral but the catalog reflects an older schema), which is
    a separate problem from ephemeral support.
    """
    error_node_ids = {e["node_id"] for e in lineage_result.get("errors", [])}
    regression = error_node_ids & {
        "model.finance_core.dim_customers",
        "model.finance_core.cohort_retention",
    }
    assert not regression, f"unexpected qualification errors: {regression}"
