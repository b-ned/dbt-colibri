"""Regression tests for Snowflake TOP-keyword recovery.

Found via a production ingestion failure: a model with a CTE aliased ``top``
could not be parsed at all, losing that model's lineage entirely. The SQL here
is synthetic, written to reproduce the shape involved.
"""

import pytest
from sqlglot.errors import ParseError

from dbt_colibri.utils.parsing_utils import parse_sql


class TestSnowflakeTopIdentifier:
    """``top`` is a legal identifier in Snowflake but a keyword to sqlglot."""

    @pytest.mark.parametrize(
        "sql",
        [
            "select top.col from t as top",
            "select top.* from t as top",
            "with top as (select 1 as a) select top.* from top",
        ],
    )
    def test_recovers_qualified_references_to_an_object_named_top(self, sql):
        assert parse_sql(sql, dialect="snowflake") is not None

    @pytest.mark.parametrize(
        "sql",
        [
            "select top 5 a from t",
            "select top (5) a from t",
            "select top 5 t.a from t",
        ],
    )
    def test_real_top_clause_still_parses(self, sql):
        parsed = parse_sql(sql, dialect="snowflake")
        assert parsed.args.get("limit") is not None

    def test_top_keyword_is_restored_after_recovery(self):
        from sqlglot.dialects.snowflake import Snowflake

        parse_sql("select top.col from t as top", dialect="snowflake")
        assert "TOP" in Snowflake.Tokenizer.KEYWORDS

        # The keyword still applies, so real TOP syntax is unaffected afterwards.
        assert parse_sql("select top 5 a from t", dialect="snowflake").args.get("limit")

    def test_unrelated_syntax_errors_still_raise(self):
        with pytest.raises(ParseError):
            parse_sql("select from from from", dialect="snowflake")

    def test_other_dialects_are_untouched(self):
        # Dialects without a TOP keyword parse this natively; no recovery needed.
        assert parse_sql("select top.col from t as top", dialect="postgres") is not None
