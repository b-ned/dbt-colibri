import re
import threading
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.lineage import maybe_parse

# Snowflake accepts TOP as an ordinary identifier, but sqlglot's tokenizer always
# maps it to TokenType.TOP.  A qualified reference such as ``SELECT top.col`` is
# therefore parsed as the ``SELECT TOP <n>`` clause and blows up on the dot.
# Guards the temporary tokenizer mutation in parse_sql below.
_TOP_KEYWORD_LOCK = threading.Lock()


def parse_sql(sql, dialect):
    """``maybe_parse`` with a recovery pass for Snowflake's TOP keyword clash.

    On Snowflake, a CTE or table aliased ``top`` makes ``SELECT top.col``
    unparseable (sqlglot reads TOP as the row-limit clause).  Real ``SELECT TOP
    5`` syntax is far more common than an object named ``top``, so TOP stays a
    keyword for the first attempt; only once that has failed do we retry with it
    demoted to a plain identifier.
    """
    try:
        return maybe_parse(sql, dialect=dialect)
    except ParseError:
        recovered = _parse_with_top_as_identifier(sql, dialect)
        if recovered is not None:
            return recovered
        raise


def _parse_with_top_as_identifier(sql, dialect):
    """Re-parse *sql* with Snowflake's TOP keyword demoted to an identifier.

    Returns ``None`` when the workaround does not apply or does not help, so the
    caller can re-raise the original, more meaningful error.
    """
    if dialect != "snowflake":
        return None

    from sqlglot.dialects.snowflake import Snowflake

    with _TOP_KEYWORD_LOCK:
        keywords = Snowflake.Tokenizer.KEYWORDS
        if "TOP" not in keywords:
            return None
        saved = keywords.pop("TOP")
        try:
            return maybe_parse(sql, dialect=dialect)
        except ParseError:
            return None
        finally:
            keywords["TOP"] = saved



_TSQL_BRACKETED_IDENTIFIER = re.compile(r"\[((?:[^\]]|\]\])*)\]")


def _unquote_tsql_identifier(match: re.Match) -> str:
    """Remove TSQL brackets and unescape literal closing brackets."""
    return match.group(1).replace("]]", "]")


def normalize_table_relation_name(name: str, dialect=None) -> str:
    """Normalize manifest relation names for matching against SQLGlot tables.

    TSQL-family adapters emit bracket-quoted relation names in dbt artifacts,
    while SQLGlot exposes the corresponding table components without brackets.
    Only remove square-bracket quoting for those dialects so literal brackets in
    identifiers from other dialects remain untouched.
    """
    no_quotes = re.sub(r'"', '', name)
    no_ticks = re.sub(r'`', '', no_quotes)
    if dialect in {"fabric", "sqlserver", "tsql"}:
        return _TSQL_BRACKETED_IDENTIFIER.sub(_unquote_tsql_identifier, no_ticks)
    return no_ticks

def remove_quotes(expression):
    """Version 2: More aggressive approach"""
    def transform_identifier(node):
        if isinstance(node, exp.Identifier) and node.quoted:
            unquoted = node.this
            # print(f"    Converting identifier: {node.this!r} (quoted={node.quoted}) -> {unquoted}")
            return exp.Identifier(this=unquoted, quoted=False)
        return node

    return expression.transform(transform_identifier)

def remove_upper(expression):
    """Version 2: More aggressive approach"""
    def transform_identifier(node):
        if isinstance(node, exp.Identifier) and node.quoted:
            unquoted = node.this.lower()
            # print(f"    Converting identifier: {node.this!r} (quoted={node.quoted}) -> {unquoted}")
            return exp.Identifier(this=unquoted, quoted=True)
        return node

    return expression.transform(transform_identifier)
