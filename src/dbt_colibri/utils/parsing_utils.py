import re
from sqlglot import exp


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
