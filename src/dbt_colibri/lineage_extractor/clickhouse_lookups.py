"""Columns read by ClickHouse lookup functions (dictGet*, dictHas, joinGet*, ...).

sqlglot parses these calls as exp.Anonymous and the dictionary or Join table they
read is not a FROM source, so scope-based lineage never reaches it. This module
names the relation and the columns each call reads, e.g.
``dictGet(db.customers, ('name', 'segment'), id)`` -> ``(db.customers, name)``,
``(db.customers, segment)``; ``dictHas(db.customers, id)`` -> the dictionary key.

Function reference: https://clickhouse.com/docs/sql-reference/functions/ext-dict-functions
and https://clickhouse.com/docs/sql-reference/functions/other-functions#joinget
"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import Scope, find_all_in_scope

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

_DICT_GET_TYPES = (
    "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64",
    "Float32", "Float64", "Date", "DateTime", "UUID", "IPv4", "IPv6", "String",
)

# Calls whose second argument names the attribute (or a tuple of attributes) they read.
ATTRIBUTE_LOOKUPS = frozenset(
    {
        "dictGet", "dictGetOrDefault", "dictGetOrNull", "dictGetAll", "dictGetKeys",
        "joinGet", "joinGetOrNull",
        *(f"dictGet{type_name}" for type_name in _DICT_GET_TYPES),
        *(f"dictGet{type_name}OrDefault" for type_name in _DICT_GET_TYPES),
    }
)

# Calls that read the dictionary key. The hierarchy functions also read the
# HIERARCHICAL attribute, which the call does not name, so it gets no edge.
KEY_LOOKUPS = frozenset(
    {
        "dictHas", "dictGetKeys", "dictIsIn", "dictGetHierarchy", "dictGetChildren",
        "dictGetDescendants", "dictGetRoot",
    }
)


def lookup_columns(
    expression: exp.Expression,
    scope: Scope,
    dialect: DialectType,
    dictionary_keys: t.Optional[t.Mapping[str, t.Sequence[str]]] = None,
) -> t.List[t.Tuple[exp.Table, str]]:
    """Returns (relation, column) for every column the lookup calls in ``expression`` read.

    ``dictionary_keys`` maps a lowercase ``db.name`` to the dictionary's key columns.
    A call whose relation or attribute is not a literal name yields nothing, so an
    unknown read is a missing edge, never a wrong one.
    """
    if not isinstance(Dialect.get_or_raise(dialect), ClickHouse):
        return []

    columns = []
    for call in find_all_in_scope(expression, exp.Anonymous):
        name = call.name
        if name not in ATTRIBUTE_LOOKUPS and name not in KEY_LOOKUPS:
            continue
        arguments = call.expressions
        table = _relation(arguments[0], scope) if arguments else None
        if table is None:
            continue
        names: t.List[str] = []
        if name in ATTRIBUTE_LOOKUPS and len(arguments) > 1:
            names.extend(_literal_names(arguments[1]))
        if name in KEY_LOOKUPS:
            names.extend((dictionary_keys or {}).get(f"{table.db}.{table.name}".lower(), ()))
        # The extractor reads the column as the last dot-separated part of a node name.
        columns.extend((table, column) for column in names if "." not in column)
    return columns


def _relation(argument: exp.Expression, scope: Scope) -> t.Optional[exp.Table]:
    """'db.name' or db.name -> Table; None when the database is not spelled out."""
    argument = argument.unnest()
    if isinstance(argument, exp.Literal) and argument.is_string:
        try:
            table = exp.to_table(argument.name, dialect="clickhouse")
        except ParseError:
            return None
    elif (
        isinstance(argument, exp.Column)
        and len(argument.parts) == 2
        # A FROM alias here means the optimizer qualified a bare name, or it is a real column.
        and argument.table not in scope.sources
    ):
        table = exp.table_(argument.name, db=argument.table)
    else:
        return None
    if not table.db or table.catalog:
        return None
    return table


def _literal_names(argument: exp.Expression) -> t.List[str]:
    """'a' -> ['a']; ('a', 'b') or tuple('a', 'b') -> ['a', 'b']; anything else -> []."""
    argument = argument.unnest()
    items = argument.expressions if isinstance(argument, (exp.Tuple, exp.Struct)) else [argument]
    return [item.name for item in items if isinstance(item, exp.Literal) and item.is_string]
