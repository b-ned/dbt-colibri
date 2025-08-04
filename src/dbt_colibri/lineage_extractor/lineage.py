# This file is a modified version of a lineage.py file from sqlglot (https://github.com/tobymao/sqlglot)
# Original License: MIT
# Modifications Copyright (c) 2025 b-ned

from __future__ import annotations
import logging
import typing as t
from dataclasses import dataclass, field

from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, normalize_identifiers, qualify
from sqlglot.optimizer.scope import ScopeType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger("sqlglot")


@dataclass(frozen=True)
class Node:
    name: str
    expression: exp.Expression
    source: exp.Expression
    downstream: t.List[Node] = field(default_factory=list)
    source_name: str = ""
    reference_node_name: str = ""

    def walk(self) -> t.Iterator[Node]:
        yield self

        for d in self.downstream:
            yield from d.walk()

    

def prepare_scope(
    sql: exp.Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    sources: t.Optional[t.Mapping[str, str | exp.Query]] = None,
    dialect: DialectType = None,
    **kwargs,
) -> tuple[exp.Expression, Scope]:
    """Qualify and build the scope once, for reuse across many lineage calls."""
    expression = sql

    if sources:
        expression = exp.expand(
            expression,
            {k: t.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
            dialect=dialect,
        )

    expression = qualify.qualify(
        expression,
        dialect=dialect,
        schema=schema,
        **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
    )

    scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, SQL must be SELECT")

    return expression, scope


def lineage(
    column: str | exp.Column,
    sql: str | exp.Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    sources: t.Optional[t.Mapping[str, str | exp.Query]] = None,
    dialect: DialectType = None,
    scope: t.Optional[Scope] = None,
    trim_selects: bool = True,
    **kwargs,
) -> Node:
    """Build the lineage graph for a column of a SQL query.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building lineage.
        dialect: The dialect of input SQL.
        scope: A pre-created scope to use instead.
        trim_selects: Whether or not to clean up selects by trimming to only relevant columns.
        **kwargs: Qualification optimizer kwargs.

    Returns:
        A lineage node.
    """

    # expression = maybe_parse(sql, dialect=dialect)
    expression = sql # Already parsed upstream
    column = normalize_identifiers.normalize_identifiers(column, dialect=dialect).name

    if sources:
        expression = exp.expand(
            expression,
            {k: t.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
            dialect=dialect,
        )

    if not scope:
        expression = qualify.qualify(
            expression,
            dialect=dialect,
            schema=schema,
            **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
        )

        scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    select_names = {select.alias_or_name for select in scope.expression.selects}
    if column not in select_names:
        raise SqlglotError(f"Cannot find column '{column}' in query.")

    return to_node(column, scope, dialect, trim_selects=trim_selects)


def to_node(
    column: str | int,
    scope: Scope,
    dialect: DialectType,
    scope_name: t.Optional[str] = None,
    upstream: t.Optional[Node] = None,
    source_name: t.Optional[str] = None,
    reference_node_name: t.Optional[str] = None,
    trim_selects: bool = True,
    visited: t.Optional[set] = None,  # <-- Add visited set
) -> Node:
    if visited is None:
        visited = set()

    key = (column, id(scope))
    if key in visited:
        # Already visited this column-scope, skip recursion
        return None
    visited.add(key)

    # Find the specific select clause that is the source of the column we want.
    select = (
        scope.expression.selects[column]
        if isinstance(column, int)
        else next(
            (select for select in scope.expression.selects if select.alias_or_name == column),
            exp.Star() if scope.expression.is_star else scope.expression,
        )
    )

    if isinstance(scope.expression, exp.Subquery):
        for source in scope.subquery_scopes:
            return to_node(
                column,
                scope=source,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                visited=visited,
            )
    if isinstance(scope.expression, exp.SetOperation):
        name = type(scope.expression).__name__.upper()
        upstream = upstream or Node(name=name, source=scope.expression, expression=select)

        index = (
            column
            if isinstance(column, int)
            else next(
                (
                    i
                    for i, select in enumerate(scope.expression.selects)
                    if select.alias_or_name == column or select.is_star
                ),
                -1,
            )
        )

        if index == -1:
            raise ValueError(f"Could not find {column} in {scope.expression}")

        for s in scope.union_scopes:
            to_node(
                index,
                scope=s,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                visited=visited,
            )

        return upstream

    if trim_selects and isinstance(scope.expression, exp.Select):
        source = exp.Select()
        source.set("expressions", [select])
        source.set("from", scope.expression.args.get("from"))
        source.set("where", scope.expression.args.get("where"))
        source.set("group", scope.expression.args.get("group"))
    else:
        source = scope.expression

    node = Node(
        name=f"{scope_name}.{column}" if scope_name else str(column),
        source=source,
        expression=select,
        source_name=source_name or "",
        reference_node_name=reference_node_name or "",
    )

    if upstream:
        upstream.downstream.append(node)

    subquery_scopes = {
        id(subquery_scope.expression): subquery_scope for subquery_scope in scope.subquery_scopes
    }

    for subquery in find_all_in_scope(select, exp.UNWRAPPED_QUERIES):
        subquery_scope = subquery_scopes.get(id(subquery))
        if not subquery_scope:
            logger.warning(f"Unknown subquery scope: {subquery.sql(dialect=dialect)}")
            continue

        for name in subquery.named_selects:
            to_node(
                name,
                scope=subquery_scope,
                dialect=dialect,
                upstream=node,
                trim_selects=trim_selects,
                visited=visited,
            )

    if select.is_star:
        for source in scope.sources.values():
            if isinstance(source, Scope):
                source = source.expression
            node.downstream.append(
                Node(name=select.sql(comments=False), source=source, expression=source)
            )

    source_columns = set(find_all_in_scope(select, exp.Column))

    if isinstance(source, exp.UDTF):
        source_columns |= set(source.find_all(exp.Column))
        derived_tables = [
            source.expression.parent
            for source in scope.sources.values()
            if isinstance(source, Scope) and source.is_derived_table
        ]
    else:
        derived_tables = scope.derived_tables

    source_names = {
        dt.alias: dt.comments[0].split()[1]
        for dt in derived_tables
        if dt.comments and dt.comments[0].startswith("source: ")
    }

    for c in source_columns:
        table = c.table
        source = scope.sources.get(table)

        if isinstance(source, Scope):
            reference_node_name = None
            if source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                reference_node_name = table
            elif source.scope_type == ScopeType.CTE:
                selected_node, _ = scope.selected_sources.get(table, (None, None))
                reference_node_name = selected_node.name if selected_node else None

            to_node(
                c.name,
                scope=source,
                dialect=dialect,
                scope_name=table,
                upstream=node,
                source_name=source_names.get(table) or source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
                visited=visited,
            )
        else:
            source = source or exp.Placeholder()
            node.downstream.append(
                Node(name=c.sql(comments=False), source=source, expression=source)
            )

    return node
