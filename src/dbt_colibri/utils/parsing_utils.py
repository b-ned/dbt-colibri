import re
from sqlglot import exp

def normalize_table_relation_name(name: str) -> str:
    # Remove surrounding quotes
    no_quotes = re.sub(r'"', '', name)
    no_ticks = re.sub(r'`', '', no_quotes)
    # Lowercase for safety
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