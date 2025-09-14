import re

def normalize_table_relation_name(name: str) -> str:
    # Remove surrounding quotes
    no_quotes = re.sub(r'"', '', name)
    no_ticks = re.sub(r'`', '', no_quotes)
    # Lowercase for safety
    return no_ticks