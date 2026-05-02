
from sqlglot.lineage import maybe_parse, SqlglotError, exp
import logging
from ..utils import json_utils, parsing_utils
from .lineage import lineage, prepare_scope, extract_structural_lineage
import re
from importlib.metadata import version, PackageNotFoundError
import gc


def _normalize_column_name(name: str) -> str:
    """Normalize a column name for lineage resolution.

    - Strips surrounding single/double quotes.
    - Removes PostgreSQL-style type casts (``::type``).
    - Strips leading ``$`` (Snowflake session variable prefix).
    """
    name = name.strip('"').strip("'")
    name = re.sub(r"::\s*\w+$", "", name)
    if name.startswith("$"):
        name = name[1:]
    return name


def get_select_expressions(expr: exp.Expression) -> list[exp.Expression]:
    if isinstance(expr, exp.Select):
        return expr.expressions
    elif isinstance(expr, exp.Subquery):
        return get_select_expressions(expr.this)
    elif isinstance(expr, exp.CTE):
        return get_select_expressions(expr.this)
    elif isinstance(expr, exp.With):
        return get_select_expressions(expr.this)
    elif hasattr(expr, "args") and "this" in expr.args:
        return get_select_expressions(expr.args["this"])
    return []

def extract_column_refs(expr: exp.Expression) -> list[exp.Column]:
    return list(expr.find_all(exp.Column))

class DbtColumnLineageExtractor:
    # CTE name pattern dbt uses when inlining ephemeral models into a downstream
    # model's compiled SQL.
    _EPHEMERAL_CTE_PREFIX = "__dbt__cte__"

    def __init__(self, manifest_path, catalog_path, selected_models=[]):
        # Set up logging
        self.logger = logging.getLogger("colibri")

        # Read manifest and catalog files
        self.manifest = json_utils.read_json(manifest_path)
        self.catalog = json_utils.read_json(catalog_path)
        self.schema_dict = self._generate_schema_dict_from_catalog()
        self.dialect = self._detect_adapter_type()
        # self.node_mapping = self._get_dict_mapping_full_table_name_to_dbt_node()
        self._quoted_columns_lookup = self._build_quoted_columns_lookup()
        self.nodes_with_columns = self.build_nodes_with_columns()
        self._table_to_node = {k.lower(): v for k, v in self.nodes_with_columns.items()}
        # Store references to parent and child maps for easy access
        self.parent_map = self.manifest.get("parent_map", {})
        self.child_map = self.manifest.get("child_map", {})

        # Ephemeral models don't appear in catalog.json, so we have to derive
        # their column lists from compiled SQL.  Build a registry up-front; the
        # actual column resolution is lazy so it only runs for ephemerals that
        # are actually referenced by a model under analysis.
        self._ephemeral_registry = self._build_ephemeral_registry()
        # Reverse map from CTE name -> ephemeral unique_id, for the
        # `__dbt__cte__<name>` re-attribution pass.
        self._ephemeral_cte_to_node = {
            f"{self._EPHEMERAL_CTE_PREFIX}{entry['name']}".lower(): nid
            for nid, entry in self._ephemeral_registry.items()
        }
        # Process selected models
        self.colibri_version = self._get_colibri_version()
        # Respect user-provided selection; otherwise default to all model/snapshot nodes
        if selected_models:
            self.selected_models = selected_models
        else:
            self.selected_models = [
                node
                for node in self.manifest["nodes"].keys()
                if self.manifest["nodes"][node].get("resource_type") in ("model", "snapshot")
            ]

        # Run validation checks
        self._validate_models()

    def _validate_models(self):
        """
        Validate models in manifest and catalog:
        1. Check for missing compiled SQL.
        2. Check for non-materialized models.

        Stores validation counts as instance attributes for downstream consumers:
        - self.total_model_count: total number of models in manifest
        - self.unmaterialized_model_count: models missing from catalog
        """
        all_models = [
            node_id for node_id, node in self.manifest.get("nodes", {}).items()
            if node.get("resource_type") == "model"
        ]

        self.total_model_count = len(all_models)

        # --- Missing compiled SQL ---
        missing_compiled = [
            node_id for node_id in all_models
            if not self.manifest["nodes"][node_id].get("compiled_code")
        ]

        if missing_compiled:
            total = len(all_models)
            missing = len(missing_compiled)
            msg = f"{missing}/{total} models are missing compiled SQL. Ensure dbt compile was run."

            self.logger.error(msg)

        # --- Non-materialized models (missing from catalog) ---
        catalog_models = set(self.catalog.get("nodes", {}).keys())
        non_materialized = set(all_models) - catalog_models
        self.unmaterialized_model_count = len(non_materialized)

        if non_materialized:
            ephemeral_missing = {
                nid for nid in non_materialized
                if self.manifest["nodes"][nid].get("config", {}).get("materialized") == "ephemeral"
            }
            other_missing = non_materialized - ephemeral_missing
            if ephemeral_missing:
                self.logger.info(
                    f"{len(ephemeral_missing)}/{len(all_models)} ephemeral models not in catalog (columns derived from SQL)."
                )
            if other_missing:
                self.logger.error(
                    f"{len(other_missing)}/{len(all_models)} non-ephemeral models missing from catalog."
                )

    def _get_colibri_version(self):
        try:
            return version("dbt-colibri")
        except PackageNotFoundError:
            return "unknown"
        
    # Dialects where ``quote: true`` means the identifier is case-sensitive.
    _CASE_SENSITIVE_QUOTE_DIALECTS = frozenset({
        "snowflake", "postgres", "oracle", "clickhouse",
    })

    def _build_quoted_columns_lookup(self):
        """Build a lookup of quoted columns (quote=True) across all manifest nodes.

        Returns a dict of ``{node_id: {lower_col_name: original_col_name}}``
        so that downstream code can preserve the original casing for columns
        explicitly marked as quoted in the dbt manifest.

        Only populated for dialects where quoting is case-sensitive (e.g.
        Snowflake, Postgres, Oracle, ClickHouse).  On case-insensitive
        dialects (BigQuery, DuckDB, etc.) quoting is used to escape reserved
        words but does not affect casing, so the lookup stays empty.
        """
        if self.dialect not in self._CASE_SENSITIVE_QUOTE_DIALECTS:
            return {}
        lookup = {}
        for node_id, node_data in {**self.manifest.get("nodes", {}), **self.manifest.get("sources", {})}.items():
            columns = node_data.get("columns")
            if not columns:
                continue
            quoted = {}
            for col_name, col_info in columns.items():
                if col_info.get("quote") is True:
                    quoted[col_name.lower()] = col_name
            if quoted:
                lookup[node_id] = quoted
        return lookup

    def _get_quoted_columns(self, node_id):
        """Return ``{lower_col_name: original_col_name}`` for quoted columns of *node_id*."""
        lookup = getattr(self, "_quoted_columns_lookup", None)
        if lookup is None:
            return {}
        return lookup.get(node_id, {})

    def _resolve_column_name(self, column_name, node_id):
        """Return the properly-cased column name.

        For columns with ``quote: true`` in the manifest, the original casing
        is preserved.  All other columns are lowercased for consistency.
        Handles column names that may arrive with surrounding double-quotes
        from SQLGlot (e.g. ``'"quotedCol"'``).
        """
        # Strip surrounding double-quotes that SQLGlot may add
        stripped = column_name.strip('"')
        col_lower = stripped.lower()
        quoted = self._get_quoted_columns(node_id)
        if col_lower in quoted:
            return quoted[col_lower]
        return col_lower

    def _detect_adapter_type(self):
        """
        Detect the adapter type from the manifest metadata.
        
        Returns:
            str: The detected adapter type
            
        Raises:
            ValueError: If adapter_type is not found or not supported
        """
        SUPPORTED_ADAPTERS = {'snowflake', 'bigquery', 'redshift', 'duckdb', 'postgres', 'databricks', 'athena', 'trino', 'sqlserver', 'clickhouse', 'oracle'}
        
        # Get adapter_type from manifest metadata
        adapter_type = self.manifest.get("metadata", {}).get("adapter_type")
        
        if not adapter_type:
            raise ValueError(
                "adapter_type not found in manifest metadata. "
                "Please ensure you're using a valid dbt manifest.json file."
            )
        
        if adapter_type not in SUPPORTED_ADAPTERS:
            raise ValueError(
                f"Unsupported adapter type '{adapter_type}'. "
                f"Supported adapters are: {', '.join(sorted(SUPPORTED_ADAPTERS))}"
            )
        
        self.logger.info(f"Detected adapter type: {adapter_type}")
        if adapter_type == "sqlserver":
            # Adapter type != Dialect Name for all adapters.
            return "tsql"
        
        return adapter_type

    def build_nodes_with_columns(self):
        """
        Merge manifest nodes with catalog columns, keyed by normalized relation_name.
        """
        merged = {}

        # Go over models/sources/seeds/snapshots
        for node_id, node in {**self.manifest["nodes"], **self.manifest["sources"]}.items():
            if node.get("resource_type") not in ["model", "source", "seed", "snapshot"]:
                continue
            # Skip nodes without relation_name (e.g., operations) unless they're ephemeral
            if node.get('config', {}).get('materialized') != 'ephemeral' and not node.get("relation_name"):
                continue
            if node['config'].get('materialized') == 'ephemeral':
                relation_name = node['database'] + '.' + node['schema'] + '.' + node.get('alias', node.get('name'))
            else:
                relation_name = parsing_utils.normalize_table_relation_name(node["relation_name"])

            # Start with manifest node info
            merged[relation_name] = {
                "unique_id": node_id,
                "database": node.get("database"),
                "schema": node.get("schema"),
                "name": node.get("alias") or node.get("identifier") or node.get("name"),
                "resource_type": node.get("resource_type"),
                "columns": {},
            }

            # Add richer column info from catalog if available
            if node_id in self.catalog.get("nodes", {}):
                merged[relation_name]["columns"] = self.catalog["nodes"][node_id]["columns"]
            elif node_id in self.catalog.get("sources", {}):
                merged[relation_name]["columns"] = self.catalog["sources"][node_id]["columns"]

        return merged

    def build_table_to_node(self):
        """
        Build a minimal mapping from normalized relation name to the dbt unique_id.
        This avoids holding full column structures in memory.
        """
        mapping = {}
        all_nodes = {**self.manifest.get("nodes", {}), **self.manifest.get("sources", {})}
        for node_id, node in all_nodes.items():
            if node.get("resource_type") not in ["model", "source", "seed", "snapshot"]:
                continue
            try:
                if node.get("config", {}).get("materialized") == "ephemeral":
                    relation_name = (
                        (node.get("database") or "").strip() + "." +
                        (node.get("schema") or "").strip() + "." +
                        (node.get("alias") or node.get("name"))
                    )
                else:
                    relation_name = parsing_utils.normalize_table_relation_name(node["relation_name"])
                if relation_name:
                    mapping[str(relation_name).lower()] = node_id
            except Exception as e:
                self.logger.debug(f"Skipping node {node_id} while building table map: {e}")
                continue
        return mapping
    
    def _generate_schema_dict_from_catalog(self, catalog=None):
        if not catalog:
            catalog = self.catalog
        schema_dict = {}

        def add_to_schema_dict(node):
            dbt_node = DBTNodeCatalog(node)
            db_name, schema_name, table_name = dbt_node.database, dbt_node.schema, dbt_node.name

            if db_name not in schema_dict:
                schema_dict[db_name] = {}
            if schema_name not in schema_dict[db_name]:
                schema_dict[db_name][schema_name] = {}
            if table_name not in schema_dict[db_name][schema_name]:
                schema_dict[db_name][schema_name][table_name] = {}

            col_types = dbt_node.get_column_types()

            # For columns with quote=True in the manifest, wrap the key in
            # double-quotes so SQLGlot's qualifier can match quoted identifiers.
            quoted_cols = self._get_quoted_columns(dbt_node.unique_id)
            if quoted_cols:
                wrapped = {}
                for col_name, col_type in col_types.items():
                    if col_name.lower() in quoted_cols:
                        original = quoted_cols[col_name.lower()]
                        wrapped[f'"{original}"'] = col_type
                    else:
                        wrapped[col_name] = col_type
                col_types = wrapped

            schema_dict[db_name][schema_name][table_name].update(col_types)

        for node in catalog.get("nodes", {}).values():
            add_to_schema_dict(node)

        for node in catalog.get("sources", {}).values():
            add_to_schema_dict(node)

        return schema_dict

    def _get_dict_mapping_full_table_name_to_dbt_node(self):
        mapping = {}
        for key, node in self.manifest["nodes"].items():
            # Only include model, source, and seed nodes
            if node.get("resource_type") in ["model", "source", "seed", "snapshot"]:
                try:
                    dbt_node = DBTNodeManifest(node)
                    mapping[dbt_node.full_table_name] = key
                except Exception as e:
                    self.logger.warning(f"Error processing node {key}: {e}")
        for key, node in self.manifest["sources"].items():
            try:
                dbt_node = DBTNodeManifest(node)
                mapping[dbt_node.full_table_name] = key
            except Exception as e:
                self.logger.warning(f"Error processing source {key}: {e}")
        return mapping

    def _get_list_of_columns_for_a_dbt_node(self, node):
        if node in self.catalog["nodes"]:
            columns = self.catalog["nodes"][node]["columns"]
        elif node in self.catalog["sources"]:
            columns = self.catalog["sources"][node]["columns"]
        elif node in self._ephemeral_registry:
            columns = self._resolve_ephemeral_columns(node)
            if not columns:
                self.logger.warning(
                    f"Ephemeral node {node} produced no columns from SQL parsing"
                )
            return [self._resolve_column_name(col, node) for col in columns.keys()]
        else:
            self.logger.warning(f"Node {node} not found in catalog, maybe it's not materialized")
            return []
        return [self._resolve_column_name(col, node) for col in columns.keys()]

    def _get_parent_nodes_catalog(self, model_info):
        """Build the parent-catalog dict used for schema qualification.

        Ephemeral parents are included with their resolved column projections
        but their transitive ancestors are NOT added — the inlined CTE bodies
        are replaced with column-only stubs by ``_stub_ephemeral_ctes`` so
        sqlglot never sees references to the underlying tables.
        """
        parent_nodes = list(model_info["depends_on"]["nodes"])
        parent_catalog = {"nodes": {}, "sources": {}}
        seen = set()
        queue = list(parent_nodes)
        while queue:
            parent = queue.pop()
            if parent in seen:
                continue
            seen.add(parent)
            if parent in self.catalog.get("nodes", {}):
                parent_catalog["nodes"][parent] = self.catalog["nodes"][parent]
            elif parent in self.catalog.get("sources", {}):
                parent_catalog["sources"][parent] = self.catalog["sources"][parent]
            elif parent in self._ephemeral_registry:
                parent_catalog["nodes"][parent] = self._ephemeral_catalog_entry(parent)
            else:
                if parent in parent_nodes:
                    self.logger.warning(f"Parent model {parent} not found in catalog")
        return parent_catalog

    # ------------------------------------------------------------------
    # Ephemeral model support
    # ------------------------------------------------------------------

    def _build_ephemeral_registry(self):
        """Index every ephemeral model from the manifest.

        Stores enough metadata to (a) resolve columns from SQL on demand and
        (b) inject synthesized schema entries for downstream qualification.
        """
        registry = {}
        for node_id, node in self.manifest.get("nodes", {}).items():
            if node.get("config", {}).get("materialized") != "ephemeral":
                continue
            registry[node_id] = {
                "unique_id": node_id,
                "database": node.get("database"),
                "schema": node.get("schema"),
                "name": node.get("alias") or node.get("identifier") or node.get("name"),
                "compiled_code": node.get("compiled_code") or "",
                "depends_on": (node.get("depends_on") or {}).get("nodes", []),
                # Cached column dict, populated by _resolve_ephemeral_columns().
                "columns": None,
            }
        return registry

    def _ephemeral_catalog_entry(self, node_id):
        """Return a catalog-shaped dict for an ephemeral node."""
        entry = self._ephemeral_registry[node_id]
        columns = self._resolve_ephemeral_columns(node_id)
        return {
            "unique_id": node_id,
            "metadata": {
                "database": entry["database"],
                "schema": entry["schema"],
                "name": entry["name"],
                "type": "ephemeral",
            },
            "columns": columns,
        }

    def _resolve_ephemeral_columns(self, node_id, _visiting=None):
        """Derive the projected columns of an ephemeral model from its compiled SQL.

        Recursive: ephemeral A may project ``select * from ephemeral B``; we
        resolve B first so its column list expands A's star.  Cycles are
        defended against with a visiting set.
        """
        entry = self._ephemeral_registry.get(node_id)
        if entry is None:
            return {}
        if entry["columns"] is not None:
            return entry["columns"]

        _visiting = _visiting or set()
        if node_id in _visiting:
            self.logger.warning(
                f"Ephemeral cycle detected at {node_id}; returning empty columns"
            )
            return {}
        _visiting = _visiting | {node_id}

        compiled = entry["compiled_code"]
        if not compiled:
            entry["columns"] = {}
            return entry["columns"]

        # Build the schema dict of this ephemeral's parents.  Catalog parents
        # contribute directly; ephemeral parents recurse.
        parent_catalog = {"nodes": {}, "sources": {}}
        for parent in entry["depends_on"]:
            if parent in self.catalog.get("nodes", {}):
                parent_catalog["nodes"][parent] = self.catalog["nodes"][parent]
            elif parent in self.catalog.get("sources", {}):
                parent_catalog["sources"][parent] = self.catalog["sources"][parent]
            elif parent in self._ephemeral_registry:
                # Recurse, then synthesize the catalog entry from the result.
                self._resolve_ephemeral_columns(parent, _visiting=_visiting)
                parent_catalog["nodes"][parent] = self._ephemeral_catalog_entry(parent)
        schema = self._generate_schema_dict_from_catalog(parent_catalog)

        try:
            sql = self._sanitize_sql_for_parsing(compiled)
            parsed = maybe_parse(sql, dialect=self.dialect)
            if self.dialect == "postgres" and not self._schema_has_quoted_keys(schema):
                parsed = parsing_utils.remove_quotes(parsed)
            if self.dialect == "bigquery":
                parsed = parsing_utils.remove_upper(parsed)
            qualified, _ = prepare_scope(parsed, schema=schema, dialect=self.dialect)
        except Exception as e:
            self.logger.warning(
                f"Failed to qualify ephemeral {node_id}: {e}; columns will be empty"
            )
            entry["columns"] = {}
            return entry["columns"]

        # The outermost SELECT (or the SELECT inside a WITH wrapper) carries
        # the projected columns.  For UNION-shaped ephemerals (`select *
        # from a union all select * from b`) the top expression is a
        # SetOperation and qualify expands the * inside each branch.
        outer = self._outer_select(qualified)
        columns = {}
        if outer is not None:
            for sel in outer.expressions:
                # qualify with expand_stars=True turns `*` into individual
                # columns; if it can't (e.g. star against an unresolvable
                # parent) the literal `*` survives — skip it with a warning.
                if isinstance(sel, exp.Star) or sel.is_star:
                    self.logger.warning(
                        f"Ephemeral {node_id} contains an unexpanded * — "
                        f"some parent lacked column information"
                    )
                    continue
                name = sel.alias_or_name
                if not name:
                    continue
                columns[name] = {
                    "type": "UNKNOWN",
                    "name": name,
                    "index": len(columns) + 1,
                    "comment": None,
                }

        entry["columns"] = columns
        return columns

    @staticmethod
    def _outer_select(expr):
        """Return the top-level node whose ``expressions`` are the projected columns.

        For plain ``SELECT`` queries this is the Select itself.  For set
        operations (``UNION``/``EXCEPT``/``INTERSECT``) the projection is
        defined by the leftmost branch — sqlglot stores it in ``args["this"]``.
        """
        if isinstance(expr, exp.Select):
            return expr
        if isinstance(expr, exp.SetOperation):
            # Recurse into the left branch until we hit a Select.
            return DbtColumnLineageExtractor._outer_select(expr.args.get("this"))
        inner = expr.args.get("this") if hasattr(expr, "args") else None
        if isinstance(inner, (exp.Select, exp.SetOperation)):
            return DbtColumnLineageExtractor._outer_select(inner)
        if hasattr(expr, "find"):
            sel = expr.find(exp.Select)
            if sel is not None:
                return sel
        return None

    def _match_ephemeral_cte(self, node_name):
        """If *node_name* refers into a ``__dbt__cte__<eph>`` CTE, return the
        ephemeral's unique_id; otherwise None.

        ``Node.name`` from sqlglot's lineage walk is shaped like
        ``"<scope_name>.<column>"`` when the walker descends into a CTE, where
        ``<scope_name>`` is the CTE alias.  dbt names the inlined ephemeral CTE
        ``__dbt__cte__<model_name>``.
        """
        if not node_name or "." not in node_name:
            return None
        alias = node_name.rsplit(".", 1)[0].lower()
        # The walker may be deeper in a chain (e.g. ``__dbt__cte__a.col`` then a
        # later descent yields ``__dbt__cte__a.col.something``); in that case
        # only the leftmost dotted segment is the CTE alias.
        head = alias.split(".")[0]
        return self._ephemeral_cte_to_node.get(head)

    def _walk_with_ephemeral_attribution(self, root_node):
        """Walk a sqlglot lineage Node tree, yielding either real-table refs or
        ephemeral attributions.

        Yields tuples:
          - ``("table", node)``: an existing leaf table reference.
          - ``("ephemeral", {"dbt_node": eph_id, "column": col})``: a CTE node
            whose alias matches ``__dbt__cte__<ephemeral>``.  The walker does
            **not** descend into this subtree — the ephemeral itself is
            processed as a top-level model and carries its own lineage.

        The caller decides how to record each yielded item.
        """
        if root_node is None:
            return

        def visit(node):
            if node is None:
                return
            ephemeral_id = self._match_ephemeral_cte(getattr(node, "name", None))
            if ephemeral_id is not None:
                col = node.name.rsplit(".", 1)[-1]
                yield (
                    "ephemeral",
                    {"dbt_node": ephemeral_id, "column": col},
                )
                return  # prune deeper traversal
            source = getattr(node, "source", None)
            if source is not None and getattr(source, "key", None) == "table":
                yield ("table", node)
            for d in getattr(node, "downstream", []) or []:
                yield from visit(d)

        yield from visit(root_node)
    
    def _stub_ephemeral_ctes(self, sql):
        """Replace the body of each ``__dbt__cte__<eph>`` CTE with a column-only
        stub so that sqlglot lineage stops at the ephemeral boundary instead of
        tracing through the inlined SQL into transitive ancestors.
        """
        parsed = maybe_parse(sql, dialect=self.dialect)
        changed = False
        for cte in parsed.find_all(exp.CTE):
            alias = cte.alias
            if not alias:
                continue
            node_id = self._ephemeral_cte_to_node.get(alias.lower())
            if node_id is None:
                continue
            columns = self._resolve_ephemeral_columns(node_id)
            if not columns:
                continue
            projections = [
                exp.alias_(exp.Null(), col_name, quoted=False)
                for col_name in columns
            ]
            stub = exp.Select(expressions=projections)
            cte.set("this", stub)
            changed = True
        return parsed.sql(dialect=self.dialect) if changed else sql

    def _sanitize_sql_for_parsing(self, sql):
        # Placeholder for any SQL sanitization needed before parsing
        if self.dialect != "oracle":
            return sql

        sql = re.sub(r"(?i)\bLISTAGG\s*\(\s*DISTINCT\s+", "LISTAGG(", sql)

        sql = re.sub(
            r"(?is)\bON\s+OVERFLOW\s+(?:TRUNCATE|ERROR)\b(?:\s+'[^']*')?(?:\s+(?:WITH|WITHOUT)\s+COUNT)?",
            "",
            sql,
        )

        return sql

    @staticmethod
    def _schema_has_quoted_keys(schema):
        """Return True if any column key in *schema* is double-quote-wrapped."""
        for db in schema.values():
            for sch in db.values():
                for tbl in sch.values():
                    if any(k.startswith('"') for k in tbl):
                        return True
        return False

    def _extract_lineage_for_model(self, model_sql, schema, model_node, resource_type, selected_columns=[]):
        lineage_map = {}
        model_sql_for_parse = self._sanitize_sql_for_parsing(model_sql)
        parsed_model_sql = maybe_parse(model_sql_for_parse, dialect=self.dialect)
        # sqlglot does not unfold * to schema when the schema has quotes, or upper (for BigQuery)
        # Skip remove_quotes when the schema contains quoted column keys, as
        # stripping AST quotes prevents SQLGlot from matching case-sensitive columns.
        if self.dialect == "postgres" and not self._schema_has_quoted_keys(schema):
            parsed_model_sql = parsing_utils.remove_quotes(parsed_model_sql)
        if self.dialect == "bigquery":
            parsed_model_sql = parsing_utils.remove_upper(parsed_model_sql)
        qualified_expr, scope = prepare_scope(parsed_model_sql, schema=schema, dialect=self.dialect)

        for column_name in selected_columns:
            normalized_column = _normalize_column_name(column_name)
            if resource_type == "snapshot" and column_name in ["dbt_valid_from", "dbt_valid_to", "dbt_updated_at", "dbt_scd_id"]:
                self.logger.debug(f"Skipping special snapshot column {column_name}")
                lineage_map[column_name] = []
                continue
        
            try:
                lineage_node = lineage(normalized_column, qualified_expr, schema=schema, dialect=self.dialect, scope=scope)
                lineage_map[column_name] = lineage_node
            
            except SqlglotError:
                
                # Fallback: try to parse as expression and extract columns
                try:
                    # parsed_sql = sqlglot.parse_one(model_sql, dialect=self.dialect)
                    parsed_sql = parsed_model_sql
                    alias_expr_map = {}

                    select_exprs = get_select_expressions(parsed_sql)

                    alias_expr_map = {}
                    for expr in select_exprs:
                        alias = expr.alias_or_name
                        if alias:
                            alias_expr_map[alias.lower()] = expr
                    expr = alias_expr_map.get(normalized_column)
                    self.logger.debug(f"Available aliases in query: {list(alias_expr_map.keys())}")
                    if expr:
                        upstream_columns = extract_column_refs(expr)
                        lineage_nodes = []
                        for col in upstream_columns:
                            try:
                                lineage_nodes.append(
                                    lineage(
                                        col.name,
                                        qualified_expr,
                                        schema=schema,
                                        dialect=self.dialect,
                                        scope=scope
                                    )
                                )
                            except SqlglotError as e_inner:
                                self.logger.error(
                                    f"Could not resolve lineage for '{col.name}' in alias '{column_name}': {e_inner}"
                                )
                        lineage_map[column_name] = lineage_nodes
                    else:
                        self.logger.debug(f"No expression found for alias '{model_node}' '{column_name}'")
                        lineage_map[column_name] = []


                except Exception as e2:
                    self.logger.error(f"Fallback error on {column_name}: {e2}")
                    lineage_map[column_name] = []
            except Exception as e:
                self.logger.error(
                    f"Unexpected error processing model {model_node}, column {column_name}: {e}"
                )
                lineage_map[column_name] = []

        return lineage_map

    def build_lineage_map(self):
        lineage_map = {}
        total_models = len(self.selected_models)
        processed_count = 0
        error_count = 0

        for model_node, model_info in self.manifest["nodes"].items():

            if self.selected_models and model_node not in self.selected_models:
                continue

            processed_count += 1
            self.logger.debug(f"{processed_count}/{total_models} Processing model {model_node}")

            try:
                if model_info["path"].endswith(".py"):
                    self.logger.debug(
                        f"Skipping column lineage detection for Python model {model_node}"
                    )
                    continue
                if model_info["resource_type"] not in ["model", "snapshot"]:
                    self.logger.debug(
                        f"Skipping column lineage detection for {model_node} as it's not a model but a {model_info['resource_type']}"
                    )
                    continue

                if "compiled_code" not in model_info or not model_info["compiled_code"]:
                    self.logger.debug(f"Skipping {model_node} as it has no compiled SQL code")
                    continue

                parent_catalog = self._get_parent_nodes_catalog(model_info)
                columns = self._get_list_of_columns_for_a_dbt_node(model_node)
                schema = self._generate_schema_dict_from_catalog(parent_catalog)
                model_sql = self._stub_ephemeral_ctes(model_info["compiled_code"])
                resource_type = model_info.get("resource_type")
                model_lineage = self._extract_lineage_for_model(
                    model_sql=model_sql,
                    schema=schema,
                    model_node=model_node,
                    selected_columns=columns,
                    resource_type=resource_type,
                )
                if model_lineage:  # Only add if we got valid lineage results
                    lineage_map[model_node] = model_lineage
           
            except Exception as e:
                error_count += 1
                self.logger.error(f"Error processing model {model_node}: {str(e)}")
                self.logger.debug("Continuing with next model...")
                continue

        if error_count > 0:
            self.logger.info(
                f"Completed with {error_count} errors out of {processed_count} models processed"
            )
        return lineage_map
    
    def _table_key_from_sqlglot_table_node(self, node):
        catalog = (node.source.catalog or "").strip()
        db = (node.source.db or "").strip()
        table_name = (node.source.name or "").strip()

        if not table_name:
            return ""

        if not catalog:
            if not db:
                return table_name.lower()

            return f"{db}.{table_name}".lower()
        
        if not db:
            return f"{catalog}.{table_name}".lower()

        return f"{catalog}.{db}.{table_name}".lower()

    def get_dbt_node_from_sqlglot_table_node(self, node, model_node):
        if node.source.key != "table":
            raise ValueError(f"Node source is not a table, but {node.source.key}")

        if not node.source.catalog and not node.source.db:
            return None

        column_name_raw = node.name.split(".")[-1]

        if self.dialect == 'clickhouse':
            table_name = f"{node.source.db}.{node.source.name}"
        elif self.dialect == 'oracle':
            table_name = self._table_key_from_sqlglot_table_node(node)
        else:
            table_name = f"{node.source.catalog}.{node.source.db}.{node.source.name}"

        match = self._table_to_node.get(table_name.lower())
        if match:
            dbt_node = match["unique_id"]
        else:
            # Check if the table is hardcoded in raw code.
            raw_code = self.manifest["nodes"][model_node]["raw_code"].lower()

            # Try different variations of the table name
            table_variations = [
                table_name,  # full: .public.customers_hardcoded or test_db.public.customers_hardcoded
                table_name.lstrip("."),  # without leading dot: public.customers_hardcoded
                f"{node.source.db}.{node.source.name}".lower(),  # db.table: public.customers_hardcoded
                node.source.name.lower(),  # just table name: customers_hardcoded
            ]

            # Remove duplicates while preserving order
            table_variations = list(dict.fromkeys(table_variations))

            found_hardcoded = False
            for variation in table_variations:
                if variation and variation in raw_code:
                    dbt_node = f"_HARDCODED_REF___{table_name.lower()}"
                    found_hardcoded = True
                    break

            if not found_hardcoded:
                self.logger.warning(f"Table {table_name} not found in node mapping")
                dbt_node = f"_NOT_FOUND___{table_name.lower()}"
            # raise ValueError(f"Table {table_name} not found in node mapping")

        # Preserve original case for quoted columns in the parent node
        column_name = self._resolve_column_name(column_name_raw, dbt_node)

        return {"column": column_name, "dbt_node": dbt_node}

    def get_columns_lineage_from_sqlglot_lineage_map(self, lineage_map, picked_columns=[]):
        columns_lineage = {}
        # Initialize all selected models before accessing them
        for model in self.selected_models:
            columns_lineage[model] = {}

        for model_node, columns in lineage_map.items():
            model_node_lower = model_node
            if not self.manifest.get("parent_map", {}).get(model_node_lower) and \
                not self.manifest.get("child_map", {}).get(model_node_lower):
                    continue

            if model_node_lower not in columns_lineage:
                # Add any model node from lineage_map that might not be in selected_models
                columns_lineage[model_node_lower] = {}

            for column, node in columns.items():
                column = self._resolve_column_name(column, model_node_lower)
                if picked_columns and column not in picked_columns:
                    continue

                columns_lineage[model_node_lower][column] = []

                # Handle the case where node is a list (empty lineage result)
                if isinstance(node, list):
                    continue

                # Process nodes with a walk method
                for n in node.walk():
                    if n.source.key == "table":
                        parent_columns = self.get_dbt_node_from_sqlglot_table_node(n, model_node)
                        if not parent_columns:
                            continue
                        parent_columns["lineage_type"] = node.lineage_type
                        if (
                            parent_columns["dbt_node"] != model_node
                            and parent_columns not in columns_lineage[model_node_lower][column]
                        ):
                            columns_lineage[model_node_lower][column].append(parent_columns)

                if not columns_lineage[model_node_lower][column]:
                    self.logger.debug(f"No lineage found for {model_node} - {column}")
        return columns_lineage


    @staticmethod
    def _ci_get(mapping, key):
        """Case-insensitive dict key lookup. Return the value if *key* (lowered)
        matches any key in *mapping*, otherwise ``None``."""
        if key in mapping:
            return mapping[key]
        key_lower = key.lower()
        for k, v in mapping.items():
            if k.lower() == key_lower:
                return v
        return None

    @staticmethod
    def _ci_key(mapping, key):
        """Return the actual key in *mapping* that matches *key* case-insensitively,
        or *key* itself if not found."""
        if key in mapping:
            return key
        key_lower = key.lower()
        for k in mapping:
            if k.lower() == key_lower:
                return k
        return key

    @staticmethod
    def find_all_related(lineage_map, model_node, column, visited=None):
        """Find all related columns in lineage_map that connect to model_node.column."""
        model_node = model_node.lower()
        if visited is None:
            visited = set()

        related = {}

        # Check if the model_node exists in lineage_map
        model_entry = DbtColumnLineageExtractor._ci_get(lineage_map, model_node)
        if model_entry is None:
            return related

        # Case-insensitive column lookup to support quoted (mixed-case) keys
        column_key = DbtColumnLineageExtractor._ci_key(model_entry, column)
        if column_key not in model_entry:
            return related

        # Process each related node
        for related_node in model_entry[column_key]:
            related_model = related_node["dbt_node"].lower()
            related_column = related_node["column"]

            if (related_model, related_column.lower()) not in visited:
                visited.add((related_model, related_column.lower()))

                if related_model not in related:
                    related[related_model] = []

                if related_column not in related[related_model]:
                    related[related_model].append(related_column)

                # Recursively find further related columns
                further_related = DbtColumnLineageExtractor.find_all_related(
                    lineage_map, related_model, related_column, visited
                )

                # Merge the results
                for further_model, further_columns in further_related.items():
                    if further_model not in related:
                        related[further_model] = []

                    for col in further_columns:
                        if col not in related[further_model]:
                            related[further_model].append(col)

        return related

    @staticmethod
    def find_all_related_with_structure(lineage_map, model_node, column, visited=None):
        """Find all related columns with hierarchical structure."""
        model_node = model_node.lower()
        if visited is None:
            visited = set()

        # Initialize the related structure for the current node and column.
        related_structure = {}

        # Case-insensitive lookup for model and column
        model_entry = DbtColumnLineageExtractor._ci_get(lineage_map, model_node)
        if model_entry is None:
            return related_structure

        column_key = DbtColumnLineageExtractor._ci_key(model_entry, column)
        if column_key not in model_entry:
            return related_structure

        # Process each related node
        for related_node in model_entry[column_key]:
            related_model = related_node["dbt_node"].lower()
            related_column = related_node["column"]

            if (related_model, related_column.lower()) not in visited:
                visited.add((related_model, related_column.lower()))

                # Recursively get the structure for each related node
                subsequent_structure = DbtColumnLineageExtractor.find_all_related_with_structure(
                    lineage_map, related_model, related_column, visited
                )

                # Use a structure to show relationships distinctly
                if related_model not in related_structure:
                    related_structure[related_model] = {}

                # Add information about the column lineage
                related_structure[related_model][related_column] = {"+": subsequent_structure}

        return related_structure

    def extract_project_lineage(self):
        """
        Stream lineage extraction to minimize peak memory:
        - For each model, parse/qualify once.
        - For each column, compute lineage, immediately materialize parents and update children.
        - Do not accumulate sqlglot Node graphs across the entire project.
        """
        self.logger.info("Streaming lineage extraction (memory-optimized)...")

        parents: dict = {}
        children: dict = {}

        # Prepare model list respecting selection if provided
        all_models = (
            [m for m in self.selected_models]
            if self.selected_models
            else [
                node_id
                for node_id, node in self.manifest.get("nodes", {}).items()
                if node.get("resource_type") in ["model", "snapshot"]
            ]
        )

        total_models = len(all_models)
        processed_count = 0
        error_count = 0
        errors = []

        for model_node in all_models:
            model_info = self.manifest["nodes"].get(model_node)
            if not model_info:
                continue

            processed_count += 1
            self.logger.debug(f"{processed_count}/{total_models} Processing model {model_node}")

            try:
                if model_info.get("path", "").endswith(".py"):
                    self.logger.debug(
                        f"Skipping column lineage detection for Python model {model_node}"
                    )
                    continue
                if model_info.get("resource_type") not in ["model", "snapshot"]:
                    continue
                if not model_info.get("compiled_code"):
                    self.logger.debug(f"Skipping {model_node} as it has no compiled SQL code")
                    continue

                parent_catalog = self._get_parent_nodes_catalog(model_info)
                schema = self._generate_schema_dict_from_catalog(parent_catalog)
                model_sql = self._stub_ephemeral_ctes(model_info["compiled_code"])

                # Parse and qualify once per model
                model_sql_for_parse = self._sanitize_sql_for_parsing(model_sql)
                parsed_model_sql = maybe_parse(model_sql_for_parse, dialect=self.dialect)
                if self.dialect == "postgres" and not self._schema_has_quoted_keys(schema):
                    parsed_model_sql = parsing_utils.remove_quotes(parsed_model_sql)
                if self.dialect == "bigquery":
                    parsed_model_sql = parsing_utils.remove_upper(parsed_model_sql)
                qualified_expr, scope = prepare_scope(parsed_model_sql, schema=schema, dialect=self.dialect)

                # Initialize parents entry for this model
                model_parents: dict = {}

                columns = self._get_list_of_columns_for_a_dbt_node(model_node)

                quoted_cols = self._get_quoted_columns(model_node)

                for column_name in columns:
                    # Preserve original casing for quoted columns
                    col_lower = column_name.lower()
                    column_key = quoted_cols[col_lower] if col_lower in quoted_cols else col_lower
                    # Snapshot special columns
                    if model_info.get("resource_type") == "snapshot" and column_name in [
                        "dbt_valid_from",
                        "dbt_valid_to",
                        "dbt_updated_at",
                        "dbt_scd_id",
                    ]:
                        self.logger.debug(f"Skipping special snapshot column {column_name}")
                        model_parents[column_key] = []
                        continue

                    model_parents[column_key] = []

                    def append_parent(parent_columns, lineage_type, _model_parents=model_parents, _column_key=column_key):
                        parent_model = parent_columns["dbt_node"]
                        parent_col = parent_columns["column"]
                        if parent_model == model_node:
                            return
                        if parent_columns not in _model_parents[_column_key]:
                            parent_columns["lineage_type"] = lineage_type
                            _model_parents[_column_key].append(parent_columns)
                        # Update children incrementally
                        children.setdefault(parent_model, {}).setdefault(parent_col, []).append(
                            {"column": _column_key, "dbt_node": model_node}
                        )

                    try:
                        normalized_column = _normalize_column_name(column_name)
                        lineage_node = lineage(
                            normalized_column,
                            qualified_expr,
                            schema=schema,
                            dialect=self.dialect,
                            scope=scope,
                        )

                        for kind, payload in self._walk_with_ephemeral_attribution(lineage_node):
                            if kind == "table":
                                parent_columns = self.get_dbt_node_from_sqlglot_table_node(payload, model_node)
                                if parent_columns:
                                    append_parent(parent_columns, lineage_node.lineage_type)
                            elif kind == "ephemeral":
                                # CTE-boundary attribution: stop at the ephemeral
                                # so it shows up as a direct parent of the
                                # consuming column.
                                append_parent(dict(payload), lineage_node.lineage_type)

                    except SqlglotError:
                        # Fallback: try to parse as expression and extract columns
                        try:
                            parsed_sql = parsed_model_sql
                            alias_expr_map = {}
                            select_exprs = get_select_expressions(parsed_sql)
                            for expr in select_exprs:
                                alias = expr.alias_or_name
                                if alias:
                                    alias_expr_map[alias.lower()] = expr
                            expr = alias_expr_map.get(_normalize_column_name(column_name).lower())
                            self.logger.debug(f"Available aliases in query: {list(alias_expr_map.keys())}")
                            if expr:
                                upstream_columns = extract_column_refs(expr)
                                for col in upstream_columns:
                                    try:
                                        sub_node = lineage(
                                            col.name,
                                            qualified_expr,
                                            schema=schema,
                                            dialect=self.dialect,
                                            scope=scope,
                                        )
                                        for kind, payload in self._walk_with_ephemeral_attribution(sub_node):
                                            if kind == "table":
                                                parent_columns = self.get_dbt_node_from_sqlglot_table_node(payload, model_node)
                                                if parent_columns:
                                                    append_parent(parent_columns, sub_node.lineage_type)
                                            elif kind == "ephemeral":
                                                append_parent(dict(payload), sub_node.lineage_type)
                                    except SqlglotError as e_inner:
                                        self.logger.error(
                                            f"Could not resolve lineage for '{col.name}' in alias '{column_name}': {e_inner}"
                                        )
                            else:
                                self.logger.debug(f"No expression found for alias '{model_node}' '{column_name}'")
                        except Exception as e2:
                            self.logger.error(f"Fallback error on {column_name}: {e2}")
                    except Exception as e:
                        self.logger.error(
                            f"Unexpected error processing model {model_node}, column {column_name}: {e}"
                        )

                # Extract structural lineage (WHERE/HAVING/JOIN ON columns)
                try:
                    structural = extract_structural_lineage(scope, self.dialect)
                    for edge_type, nodes_list in structural.items():
                        if not nodes_list:
                            continue
                        key = f"__colibri_{edge_type}__"
                        entries = []
                        for struct_node in nodes_list:
                            if struct_node is None:
                                continue
                            for kind, payload in self._walk_with_ephemeral_attribution(struct_node):
                                parent_columns = None
                                if kind == "table":
                                    parent_columns = self.get_dbt_node_from_sqlglot_table_node(payload, model_node)
                                elif kind == "ephemeral":
                                    parent_columns = dict(payload)
                                if parent_columns and parent_columns["dbt_node"] != model_node:
                                    parent_columns["lineage_type"] = edge_type
                                    if parent_columns not in entries:
                                        entries.append(parent_columns)
                                        # Update children map
                                        parent_model = parent_columns["dbt_node"]
                                        parent_col = parent_columns["column"]
                                        children.setdefault(parent_model, {}).setdefault(parent_col, []).append(
                                            {"column": key, "dbt_node": model_node}
                                        )
                        if entries:
                            model_parents[key] = entries
                except Exception as e:
                    self.logger.debug(f"Could not extract structural columns for {model_node}: {e}")

                if model_parents:
                    parents[model_node] = model_parents

                # Aggressively release large per-model structures
                del qualified_expr, scope, parsed_model_sql, model_parents
                if processed_count % 50 == 0:
                    gc.collect()

            except Exception as e:
                error_count += 1
                self.logger.error(f"Error processing model {model_node}: {str(e)}")
                errors.append({
                    "node_id": model_node,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                })
                self.logger.debug("Continuing with next model...")
                continue

        if error_count > 0:
            self.logger.info(
                f"Completed with {error_count} errors out of {processed_count} models processed"
            )

        return {"lineage": {"parents": parents, "children": children}, "errors": errors}

class DBTNodeCatalog:
    def __init__(self, node_data):
        # Handle cases where metadata might be missing
        if "metadata" not in node_data:
            raise ValueError(f"Node data missing metadata field: {node_data}")

        self.database = node_data["metadata"]["database"]
        self.unique_id = node_data["unique_id"].lower()
        self.schema = node_data["metadata"]["schema"]
        self.name = node_data["metadata"]["name"]
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return self.unique_id

    def get_column_types(self):
        return {col_name: col_info["type"] for col_name, col_info in self.columns.items()}


class DBTNodeManifest:
    def __init__(self, node_data):
        self.database = node_data["database"]
        self.schema = node_data["schema"]
        self.relation_name = parsing_utils.normalize_table_relation_name(node_data["relation_name"])
        # self.dialect = dialect
        # Check alias first
        if node_data.get("alias"):
            self.name = node_data.get("alias")
        else:
            self.name = node_data.get("identifier", node_data["name"])
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return self.relation_name

# TODO: add metadata columns to external tables