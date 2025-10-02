
from sqlglot.lineage import maybe_parse, SqlglotError, exp
import logging
from ..utils import json_utils, parsing_utils
from .lineage import lineage, prepare_scope
import re
from importlib.metadata import version, PackageNotFoundError
import gc


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
    def __init__(self, manifest_path, catalog_path, selected_models=[]):
        # Set up logging
        self.logger = logging.getLogger("colibri")

        # Read manifest and catalog files
        self.manifest = json_utils.read_json(manifest_path)
        self.catalog = json_utils.read_json(catalog_path)      
        self.schema_dict = self._generate_schema_dict_from_catalog()
        self.dialect = self._detect_adapter_type()
        # self.node_mapping = self._get_dict_mapping_full_table_name_to_dbt_node()
        self.nodes_with_columns = self.build_nodes_with_columns()
        # Store references to parent and child maps for easy access
        self.parent_map = self.manifest.get("parent_map", {})
        self.child_map = self.manifest.get("child_map", {})
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
        """
        all_models = [
            node_id for node_id, node in self.manifest.get("nodes", {}).items()
            if node.get("resource_type") == "model"
        ]

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

        if non_materialized:
            msg = f"{len(non_materialized)}/{len(all_models)} models are not materialized (missing from catalog)."
            self.logger.error(msg)

    def _get_colibri_version(self):
        try:
            return version("dbt-colibri")
        except PackageNotFoundError:
            return "unknown"
        
    def _detect_adapter_type(self):
        """
        Detect the adapter type from the manifest metadata.
        
        Returns:
            str: The detected adapter type
            
        Raises:
            ValueError: If adapter_type is not found or not supported
        """
        SUPPORTED_ADAPTERS = {'snowflake', 'bigquery', 'redshift', 'duckdb', 'postgres', 'databricks'}
        
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

            schema_dict[db_name][schema_name][table_name].update(dbt_node.get_column_types())

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
        else:
            self.logger.warning(f"Node {node} not found in catalog, maybe it's not materialized")
            return []
        return [col.lower() for col in list(columns.keys())]

    def _get_parent_nodes_catalog(self, model_info):
        parent_nodes = model_info["depends_on"]["nodes"]
        parent_catalog = {"nodes": {}, "sources": {}}
        for parent in parent_nodes:
            if parent in self.catalog["nodes"]:
                parent_catalog["nodes"][parent] = self.catalog["nodes"][parent]
            elif parent in self.catalog["sources"]:
                parent_catalog["sources"][parent] = self.catalog["sources"][parent]
            else:
                self.logger.warning(f"Parent model {parent} not found in catalog")
        return parent_catalog

    def _extract_lineage_for_model(self, model_sql, schema, model_node, resource_type, selected_columns=[]):
        lineage_map = {}
        parsed_model_sql = maybe_parse(model_sql, dialect=self.dialect)
        # sqlglot does not unfold * to schema when the schema has quotes, or upper (for BigQuery)
        if self.dialect == "postgres":
            parsed_model_sql = parsing_utils.remove_quotes(parsed_model_sql)
        if self.dialect == "bigquery":
            parsed_model_sql = parsing_utils.remove_upper(parsed_model_sql)
        qualified_expr, scope = prepare_scope(parsed_model_sql, schema=schema, dialect=self.dialect)
        def normalize_column_name(name: str) -> str:
            name = name.strip('"').strip("'")
            # Remove type casts like '::date' or '::timestamp'
            name = re.sub(r"::\s*\w+$", "", name)
            if name.startswith("$"):
                name = name[1:]
            return name

        for column_name in selected_columns:
            normalized_column = normalize_column_name(column_name)
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
                model_sql = model_info["compiled_code"]
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

    def get_dbt_node_from_sqlglot_table_node(self, node, model_node):
        if node.source.key != "table":
            raise ValueError(f"Node source is not a table, but {node.source.key}")
        column_name = node.name.split(".")[-1].lower()
        table_name = f"{node.source.catalog}.{node.source.db}.{node.source.name}"
        
        for key, data in self.nodes_with_columns.items():
            if key.lower() == table_name.lower():
                dbt_node = data["unique_id"]
                break
        
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
                column = column.lower()
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
                        parent_columns["lineage_type"] = node.lineage_type
                        if (
                            parent_columns["dbt_node"] != model_node
                            and parent_columns not in columns_lineage[model_node_lower][column]
                        ):
                            columns_lineage[model_node_lower][column].append(parent_columns)

                if not columns_lineage[model_node_lower][column]:
                    self.logger.debug(f"No lineage found for {model_node} - {column}")
        return columns_lineage

    def get_lineage_to_direct_children_from_lineage_to_direct_parents(
        self, lineage_to_direct_parents
    ):
        children_lineage = {}

        for child_model, columns in lineage_to_direct_parents.items():
            child_model = child_model
            for child_column, parents in columns.items():
                child_column = child_column.lower()
                for parent in parents:
                    parent_model = parent["dbt_node"]
                    parent_column = parent["column"].lower()

                    if parent_model not in children_lineage:
                        children_lineage[parent_model] = {}

                    if parent_column not in children_lineage[parent_model]:
                        children_lineage[parent_model][parent_column] = []

                    children_lineage[parent_model][parent_column].append(
                        {"column": child_column, "dbt_node": child_model}
                    )
        return children_lineage

    @staticmethod
    def find_all_related(lineage_map, model_node, column, visited=None):
        """Find all related columns in lineage_map that connect to model_node.column."""
        column = column.lower()
        model_node = model_node.lower()
        if visited is None:
            visited = set()

        related = {}

        # Check if the model_node exists in lineage_map
        if model_node not in lineage_map:
            return related

        # Check if the column exists in the model_node
        if column not in lineage_map[model_node]:
            return related

        # Process each related node
        for related_node in lineage_map[model_node][column]:
            related_model = related_node["dbt_node"].lower()
            related_column = related_node["column"].lower()

            if (related_model, related_column) not in visited:
                visited.add((related_model, related_column))

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
        column = column.lower()
        if visited is None:
            visited = set()

        # Initialize the related structure for the current node and column.
        related_structure = {}

        # Return empty if model or column doesn't exist
        if model_node not in lineage_map:
            return related_structure

        if column not in lineage_map[model_node]:
            return related_structure

        # Process each related node
        for related_node in lineage_map[model_node][column]:
            related_model = related_node["dbt_node"].lower()
            related_column = related_node["column"].lower()

            if (related_model, related_column) not in visited:
                visited.add((related_model, related_column))

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
                model_sql = model_info["compiled_code"]

                # Parse and qualify once per model
                parsed_model_sql = maybe_parse(model_sql, dialect=self.dialect)
                if self.dialect == "postgres":
                    parsed_model_sql = parsing_utils.remove_quotes(parsed_model_sql)
                if self.dialect == "bigquery":
                    parsed_model_sql = parsing_utils.remove_upper(parsed_model_sql)
                qualified_expr, scope = prepare_scope(parsed_model_sql, schema=schema, dialect=self.dialect)

                def normalize_column_name(name: str) -> str:
                    name = name.strip('"').strip("'")
                    name = re.sub(r"::\s*\w+$", "", name)
                    if name.startswith("$"):
                        name = name[1:]
                        
                    return name

                # Initialize parents entry for this model
                model_parents: dict = {}

                columns = self._get_list_of_columns_for_a_dbt_node(model_node)

                for column_name in columns:
                    column_key = column_name.lower()
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
                        parent_col = parent_columns["column"].lower()
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
                        normalized_column = normalize_column_name(column_name)
                        lineage_node = lineage(
                            normalized_column,
                            qualified_expr,
                            schema=schema,
                            dialect=self.dialect,
                            scope=scope,
                        )

                        for n in lineage_node.walk():
                            if n.source.key == "table":
                                parent_columns = self.get_dbt_node_from_sqlglot_table_node(n, model_node)
                                append_parent(parent_columns, lineage_node.lineage_type)

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
                            expr = alias_expr_map.get(normalize_column_name(column_name).lower())
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
                                        for n in sub_node.walk():
                                            if n.source.key == "table":
                                                parent_columns = self.get_dbt_node_from_sqlglot_table_node(n, model_node)
                                                append_parent(parent_columns, sub_node.lineage_type)
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

                if model_parents:
                    parents[model_node] = model_parents

                # Aggressively release large per-model structures
                del qualified_expr, scope, parsed_model_sql, model_parents
                if processed_count % 50 == 0:
                    gc.collect()

            except Exception as e:
                error_count += 1
                self.logger.error(f"Error processing model {model_node}: {str(e)}")
                self.logger.debug("Continuing with next model...")
                continue

        if error_count > 0:
            self.logger.info(
                f"Completed with {error_count} errors out of {processed_count} models processed"
            )

        return {"lineage": {"parents": parents, "children": children}}

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