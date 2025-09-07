import warnings
from sqlglot.lineage import maybe_parse, SqlglotError, exp
import logging
from ..utils import json_utils
from .lineage import lineage, prepare_scope
import re
import sqlglot


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
        self.logger = logging.getLogger("dbt_column_lineage")

        # Read manifest and catalog files
        self.manifest = json_utils.read_json(manifest_path)
        self.catalog = json_utils.read_json(catalog_path)
        self.schema_dict = self._generate_schema_dict_from_catalog()
        self.node_mapping = self._get_dict_mapping_full_table_name_to_dbt_node()
        self.dialect = self._detect_adapter_type()
        # Store references to parent and child maps for easy access
        self.parent_map = self.manifest.get("parent_map", {})
        self.child_map = self.manifest.get("child_map", {})
        # Process selected models
        self.selected_models = []

        if not selected_models:
            # If no models specified, use all models in the manifest
            self.selected_models = [
                node
                for node in self.manifest["nodes"].keys()
                if self.manifest["nodes"][node].get("resource_type") in ("model", "snapshot")
            ]
        else:
            # Process selectors to get models
            self.selected_models = self._parse_selectors(selected_models)

    def _detect_adapter_type(self):
        """
        Detect the adapter type from the manifest metadata.
        
        Returns:
            str: The detected adapter type
            
        Raises:
            ValueError: If adapter_type is not found or not supported
        """
        SUPPORTED_ADAPTERS = {'snowflake', 'bigquery', 'redshift', 'duckdb', 'postgres'}
        
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

    def _parse_selectors(self, selectors):
        """
        Parse dbt-style selectors to expand the list of selected models.

        This implements a subset of dbt's node selection syntax, allowing you to select models
        using the same patterns you're familiar with from dbt commands.

        Supported selector types:
        - Simple names: "model_name" or "model.package.model_name"
        - Source references: "source.schema.name"

        Graph operators:
        - Ancestors: "+model_name" (include model and all its upstream/parent models)
        - Descendants: "model_name+" (include model and all its downstream/child models)
        - Both: "+model_name+" (include model, all its ancestors and all its descendants)

        Set operators:
        - Union (OR): "model1 model2" (models matching either selector)
        - Intersection (AND): "model1,model2" (models matching both selectors)

        Resource selectors:
        - Tags: "tag:my_tag" (models with specific tag)
        - Path: "path:models/finance" (models in specific path)
        - Package: "package:my_package" (models in specific package)

        These selectors can be combined in complex ways, such as:
        - "tag:daily,+orders" (models tagged as 'daily' AND are also ancestors of 'orders')
        - "customers+ tag:finance" (descendants of 'customers' OR models with 'finance' tag)

        Returns:
            list: Expanded list of model names after applying selector logic
        """
        if not selectors:
            return []

        # If selectors is already a list of model names without any special syntax, return as is
        if all(
            selector in self.manifest["nodes"] or selector in self.manifest.get("sources", {})
            for selector in selectors
        ):
            return selectors

        # Handle list of selector expressions
        expanded_models = set()

        for selector_expr in selectors:
            # Check for intersection (comma-separated parts)
            if "," in selector_expr:
                parts = selector_expr.split(",")
                intersection_sets = []

                for part in parts:
                    # Recursively parse each part
                    part_models = self._parse_selectors([part])
                    intersection_sets.append(set(part_models))

                # Intersect all parts
                if intersection_sets:
                    result = intersection_sets[0]
                    for s in intersection_sets[1:]:
                        result = result.intersection(s)
                    expanded_models.update(result)

            # Handle union (space-separated parts)
            elif " " in selector_expr:
                parts = selector_expr.split()
                for part in parts:
                    # Recursively parse each part
                    part_models = self._parse_selectors([part])
                    expanded_models.update(part_models)

            # Handle tag selector: tag:my_tag
            elif selector_expr.startswith("tag:"):
                tag = selector_expr[4:]
                matching_models = self._get_models_by_tag(tag)
                expanded_models.update(matching_models)

            # Handle path selector: path:models/finance
            elif selector_expr.startswith("path:"):
                path = selector_expr[5:]
                matching_models = self._get_models_by_path(path)
                expanded_models.update(matching_models)

            # Handle package selector: package:my_package
            elif selector_expr.startswith("package:"):
                package = selector_expr[8:]
                matching_models = self._get_models_by_package(package)
                expanded_models.update(matching_models)

            # Handle both ancestors and descendants: +model_name+
            elif selector_expr.startswith("+") and selector_expr.endswith("+"):
                model_name = selector_expr[1:-1]
                if model_name in self.manifest["nodes"] or model_name in self.manifest.get(
                    "sources", {}
                ):
                    expanded_models.add(model_name)
                    expanded_models.update(self._get_all_ancestors(model_name))
                    expanded_models.update(self._get_all_descendants(model_name))
                else:
                    # Try to resolve without prefix/suffix
                    matches = self._resolve_node_by_name(model_name)
                    for match in matches:
                        expanded_models.add(match)
                        expanded_models.update(self._get_all_ancestors(match))
                        expanded_models.update(self._get_all_descendants(match))

            # Handle ancestors (upstream/parents): +model_name
            elif selector_expr.startswith("+"):
                model_name = selector_expr[1:]
                if model_name in self.manifest["nodes"] or model_name in self.manifest.get(
                    "sources", {}
                ):
                    expanded_models.add(model_name)
                    expanded_models.update(self._get_all_ancestors(model_name))
                else:
                    # Try to resolve without prefix
                    matches = self._resolve_node_by_name(model_name)
                    for match in matches:
                        expanded_models.add(match)
                        expanded_models.update(self._get_all_ancestors(match))

            # Handle descendants (downstream/children): model_name+
            elif selector_expr.endswith("+"):
                model_name = selector_expr[:-1]
                if model_name in self.manifest["nodes"] or model_name in self.manifest.get(
                    "sources", {}
                ):
                    expanded_models.add(model_name)
                    expanded_models.update(self._get_all_descendants(model_name))
                else:
                    # Try to resolve without prefix
                    matches = self._resolve_node_by_name(model_name)
                    for match in matches:
                        expanded_models.add(match)
                        expanded_models.update(self._get_all_descendants(match))

            # Handle direct node reference
            elif selector_expr in self.manifest["nodes"] or selector_expr in self.manifest.get(
                "sources", {}
            ):
                expanded_models.add(selector_expr)

            # Handle source reference
            elif selector_expr.startswith("source."):
                if selector_expr in self.manifest.get("sources", {}):
                    expanded_models.add(selector_expr)

            # Handle node name without resource type prefix
            else:
                # Try to find the node by name
                matching_nodes = self._resolve_node_by_name(selector_expr)
                expanded_models.update(matching_nodes)

            # exclude sources after expansion
            expanded_models = {x for x in expanded_models if not x.startswith("source.")}

        return list(expanded_models)

    def _resolve_node_by_name(self, node_name):
        """Find nodes matching a name without full node path prefixes"""
        # First look for models
        matching_nodes = [
            node_id
            for node_id in self.manifest["nodes"]
            if self.manifest["nodes"][node_id]["resource_type"] == "model"
            and self.manifest["nodes"][node_id]["name"] == node_name
        ]

        # Then look for sources
        if "sources" in self.manifest:
            for source_id, source_data in self.manifest["sources"].items():
                if source_data.get("name") == node_name:
                    matching_nodes.append(source_id)

        return matching_nodes

    def _get_models_by_tag(self, tag):
        """Get all models with a specific tag"""
        matching_models = []

        # Check nodes (models, etc.)
        for node_name, node_info in self.manifest["nodes"].items():
            if node_info.get("resource_type") == "model" and tag in node_info.get("tags", []):
                matching_models.append(node_name)

        # Also check sources
        if "sources" in self.manifest:
            for source_name, source_info in self.manifest["sources"].items():
                if tag in source_info.get("tags", []):
                    matching_models.append(source_name)

        return matching_models

    def _get_models_by_path(self, path):
        """Get all models in a specific path"""
        matching_models = []

        # Check nodes (models, etc.)
        for node_name, node_info in self.manifest["nodes"].items():
            if node_info.get("resource_type") == "model" and path in node_info.get("path", ""):
                matching_models.append(node_name)

        # Also check sources
        if "sources" in self.manifest:
            for source_name, source_info in self.manifest["sources"].items():
                if path in source_info.get("path", ""):
                    matching_models.append(source_name)

        return matching_models

    def _get_models_by_package(self, package):
        """Get all models in a specific package"""
        matching_models = []

        # Check nodes (models, etc.)
        for node_name, node_info in self.manifest["nodes"].items():
            if node_info.get("resource_type") == "model" and package == node_info.get(
                "package_name", ""
            ):
                matching_models.append(node_name)

        # Also check sources
        if "sources" in self.manifest:
            for source_name, source_info in self.manifest["sources"].items():
                if package == source_info.get("package_name", ""):
                    matching_models.append(source_name)

        return matching_models

    def _get_all_ancestors(self, model_name):
        """Get all ancestor models (parents) using manifest's parent_map"""
        ancestors = set()
        visited = set()

        def collect_ancestors(node):
            if node in visited:
                return

            visited.add(node)

            # Get parents from parent_map
            parents = self.parent_map.get(node, [])
            for parent in parents:
                # Include both models and sources
                is_model = (
                    parent in self.manifest.get("nodes", {})
                    and self.manifest["nodes"][parent].get("resource_type") == "model"
                )
                is_source = parent in self.manifest.get("sources", {})

                if is_model or is_source:
                    ancestors.add(parent)
                    collect_ancestors(parent)

        collect_ancestors(model_name)
        return ancestors

    def _get_all_descendants(self, model_name):
        """Get all descendant models (children) using manifest's child_map"""
        descendants = set()
        visited = set()

        def collect_descendants(node):
            if node in visited:
                return

            visited.add(node)

            # Get children from child_map
            children = self.child_map.get(node, [])
            for child in children:
                # Include both models and sources
                is_model = (
                    child in self.manifest.get("nodes", {})
                    and self.manifest["nodes"][child].get("resource_type") == "model"
                )
                is_source = child in self.manifest.get("sources", {})

                if is_model or is_source:
                    descendants.add(child)
                    collect_descendants(child)

        collect_descendants(model_name)
        return descendants

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
                    warnings.warn(f"Error processing node {key}: {e}")
        for key, node in self.manifest["sources"].items():
            try:
                dbt_node = DBTNodeManifest(node)
                mapping[dbt_node.full_table_name] = key
            except Exception as e:
                warnings.warn(f"Error processing source {key}: {e}")
        return mapping

    def _get_list_of_columns_for_a_dbt_node(self, node):
        if node in self.catalog["nodes"]:
            columns = self.catalog["nodes"][node]["columns"]
        elif node in self.catalog["sources"]:
            columns = self.catalog["sources"][node]["columns"]
        else:
            warnings.warn(f"Node {node} not found in catalog, maybe it's not materialized")
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
                warnings.warn(f"Parent model {parent} not found in catalog")
        return parent_catalog

    def _extract_lineage_for_model(self, model_sql, schema, model_node, selected_columns=[]):
        lineage_map = {}
        parsed_model_sql = maybe_parse(model_sql, dialect=self.dialect)
        qualified_expr, scope = prepare_scope(parsed_model_sql, schema=schema, dialect=self.dialect)
        def normalize_column_name(name: str) -> str:
            name = name.strip('"').strip("'")
            # Remove type casts like '::date' or '::timestamp'
            name = re.sub(r"::\s*\w+$", "", name)
            if name.startswith("$"):
                name = name[1:]
            return name.lower()


        # Get columns if none provided
        if not selected_columns:
            try:
                sql = sqlglot.parse_one(model_sql, dialect=self.dialect)
                selected_columns = [
                    column.alias_or_name.lower()
                    for column in sql.select.expressions.expressions
                    if isinstance(column, (exp.Column, exp.Alias))
                ]
            except Exception as e:
                warnings.warn(f"Error parsing SQL for model {model_node}: {str(e)}")
                return {}

        for column_name in selected_columns:
            normalized_column = normalize_column_name(column_name)
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
                    self.logger.info(f"Available aliases in query: {list(alias_expr_map.keys())}")
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
                        self.logger.warning(f"No expression found for alias '{column_name}'")
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

                model_lineage = self._extract_lineage_for_model(
                    model_sql=model_sql,
                    schema=schema,
                    model_node=model_node,
                    selected_columns=columns,
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
        table_name = table_name.lower()

        if table_name in self.node_mapping:
            dbt_node = self.node_mapping[table_name].lower()
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
                warnings.warn(f"Table {table_name} not found in node mapping")
                dbt_node = f"_NOT_FOUND___{table_name.lower()}"
            # raise ValueError(f"Table {table_name} not found in node mapping")

        return {"column": column_name, "dbt_node": dbt_node}

    def get_columns_lineage_from_sqlglot_lineage_map(self, lineage_map, picked_columns=[]):
        columns_lineage = {}
        # Initialize all selected models before accessing them
        for model in self.selected_models:
            columns_lineage[model.lower()] = {}

        for model_node, columns in lineage_map.items():
            model_node_lower = model_node.lower()
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
            child_model = child_model.lower()
            for child_column, parents in columns.items():
                child_column = child_column.lower()
                for parent in parents:
                    parent_model = parent["dbt_node"].lower()
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
        self.logger.info("Building lineage map..")
        lineage_map = self.build_lineage_map()
        self.logger.info("Grabbing Parents")
        lin_parents = self.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
        self.logger.info("Grabbing Children")
        lin_children = self.get_lineage_to_direct_children_from_lineage_to_direct_parents(lin_parents)

        output = {
            
            "lineage": {
                "parents": lin_parents,
                "children": lin_children
            }
        }

        return output

class DBTNodeCatalog:
    def __init__(self, node_data):
        # Handle cases where metadata might be missing
        if "metadata" not in node_data:
            raise ValueError(f"Node data missing metadata field: {node_data}")

        self.database = node_data["metadata"]["database"]
        self.schema = node_data["metadata"]["schema"]
        self.name = node_data["metadata"]["name"]
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.name}".lower()

    def get_column_types(self):
        return {col_name: col_info["type"] for col_name, col_info in self.columns.items()}


class DBTNodeManifest:
    def __init__(self, node_data):
        self.database = node_data["database"]
        self.schema = node_data["schema"]
        # Check alias first
        if node_data.get("alias"):
            self.name = node_data.get("alias")
        else:
            self.name = node_data.get("identifier", node_data["name"])
        self.columns = node_data["columns"]

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.name}".lower()


# TODO: add metadata columns to external tables