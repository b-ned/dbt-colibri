"""
dbt Colibri MCP Server

This MCP server provides tools for querying dbt model lineage and relationships
from the colibri-manifest.json metadata file.
"""

from typing import Optional
from .lineage_index import DbtLineageIndex
from .config import MCPConfig


def create_mcp_server():
    """Create and configure the MCP server"""
    try:
        from fastmcp import FastMCP
    except ImportError:
        raise ImportError(
            "fastmcp is required for MCP server. Install with: pip install fastmcp"
        )

    mcp = FastMCP("dbt-colibri")

    # Lazy-load lineage index to avoid startup failures
    _lineage_index = None

    def get_lineage_index() -> DbtLineageIndex:
        """Lazy loader for lineage index"""
        nonlocal _lineage_index
        if _lineage_index is None:
            # Load configuration
            config = MCPConfig.load()
            if not config:
                raise RuntimeError(
                    "MCP not configured. Run 'colibri mcp config --manifest <path>' first."
                )

            # Get manifest path (download if remote)
            try:
                manifest_path = config.get_manifest_path()
            except Exception as e:
                raise RuntimeError(f"Failed to load manifest: {e}")

            # Initialize lineage index
            _lineage_index = DbtLineageIndex(manifest_path)
        
        return _lineage_index

    # Register tools
    @mcp.tool(
        description="Find all downstream models impacted by a given model. Can filter for exposure models (exp_*).",
        tags={"lineage", "downstream", "impact"}
    )
    def find_downstream_models(
        model_name: str,
        recursive: bool = False,
        exp_only: bool = False,
        limit: int = 20
    ) -> dict:
        """
        Find downstream models impacted by changes to a given model.

        Args:
            model_name: Name of the dbt model (without prefix, e.g., 'dim_customer')
            recursive: If True, find all transitive downstream models
            exp_only: If True, only return exposure models (starting with 'exp_')
            limit: Maximum number of results to return (default: 20)

        Returns:
            Dictionary with model info and list of downstream models
        """
        lineage_index = get_lineage_index()
        node_id = lineage_index.get_model_by_name(model_name)
        if not node_id:
            return {
                "error": f"Model '{model_name}' not found",
                "suggestion": "Try searching with search_models tool"
            }

        filter_type = 'exp' if exp_only else None
        downstream = lineage_index.get_downstream_models(
            node_id,
            recursive=recursive,
            filter_type=filter_type
        )

        total_count = len(downstream)
        truncated = total_count > limit

        result = {
            "model": model_name,
            "node_id": node_id,
            "total_count": total_count,
            "showing": min(total_count, limit),
            "truncated": truncated,
            "downstream_models": []
        }

        for downstream_id in sorted(downstream)[:limit]:
            node = lineage_index.get_node(downstream_id)
            if node:
                result["downstream_models"].append({
                    "id": downstream_id,
                    "name": node.get('name', ''),
                    "type": node.get('modelType', ''),
                    "schema": node.get('schema', ''),
                    "description": node.get('description', '')[:100] + "..." if len(node.get('description', '')) > 100 else node.get('description', '')
                })

        if truncated:
            result["note"] = f"Showing {limit} of {total_count} results. Increase limit parameter to see more."

        return result

    @mcp.tool(
        description="Find downstream models and columns impacted by a specific column. Supports transitive lineage.",
        tags={"lineage", "column", "impact"}
    )
    def find_column_downstream(
        model_name: str,
        column_name: str,
        recursive: bool = False,
        limit: int = 20
    ) -> dict:
        """
        Find which downstream models and columns are impacted by a specific column.

        Args:
            model_name: Name of the dbt model
            column_name: Name of the column
            recursive: If True, get all transitive downstream columns (full dependency chain)
            limit: Maximum number of results to return (default: 20)

        Returns:
            Dictionary with column info and downstream impacts
        """
        lineage_index = get_lineage_index()
        node_id = lineage_index.get_model_by_name(model_name)
        if not node_id:
            return {"error": f"Model '{model_name}' not found"}

        if recursive:
            downstream_tuples = lineage_index.get_downstream_columns_recursive(node_id, column_name)

            if not downstream_tuples:
                return {
                    "model": model_name,
                    "column": column_name,
                    "recursive": True,
                    "total_count": 0,
                    "showing": 0,
                    "truncated": False,
                    "downstream": []
                }

            total_count = len(downstream_tuples)
            truncated = total_count > limit

            result = {
                "model": model_name,
                "column": column_name,
                "recursive": True,
                "total_count": total_count,
                "showing": min(total_count, limit),
                "truncated": truncated,
                "downstream": []
            }

            for node_id_item, col_name, depth in downstream_tuples[:limit]:
                downstream_node = lineage_index.get_node(node_id_item)
                if downstream_node:
                    result["downstream"].append({
                        "model": downstream_node.get('name', ''),
                        "model_id": node_id_item,
                        "column": col_name,
                        "depth": depth,
                        "model_type": downstream_node.get('modelType', ''),
                        "description": lineage_index.get_column_description(node_id_item, col_name) or ""
                    })

            if truncated:
                result["note"] = f"Showing {limit} of {total_count} results. Increase limit parameter to see more."

            return result
        else:
            downstream = lineage_index.get_downstream_columns(node_id, column_name)

            if not downstream or column_name not in downstream:
                return {
                    "model": model_name,
                    "column": column_name,
                    "recursive": False,
                    "total_count": 0,
                    "showing": 0,
                    "truncated": False,
                    "downstream": []
                }

            total_count = len(downstream[column_name])
            truncated = total_count > limit

            result = {
                "model": model_name,
                "column": column_name,
                "recursive": False,
                "total_count": total_count,
                "showing": min(total_count, limit),
                "truncated": truncated,
                "downstream": []
            }

            for lineage in downstream[column_name][:limit]:
                downstream_node = lineage_index.get_node(lineage.dbt_node)
                if downstream_node:
                    result["downstream"].append({
                        "model": downstream_node.get('name', ''),
                        "model_id": lineage.dbt_node,
                        "column": lineage.column,
                        "depth": 1,
                        "model_type": downstream_node.get('modelType', ''),
                        "description": lineage_index.get_column_description(lineage.dbt_node, lineage.column) or ""
                    })

            if truncated:
                result["note"] = f"Showing {limit} of {total_count} results. Increase limit parameter to see more."

            return result

    @mcp.tool(
        description="Find upstream source columns that feed into a specific column. Supports transitive lineage.",
        tags={"lineage", "column", "upstream"}
    )
    def find_column_upstream(
        model_name: str,
        column_name: str,
        recursive: bool = False,
        limit: int = 20
    ) -> dict:
        """
        Trace where a column comes from in the lineage.

        Args:
            model_name: Name of the dbt model
            column_name: Name of the column
            recursive: If True, get all transitive upstream columns (full source chain)
            limit: Maximum number of results to return (default: 20)

        Returns:
            Dictionary with column info and upstream sources
        """
        lineage_index = get_lineage_index()
        node_id = lineage_index.get_model_by_name(model_name)
        if not node_id:
            return {"error": f"Model '{model_name}' not found"}

        if recursive:
            upstream_tuples = lineage_index.get_upstream_columns_recursive(node_id, column_name)

            if not upstream_tuples:
                return {
                    "model": model_name,
                    "column": column_name,
                    "recursive": True,
                    "total_count": 0,
                    "showing": 0,
                    "truncated": False,
                    "upstream": []
                }

            total_count = len(upstream_tuples)
            truncated = total_count > limit

            result = {
                "model": model_name,
                "column": column_name,
                "recursive": True,
                "total_count": total_count,
                "showing": min(total_count, limit),
                "truncated": truncated,
                "upstream": []
            }

            for node_id_item, col_name, depth in upstream_tuples[:limit]:
                upstream_node = lineage_index.get_node(node_id_item)
                if upstream_node:
                    result["upstream"].append({
                        "model": upstream_node.get('name', ''),
                        "model_id": node_id_item,
                        "column": col_name,
                        "depth": depth,
                        "model_type": upstream_node.get('modelType', ''),
                        "node_type": upstream_node.get('nodeType', ''),
                        "description": lineage_index.get_column_description(node_id_item, col_name) or ""
                    })

            if truncated:
                result["note"] = f"Showing {limit} of {total_count} results. Increase limit parameter to see more."

            return result
        else:
            upstream = lineage_index.get_upstream_columns(node_id, column_name)

            if not upstream or column_name not in upstream:
                return {
                    "model": model_name,
                    "column": column_name,
                    "recursive": False,
                    "total_count": 0,
                    "showing": 0,
                    "truncated": False,
                    "upstream": []
                }

            total_count = len(upstream[column_name])
            truncated = total_count > limit

            result = {
                "model": model_name,
                "column": column_name,
                "recursive": False,
                "total_count": total_count,
                "showing": min(total_count, limit),
                "truncated": truncated,
                "upstream": []
            }

            for lineage in upstream[column_name][:limit]:
                upstream_node = lineage_index.get_node(lineage.dbt_node)
                if upstream_node:
                    result["upstream"].append({
                        "model": upstream_node.get('name', ''),
                        "model_id": lineage.dbt_node,
                        "column": lineage.column,
                        "depth": 1,
                        "model_type": upstream_node.get('modelType', ''),
                        "node_type": upstream_node.get('nodeType', ''),
                        "description": lineage_index.get_column_description(lineage.dbt_node, lineage.column) or ""
                    })

            if truncated:
                result["note"] = f"Showing {limit} of {total_count} results. Increase limit parameter to see more."

            return result

    @mcp.tool(
        description="Get detailed information about a dbt model including description and SQL",
        tags={"metadata", "model"}
    )
    def get_model_info(model_name: str, include_sql: bool = False) -> dict:
        """
        Get comprehensive information about a dbt model.

        Args:
            model_name: Name of the dbt model
            include_sql: If True, include raw and compiled SQL

        Returns:
            Dictionary with model metadata
        """
        lineage_index = get_lineage_index()
        node_id = lineage_index.get_model_by_name(model_name)
        if not node_id:
            return {"error": f"Model '{model_name}' not found"}

        model_info = lineage_index.get_model_info(node_id)
        if not model_info:
            return {"error": f"Could not load info for '{model_name}'"}

        result = {
            "id": model_info.id,
            "name": model_info.name,
            "type": model_info.node_type,
            "model_type": model_info.model_type,
            "materialized": model_info.materialized,
            "database": model_info.database,
            "schema": model_info.schema,
            "path": model_info.path,
            "description": model_info.description,
            "column_count": len(model_info.columns),
            "columns": []
        }

        for col_name, col_info in model_info.columns.items():
            result["columns"].append({
                "name": col_name,
                "data_type": col_info.get('dataType', ''),
                "description": col_info.get('description', ''),
                "lineage_type": col_info.get('lineageType', '')
            })

        if include_sql:
            result["raw_sql"] = model_info.raw_code
            result["compiled_sql"] = model_info.compiled_code

        return result

    @mcp.tool(
        description="Get the compiled SQL for a dbt model",
        tags={"sql", "model"}
    )
    def get_compiled_sql(model_name: str) -> dict:
        """
        Get the compiled SQL code for a dbt model.

        Args:
            model_name: Name of the dbt model

        Returns:
            Dictionary with SQL code
        """
        lineage_index = get_lineage_index()
        node_id = lineage_index.get_model_by_name(model_name)
        if not node_id:
            return {"error": f"Model '{model_name}' not found"}

        model_info = lineage_index.get_model_info(node_id)
        if not model_info:
            return {"error": f"Could not load info for '{model_name}'"}

        return {
            "model": model_name,
            "compiled_sql": model_info.compiled_code,
            "raw_sql": model_info.raw_code
        }

    @mcp.tool(
        description="Search for models by name or description",
        tags={"search", "model"}
    )
    def search_models(
        query: str,
        node_type: Optional[str] = None,
        limit: int = 20
    ) -> dict:
        """
        Search for dbt models by name or description.

        Args:
            query: Search query string
            node_type: Optional filter by node type (model, source, snapshot, etc.)
            limit: Maximum number of results to return

        Returns:
            Dictionary with search results
        """
        lineage_index = get_lineage_index()
        results = lineage_index.search_models(query, node_type=node_type)

        formatted_results = []
        for node_id in results[:limit]:
            node = lineage_index.get_node(node_id)
            if node:
                formatted_results.append({
                    "id": node_id,
                    "name": node.get('name', ''),
                    "type": node.get('nodeType', ''),
                    "model_type": node.get('modelType', ''),
                    "schema": node.get('schema', ''),
                    "description": node.get('description', '')[:150] + "..." if len(node.get('description', '')) > 150 else node.get('description', '')
                })

        return {
            "query": query,
            "total_results": len(results),
            "showing": len(formatted_results),
            "results": formatted_results
        }

    @mcp.tool(
        description="Find all models that contain a specific column name",
        tags={"column", "search"}
    )
    def find_models_with_column(column_name: str) -> dict:
        """
        Find all models that have a column with the given name.

        Args:
            column_name: Name of the column to search for

        Returns:
            Dictionary with list of models containing the column
        """
        lineage_index = get_lineage_index()
        models_with_column = lineage_index.find_models_with_column(column_name)

        result = {
            "column": column_name,
            "count": len(models_with_column),
            "models": []
        }

        for node_id, col_name in models_with_column:
            node = lineage_index.get_node(node_id)
            if node:
                result["models"].append({
                    "id": node_id,
                    "name": node.get('name', ''),
                    "type": node.get('nodeType', ''),
                    "model_type": node.get('modelType', ''),
                    "schema": node.get('schema', ''),
                    "column_description": lineage_index.get_column_description(node_id, col_name) or ""
                })

        return result

    @mcp.tool(
        description="Get a summary of the dbt project metadata",
        tags={"metadata", "project"}
    )
    def get_project_summary() -> dict:
        """
        Get a summary of the dbt project including counts and metadata.

        Returns:
            Dictionary with project summary statistics
        """
        lineage_index = get_lineage_index()
        node_counts = {}
        model_type_counts = {}

        for node_id, node in lineage_index.nodes.items():
            node_type = node.get('nodeType', 'unknown')
            node_counts[node_type] = node_counts.get(node_type, 0) + 1

            if node_type == 'model':
                model_type = node.get('modelType', 'unknown')
                model_type_counts[model_type] = model_type_counts.get(model_type, 0) + 1

        return {
            "project_name": lineage_index.metadata.get('dbt_project_name', ''),
            "dbt_version": lineage_index.metadata.get('dbt_version', ''),
            "adapter_type": lineage_index.metadata.get('adapter_type', ''),
            "generated_at": lineage_index.metadata.get('generated_at', ''),
            "total_nodes": len(lineage_index.nodes),
            "total_edges": len(lineage_index.edges),
            "node_counts": node_counts,
            "model_type_counts": model_type_counts
        }

    return mcp


def run_server():
    """
    Run the MCP server
    
    Note: This is for testing only. In production, use:
    fastmcp run dbt_colibri.mcp.main:mcp
    """
    mcp = create_mcp_server()
    mcp.run()


