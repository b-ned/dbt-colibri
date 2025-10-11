"""
Lineage Index for dbt Colibri Metadata

This module provides efficient querying of dbt model lineage and metadata
from the colibri-manifest.json file.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass


@dataclass
class ColumnLineage:
    """Represents a column-level lineage relationship"""
    column: str
    dbt_node: str


@dataclass
class ModelInfo:
    """Represents a dbt model with its metadata"""
    id: str
    name: str
    node_type: str
    model_type: str
    description: str
    database: str
    schema: str
    path: str
    materialized: str
    raw_code: str
    compiled_code: str
    columns: Dict[str, Any]


class DbtLineageIndex:
    """
    Efficient in-memory index for dbt lineage queries.

    Loads the colibri-manifest.json file and provides O(1) lookups for:
    - Downstream models/columns
    - Upstream models/columns
    - Model metadata
    - Column descriptions
    """

    def __init__(self, json_path: str):
        """Load and index the dbt lineage data"""
        data_path = Path(json_path)
        if not data_path.exists():
            raise FileNotFoundError(f"Lineage file not found: {json_path}")

        with open(data_path) as f:
            data = json.load(f)

        self.nodes = data['nodes']
        self.children = data['lineage']['children']
        self.parents = data['lineage']['parents']
        self.edges = data['lineage']['edges']
        self.metadata = data['metadata']

        # Build reverse indices for faster lookups
        self._build_indices()

    def _build_indices(self):
        """Build reverse indices for efficient queries"""
        # Index: column name -> list of (node_id, column_name) that contain it
        self.column_to_nodes: Dict[str, List[tuple]] = {}

        for node_id, node in self.nodes.items():
            if 'columns' in node:
                for col_name in node['columns'].keys():
                    if col_name not in self.column_to_nodes:
                        self.column_to_nodes[col_name] = []
                    self.column_to_nodes[col_name].append((node_id, col_name))

        # Index: model name (without prefix) -> full node_id
        self.model_name_to_id: Dict[str, str] = {}
        for node_id, node in self.nodes.items():
            name = node.get('name', '')
            if name:
                self.model_name_to_id[name] = node_id

    def get_model_by_name(self, model_name: str) -> Optional[str]:
        """Get full node ID from model name"""
        return self.model_name_to_id.get(model_name)

    def get_node(self, node_id: str) -> Optional[Dict]:
        """Get node metadata by ID"""
        return self.nodes.get(node_id)

    def get_model_info(self, node_id: str) -> Optional[ModelInfo]:
        """Get structured model information"""
        node = self.get_node(node_id)
        if not node:
            return None

        return ModelInfo(
            id=node_id,
            name=node.get('name', ''),
            node_type=node.get('nodeType', ''),
            model_type=node.get('modelType', ''),
            description=node.get('description', ''),
            database=node.get('database', ''),
            schema=node.get('schema', ''),
            path=node.get('path', ''),
            materialized=node.get('materialized', ''),
            raw_code=node.get('rawCode', ''),
            compiled_code=node.get('compiledCode', ''),
            columns=node.get('columns', {})
        )

    def get_downstream_columns(self, node_id: str, column: Optional[str] = None) -> Dict[str, List[ColumnLineage]]:
        """
        Get downstream columns for a node or specific column.

        Args:
            node_id: The dbt node ID
            column: Optional specific column name. If None, returns all columns.

        Returns:
            Dict mapping source columns to list of downstream ColumnLineage objects
        """
        node_children = self.children.get(node_id, {})

        if column:
            # Return only specific column's downstream
            return {column: [
                ColumnLineage(c['column'], c['dbt_node'])
                for c in node_children.get(column, [])
            ]}

        # Return all columns' downstream
        result = {}
        for col_name, downstream in node_children.items():
            result[col_name] = [
                ColumnLineage(c['column'], c['dbt_node'])
                for c in downstream
            ]
        return result

    def get_upstream_columns(self, node_id: str, column: Optional[str] = None) -> Dict[str, List[ColumnLineage]]:
        """
        Get upstream columns for a node or specific column.

        Args:
            node_id: The dbt node ID
            column: Optional specific column name. If None, returns all columns.

        Returns:
            Dict mapping target columns to list of upstream ColumnLineage objects
        """
        node_parents = self.parents.get(node_id, {})

        if column:
            # Return only specific column's upstream
            return {column: [
                ColumnLineage(c['column'], c['dbt_node'])
                for c in node_parents.get(column, [])
            ]}

        # Return all columns' upstream
        result = {}
        for col_name, upstream in node_parents.items():
            result[col_name] = [
                ColumnLineage(c['column'], c['dbt_node'])
                for c in upstream
            ]
        return result

    def get_downstream_columns_recursive(self, node_id: str, column: str) -> List[tuple]:
        """
        Get all transitive downstream columns using BFS traversal.

        Args:
            node_id: The dbt node ID
            column: The column name to trace

        Returns:
            List of tuples: (node_id, column_name, depth)
        """
        from collections import deque

        results = []
        visited = set()
        queue = deque([(node_id, column, 0)])
        visited.add((node_id, column))

        while queue:
            current_node, current_col, depth = queue.popleft()

            # Get immediate downstream for this column
            node_children = self.children.get(current_node, {})
            downstream = node_children.get(current_col, [])

            for child in downstream:
                child_node = child['dbt_node']
                child_col = child['column']
                key = (child_node, child_col)

                # Skip if already visited
                if key not in visited:
                    visited.add(key)
                    # Add to results (this is a downstream column)
                    results.append((child_node, child_col, depth + 1))
                    # Add to queue for further traversal
                    queue.append((child_node, child_col, depth + 1))

        return results

    def get_upstream_columns_recursive(self, node_id: str, column: str) -> List[tuple]:
        """
        Get all transitive upstream columns using BFS traversal.

        Args:
            node_id: The dbt node ID
            column: The column name to trace

        Returns:
            List of tuples: (node_id, column_name, depth)
        """
        from collections import deque

        results = []
        visited = set()
        queue = deque([(node_id, column, 0)])
        visited.add((node_id, column))

        while queue:
            current_node, current_col, depth = queue.popleft()

            # Get immediate upstream for this column
            node_parents = self.parents.get(current_node, {})
            upstream = node_parents.get(current_col, [])

            for parent in upstream:
                parent_node = parent['dbt_node']
                parent_col = parent['column']
                key = (parent_node, parent_col)

                # Skip if already visited
                if key not in visited:
                    visited.add(key)
                    # Add to results (this is an upstream column)
                    results.append((parent_node, parent_col, depth + 1))
                    # Add to queue for further traversal
                    queue.append((parent_node, parent_col, depth + 1))

        return results

    def get_downstream_models(self, node_id: str, recursive: bool = False, filter_type: Optional[str] = None) -> Set[str]:
        """
        Get all downstream models.

        Args:
            node_id: The dbt node ID
            recursive: If True, get all transitive downstream models
            filter_type: Optional filter (e.g., 'exp' for exposure models)

        Returns:
            Set of downstream node IDs
        """
        downstream = set()

        # Get immediate downstream from children
        node_children = self.children.get(node_id, {})
        for column_children in node_children.values():
            for child in column_children:
                downstream.add(child['dbt_node'])

        # Recursive traversal if requested
        if recursive:
            visited = set()
            to_visit = list(downstream)

            while to_visit:
                current = to_visit.pop()
                if current in visited:
                    continue
                visited.add(current)

                current_children = self.children.get(current, {})
                for column_children in current_children.values():
                    for child in column_children:
                        child_id = child['dbt_node']
                        if child_id not in visited:
                            downstream.add(child_id)
                            to_visit.append(child_id)

        # Apply filter if specified
        if filter_type:
            if filter_type == 'exp':
                # Filter for exposure models (starting with 'exp_')
                downstream = {
                    n for n in downstream
                    if self.nodes.get(n, {}).get('name', '').startswith('exp_')
                }
            elif filter_type == 'metric':
                # Filter for metrics (would need meta field - placeholder)
                downstream = {
                    n for n in downstream
                    if self.nodes.get(n, {}).get('nodeType') == 'metric'
                }

        return downstream

    def get_upstream_models(self, node_id: str, recursive: bool = False) -> Set[str]:
        """
        Get all upstream models.

        Args:
            node_id: The dbt node ID
            recursive: If True, get all transitive upstream models

        Returns:
            Set of upstream node IDs
        """
        upstream = set()

        # Get immediate upstream from parents
        node_parents = self.parents.get(node_id, {})
        for column_parents in node_parents.values():
            for parent in column_parents:
                upstream.add(parent['dbt_node'])

        # Recursive traversal if requested
        if recursive:
            visited = set()
            to_visit = list(upstream)

            while to_visit:
                current = to_visit.pop()
                if current in visited:
                    continue
                visited.add(current)

                current_parents = self.parents.get(current, {})
                for column_parents in current_parents.values():
                    for parent in column_parents:
                        parent_id = parent['dbt_node']
                        if parent_id not in visited:
                            upstream.add(parent_id)
                            to_visit.append(parent_id)

        return upstream

    def search_models(self, query: str, node_type: Optional[str] = None) -> List[str]:
        """
        Search for models by name or description.

        Args:
            query: Search query string
            node_type: Optional node type filter (e.g., 'model', 'source')

        Returns:
            List of matching node IDs
        """
        results = []
        query_lower = query.lower()

        for node_id, node in self.nodes.items():
            # Apply node type filter
            if node_type and node.get('nodeType') != node_type:
                continue

            # Search in name and description
            name = node.get('name', '').lower()
            description = node.get('description', '').lower()

            if query_lower in name or query_lower in description:
                results.append(node_id)

        return results

    def get_column_description(self, node_id: str, column: str) -> Optional[str]:
        """Get description for a specific column"""
        node = self.get_node(node_id)
        if not node or 'columns' not in node:
            return None

        column_info = node['columns'].get(column, {})
        return column_info.get('description')

    def find_models_with_column(self, column_name: str) -> List[tuple]:
        """
        Find all models that have a specific column.

        Returns:
            List of (node_id, column_name) tuples
        """
        return self.column_to_nodes.get(column_name, [])


