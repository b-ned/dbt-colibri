
import json
import os
from collections import Counter
from dbt_colibri.utils.parsing_utils import normalize_table_relation_name


def test_normalize_relation_name_across_sql_dialects():
    """Test normalize_relation_name function with real data from different SQL dialects."""
    test_data_base = "tests/test_data"

    # Automatically discover all subdirectories in test_data
    if os.path.exists(test_data_base):
        test_data_dirs = [
            os.path.join(test_data_base, subdir)
            for subdir in os.listdir(test_data_base)
            if os.path.isdir(os.path.join(test_data_base, subdir))
        ]
    else:
        test_data_dirs = []


    for test_dir in test_data_dirs:
        if not os.path.exists(test_dir):
            continue

        manifest_path = os.path.join(test_dir, "manifest.json")
        if not os.path.exists(manifest_path):
            continue

        # Read manifest file
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        # Extract relation_names from nodes
        relation_names = []

        for node_id, node_data in manifest.get("nodes", {}).items():
            relation_name = node_data.get("relation_name")
            if relation_name and isinstance(relation_name, str):
                relation_names.append(relation_name)

        for node_id, node_data in manifest.get("sources", {}).items():
            relation_name = node_data.get("relation_name")
            if relation_name and isinstance(relation_name, str):
                relation_names.append(relation_name)

        if not relation_names:
            continue

        # Test normalize_relation_name function
        normalized_relation_names = []
        for relation_name in relation_names:
            normalized = normalize_table_relation_name(relation_name)
            normalized_relation_names.append(normalized)

            # Verify basic normalization properties
            assert isinstance(normalized, str)
            assert not normalized.startswith('"')  # Should have quotes removed
            assert not normalized.startswith("'")
            assert not normalized.endswith('"')
            assert not normalized.endswith("'")
            assert not normalized.startswith('`')  # Should have backticks removed
            assert not normalized.endswith('`')


def count_manifest_objects(manifest: dict) -> dict:
    """
    Count nodes and sources in a dbt manifest.json.
    
    Returns a dict with totals and breakdown by resource_type.
    """
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    # Count resource types inside nodes
    resource_counts = Counter(
        node.get("resource_type", "unknown") for node in nodes.values()
    )

    return {
        "nodes_total": len(nodes),
        "sources_total": len(sources),
        "total": len(nodes) + len(sources),
        "by_resource_type": dict(resource_counts),
    }


def count_edges_with_double_colon(result: dict) -> dict:
    """
    Count edges with :: (double colon) in the lineage result.
    
    These are model-level edges that show relationships between dbt models/sources.
    Edge IDs follow the pattern: "{source_node}::->{target_node}::"
    
    Args:
        result: The result dictionary from DbtColibriReportGenerator.build_full_lineage()
        
    Returns:
        dict: Contains counts of different edge types and totals.
    """
    lineage = result.get("lineage", {})
    edges = lineage.get("edges", [])
    nodes = result.get("nodes", {})
    hardcoded_nodes = { node_id: node
        for node_id, node in nodes.items()
        if node.get("nodeType") == "hardcoded"}
    
    # Count edges with :: (model-level edges)
    model_level_edges = [edge for edge in edges if "::" in edge.get("id", "")]
    
    # Count edges without :: (column-level edges) 
    column_level_edges = [edge for edge in edges if "::" not in edge.get("id", "")]
    
    return {
        "model_level_edges": len(model_level_edges),
        "column_level_edges": len(column_level_edges), 
        "total_edges": len(edges),
        "nodes_total": len(nodes),
        "nodes_by_type": dict(Counter(node.get("nodeType", "unknown") for node in nodes.values())),
        "hardcoded_nodes": len(hardcoded_nodes)
    }


