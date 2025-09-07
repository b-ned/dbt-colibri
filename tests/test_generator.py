from dbt_colibri.report.generator import DbtColibriReportGenerator
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
import pytest

def test_build_manifest_node_data_node_not_found(dbt_valid_test_data_dir):
    """Test build_manifest_node_data when node_id is not found in manifest or catalog."""
    
    # Create an extractor instance
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Create a report generator instance
    report_generator = DbtColibriReportGenerator(extractor)
    
    # Test with a non-existent node_id
    non_existent_node_id = "model.does_not_exist.fake_model"
    
    # Call build_manifest_node_data with non-existent node
    node_data = report_generator.build_manifest_node_data(non_existent_node_id)
    
    # Verify the result structure when node is not found
    expected_structure = {
        "nodeType": "unknown",
        "rawCode": None,
        "compiledCode": None,
        "materialized": None,
        "path": None,
        "database": None,
        "schema": None,
        "description": None,
        "contractEnforced": None,
        "refs": [],
        "columns": {},
    }
    
    assert node_data == expected_structure
    assert node_data["nodeType"] == "unknown"
    assert node_data["rawCode"] is None
    assert node_data["compiledCode"] is None
    assert node_data["schema"] is None
    assert node_data["description"] is None
    assert node_data["contractEnforced"] is None
    assert node_data["refs"] == []
    assert node_data["columns"] == {}


def test_detect_model_type_with_non_existent_node(dbt_valid_test_data_dir):
    """Test detect_model_type with a non-existent node_id."""
    
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    report_generator = DbtColibriReportGenerator(extractor)
    
    # Test with various non-existent node patterns
    test_cases = [
        ("model.does_not_exist.fake_model", "unknown"),
        ("model.does_not_exist.dim_fake", "dimension"),
        ("model.does_not_exist.fact_fake", "fact"),
        ("model.does_not_exist.int_fake", "intermediate"),
        ("model.does_not_exist.stg_fake", "staging"),
        ("completely.malformed.node.id", "unknown"),
        ("", "unknown"),
    ]
    
    for node_id, expected_type in test_cases:
        result = report_generator.detect_model_type(node_id)
        assert result == expected_type, f"Expected {expected_type} for {node_id}, got {result}"


def test_ensure_node_with_missing_node_creates_default(dbt_valid_test_data_dir):
    """Test that ensure_node creates a default node structure when node is missing."""
    from unittest.mock import patch
    
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    report_generator = DbtColibriReportGenerator(extractor)
    
    # Mock extract_project_lineage to return lineage data that references a non-existent node
    with patch.object(extractor, 'extract_project_lineage') as mock_extract:
        mock_extract.return_value = {
            "lineage": {
                "parents": {
                    "model.exists.child": {
                        "col1": [
                            {"dbt_node": "model.does_not_exist.parent", "column": "col1"}
                        ]
                    }
                },
                "children": {}
            }
        }
        
        # Build lineage - this should create the missing node with default values
        result = report_generator.build_full_lineage()
        
        # Verify both nodes exist in the result
        assert "model.exists.child" in result["nodes"]
        assert "model.does_not_exist.parent" in result["nodes"]
        
        # Verify the missing node has default structure
        missing_node = result["nodes"]["model.does_not_exist.parent"]
        assert missing_node["nodeType"] == "unknown"
        assert missing_node["modelType"] == "unknown"  # Since it doesn't match any prefix
        assert missing_node["rawCode"] is None
        assert missing_node["compiledCode"] is None
        assert missing_node["schema"] is None
        assert missing_node["description"] is None
        assert missing_node["columns"] == {}
        
        # Verify the edge was still created
        assert len(result["lineage"]["edges"]) > 0
        edge_found = False
        for edge in result["lineage"]["edges"]:
            if (edge["source"] == "model.does_not_exist.parent" and 
                edge["target"] == "model.exists.child"):
                edge_found = True
                break
        assert edge_found, "Expected edge between non-existent parent and child"

def test_generated_report_excludes_test_nodes(dbt_valid_test_data_dir):
    """Ensure test nodes are excluded and non-test resource types are present."""

    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    manifest_path = f"{dbt_valid_test_data_dir}/manifest.json"
    catalog_path = f"{dbt_valid_test_data_dir}/catalog.json"

    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )
    report_generator = DbtColibriReportGenerator(extractor)
    result = report_generator.build_full_lineage()
    nodes = result.get("nodes", {})
    assert nodes, "No nodes found in generated report"

    # Assert no test nodes exist
    for node_id, node_data in nodes.items():
        assert not node_id.startswith("test."), f"Test node found by ID: {node_id}"
        assert node_data.get("nodeType") != "test", f"Test node found by type: {node_id}"

    # Assert that we have some known non-test node types in the result
    expected_types = {"model", "source"}
    found_types = {node["nodeType"] for node in nodes.values()}

    missing_types = expected_types - found_types
    assert not missing_types, f"Missing expected node types: {missing_types}"
