import pytest
import os
import json
import tempfile
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from unittest.mock import patch, MagicMock
from sqlglot.lineage import SqlglotError
import logging

def test_adapter_type_validation_missing_adapter_type(dbt_valid_test_data_dir):
    """Test that missing adapter_type in manifest raises ValueError"""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    # Create a manifest without adapter_type
    with open(f"{dbt_valid_test_data_dir}/manifest.json", "r") as f:
        manifest_data = json.load(f)
    
    # Remove adapter_type from metadata
    if "metadata" in manifest_data and "adapter_type" in manifest_data["metadata"]:
        del manifest_data["metadata"]["adapter_type"]
    
    # Write modified manifest to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_manifest:
        json.dump(manifest_data, temp_manifest)
        temp_manifest_path = temp_manifest.name
    
    try:
        with pytest.raises(ValueError, match="adapter_type not found in manifest metadata"):
            DbtColumnLineageExtractor(
                manifest_path=temp_manifest_path,
                catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
            )
    finally:
        os.unlink(temp_manifest_path)


def test_adapter_type_validation_unsupported_adapter(dbt_valid_test_data_dir):
    """Test that unsupported adapter_type raises ValueError"""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    # Create a manifest with unsupported adapter_type
    with open(f"{dbt_valid_test_data_dir}/manifest.json", "r") as f:
        manifest_data = json.load(f)
    
    # Set unsupported adapter_type
    if "metadata" not in manifest_data:
        manifest_data["metadata"] = {}
    manifest_data["metadata"]["adapter_type"] = "unsupported_adapter"
    
    # Write modified manifest to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_manifest:
        json.dump(manifest_data, temp_manifest)
        temp_manifest_path = temp_manifest.name
    
    try:
        with pytest.raises(ValueError, match="Unsupported adapter type 'unsupported_adapter'"):
            DbtColumnLineageExtractor(
                manifest_path=temp_manifest_path,
                catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
            )
    finally:
        os.unlink(temp_manifest_path)


def test_adapter_type_detection_bigquery():
    """Test that BigQuery adapter type is correctly detected"""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/bigquery/manifest.json",
        catalog_path="tests/test_data/bigquery/catalog.json"
    )
    assert extractor.dialect == "bigquery"


def test_adapter_type_detection_duckdb():
    """Test that DuckDB adapter type is correctly detected"""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/duckdb/manifest.json",
        catalog_path="tests/test_data/duckdb/catalog.json"
    )
    assert extractor.dialect == "duckdb"

def test_extractor_initialization(dbt_valid_test_data_dir):
    """Test that the extractor can be initialized with valid parameters."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json",
    )

    assert isinstance(extractor, DbtColumnLineageExtractor)

    
    expected_nodes = [
        node_id
        for node_id, node_data in extractor.manifest.get("nodes", {}).items()
        if node_data.get("resource_type") in {"model", "snapshot"}
    ]

    # When selected_models is empty, it automatically selects all models and snapshots from manifest
    assert set(extractor.selected_models) == set(expected_nodes)

def test_extractor_with_specific_models(dbt_valid_test_data_dir):
    """Test that the extractor can be initialized with specific models."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    # Pick any model or snapshot from manifest
    specific_model = next((
        node_id for node_id, node_data in extractor.manifest.get("nodes", {}).items()
        if node_data.get("resource_type") in {"model", "snapshot"}
    ), None)
    if not specific_model:
        pytest.skip("No model or snapshot nodes found in manifest")
    assert specific_model in extractor.selected_models


def test_schema_dict_generation(dbt_valid_test_data_dir):
    """Test schema dictionary generation from catalog."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    
    # We'll pick any model from manifest to narrow the schema if possible
    tmp_extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    some_model = next((
        node_id for node_id, node_data in tmp_extractor.manifest.get("nodes", {}).items()
        if node_data.get("resource_type") == "model"
    ), None)
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json",
        selected_models=[some_model] if some_model else None
    )
    
    # Verify schema_dict structure
    assert extractor.schema_dict
    assert isinstance(extractor.schema_dict, dict)
    
    # Verify at least one database entry exists
    assert len(extractor.schema_dict) > 0
    
    # Get first database
    first_db = next(iter(extractor.schema_dict))
    assert extractor.schema_dict[first_db]
    assert isinstance(extractor.schema_dict[first_db], dict)
    
    # Get first schema
    first_schema = next(iter(extractor.schema_dict[first_db]))
    assert extractor.schema_dict[first_db][first_schema]
    assert isinstance(extractor.schema_dict[first_db][first_schema], dict)
    
    # Get first table
    first_table = next(iter(extractor.schema_dict[first_db][first_schema]))
    assert extractor.schema_dict[first_db][first_schema][first_table]
    assert isinstance(extractor.schema_dict[first_db][first_schema][first_table], dict)
    
    # Verify that table has column types
    assert len(extractor.schema_dict[first_db][first_schema][first_table]) > 0

def test_nodes_with_columns(dbt_valid_test_data_dir):
    """Test the merged node mapping with columns, keyed by normalized relation_name."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")

    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )

    nodes_with_columns = extractor.nodes_with_columns

    # Basic structure
    assert nodes_with_columns
    assert isinstance(nodes_with_columns, dict)
    assert len(nodes_with_columns) > 0

    # Verify keys are normalized relation names
    for relation_name, node_info in nodes_with_columns.items():
        assert isinstance(relation_name, str)
        assert "." in relation_name  # should look like catalog.schema.table

        # Verify node_info structure
        assert "unique_id" in node_info
        assert node_info["unique_id"].startswith(
            ("model.", "source.", "seed.", "snapshot.")
        )

        assert "database" in node_info
        assert "schema" in node_info
        assert "name" in node_info
        assert "columns" in node_info
        assert isinstance(node_info["columns"], dict)


def test_get_list_of_columns(dbt_valid_test_data_dir, caplog):
    """Test retrieving columns for a dbt node."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Try with a known model
    # Pick any model ID present in both manifest and catalog
    model_node = next(iter(extractor.manifest.get("nodes", {}).keys()), None)
    if not model_node:
        pytest.skip("No nodes in manifest")
    columns = extractor._get_list_of_columns_for_a_dbt_node(model_node)
    
    # Verify columns were returned
    assert columns
    assert isinstance(columns, list)
    assert len(columns) >= 0

    # Test with a guaranteed non-existent node
    missing_node = "model.does_not_exist"
    with caplog.at_level(logging.WARNING, logger="colibri"):
        no_columns = extractor._get_list_of_columns_for_a_dbt_node(missing_node)
        assert no_columns == []
        assert missing_node in caplog.text

def test_get_parent_nodes_catalog(dbt_valid_test_data_dir):
    """Test getting parent nodes catalog."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get a model that has dependencies
    # Select a node that has at least one dependency if available
    model_node = None
    for node_id, data in extractor.manifest.get("nodes", {}).items():
        deps = (data.get("depends_on") or {}).get("nodes", [])
        if deps:
            model_node = node_id
            break
    if model_node is None:
        pytest.skip("No nodes with dependencies found")
    model_info = extractor.manifest["nodes"][model_node]
    
    # Get parent catalog
    parent_catalog = extractor._get_parent_nodes_catalog(model_info)
    
    # Verify parent catalog structure
    assert parent_catalog
    assert "nodes" in parent_catalog
    assert "sources" in parent_catalog
    
    # Verify at least one parent exists (either in nodes or sources)
    parent_count = len(parent_catalog["nodes"]) + len(parent_catalog["sources"])
    assert parent_count > 0

def test_get_parents_snapshot_catalog(dbt_valid_test_data_dir):
    """Test getting parent nodes catalog."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get a model that has dependencies
    # Pick any snapshot node if present
    snapshot_node = next((
        node_id for node_id, node in extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "snapshot"
    ), None)
    if snapshot_node is None:
        pytest.skip("No snapshot nodes found")
    model_info = extractor.manifest["nodes"][snapshot_node]
    
    # Get parent catalog
    parent_catalog = extractor._get_parent_nodes_catalog(model_info)
    
    # Verify parent catalog structure
    assert parent_catalog
    assert "nodes" in parent_catalog
    assert "sources" in parent_catalog
    
    # Verify at least one parent exists (either in nodes or sources)
    parent_count = len(parent_catalog["nodes"]) + len(parent_catalog["sources"])
    assert parent_count > 0


def test_generate_schema_dict_snapshot_catalog(dbt_valid_test_data_dir):
    """Test getting parent nodes catalog."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get a model that has dependencies
    snapshot_node = next((
        node_id for node_id, node in extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "snapshot"
    ), None)
    if snapshot_node is None:
        pytest.skip("No snapshot nodes found")
    model_info = extractor.manifest["nodes"][snapshot_node]
    
    # Get parent catalog
    parent_catalog = extractor._get_parent_nodes_catalog(model_info)
    schema = extractor._generate_schema_dict_from_catalog(parent_catalog)
    
    # Verify parent catalog structure
    assert schema
    assert "nodes" in parent_catalog
    assert "sources" in parent_catalog
    
    # Verify at least one parent exists (either in nodes or sources)
    parent_count = len(parent_catalog["nodes"]) + len(parent_catalog["sources"])
    assert parent_count > 0


@patch('dbt_colibri.lineage_extractor.extractor.lineage')
def test_extract_lineage_for_model(mock_lineage):
    """Test extracting lineage for a model."""
    # Mock the lineage function to return a predictable result
    mock_lineage.return_value = [MagicMock()]
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/1.10/manifest.json",
        catalog_path="tests/test_data/1.10/catalog.json"
    )
    
    # Create test inputs
    model_sql = "SELECT id as customer_id, name FROM customers"
    schema = {"test_db": {"test_schema": {"customers": {"id": "int", "name": "varchar"}}}}
    model_node = "model.test.test_model"
    selected_columns = ["customer_id", "name"]
    
    # Call the method
    lineage_map = extractor._extract_lineage_for_model(
        model_sql=model_sql,
        schema=schema,
        model_node=model_node,
        selected_columns=selected_columns,
        resource_type="model"
    )
    
    # Verify the result
    assert lineage_map
    assert isinstance(lineage_map, dict)
    assert "customer_id" in lineage_map
    assert "name" in lineage_map
    
    # Verify lineage was called for each column
    assert mock_lineage.call_count == 2

def test_extract_snapshot_lineage_with_real_data(dbt_valid_test_data_dir):
    """Test extracting lineage for a model using actual test data."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get a real model from the manifest
    model_node = next((
        node_id for node_id, node in extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "snapshot"
    ), None)
    if model_node is None:
        pytest.skip("No snapshot nodes found")
    model_info = extractor.manifest["nodes"][model_node]
    model_sql = model_info["compiled_code"]
    
    # Get parent catalog and schema
    parent_catalog = extractor._get_parent_nodes_catalog(model_info)
    schema = extractor._generate_schema_dict_from_catalog(parent_catalog)
    
    # Get columns from the catalog
    columns = extractor._get_list_of_columns_for_a_dbt_node(model_node)
    
    # Call the method
    lineage_map = extractor._extract_lineage_for_model(
        model_sql=model_sql,
        schema=schema,
        model_node=model_node,
        selected_columns=columns,
        resource_type="snapshot"
    )
    
    # Verify the result
    assert lineage_map
    assert isinstance(lineage_map, dict)
    assert len(lineage_map) > 0
    
    # Check that at least one column has lineage information
    assert any(lineage for lineage in lineage_map.values())

def test_extract_lineage_with_real_data(dbt_valid_test_data_dir):
    """Test extracting lineage for a model using actual test data."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get a real model from the manifest
    model_node = next((
        node_id for node_id, node in extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model" and node.get("compiled_code")
    ), None)
    if model_node is None:
        pytest.skip("No suitable model with compiled SQL found")
    model_info = extractor.manifest["nodes"][model_node]
    model_sql = model_info["compiled_code"]
    
    # Get parent catalog and schema
    parent_catalog = extractor._get_parent_nodes_catalog(model_info)
    schema = extractor._generate_schema_dict_from_catalog(parent_catalog)
    
    # Get columns from the catalog
    columns = extractor._get_list_of_columns_for_a_dbt_node(model_node)
    
    # Call the method
    lineage_map = extractor._extract_lineage_for_model(
        model_sql=model_sql,
        schema=schema,
        model_node=model_node,
        selected_columns=columns,
        resource_type="model"
    )
    
    # Verify the result
    assert lineage_map
    assert isinstance(lineage_map, dict)
    assert len(lineage_map) > 0
    
    # Check that at least one column has lineage information
    assert any(lineage for lineage in lineage_map.values())

@patch('dbt_colibri.lineage_extractor.extractor.lineage')
def test_extract_lineage_error_handling(mock_lineage, dbt_valid_test_data_dir):
    """Test error handling during lineage extraction."""
    # Mock the lineage function to raise an error
    mock_lineage.side_effect = SqlglotError("Test error")
    
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Create test inputs
    model_sql = "SELECT id as customer_id FROM customers"
    schema = {"test_db": {"test_schema": {"customers": {"id": "int"}}}}
    model_node = "model.test.test_model"
    selected_columns = ["customer_id"]
    
    # Test that no exception is raised and empty result is returned
    lineage_map = extractor._extract_lineage_for_model(
        model_sql=model_sql,
        schema=schema,
        model_node=model_node,
        selected_columns=selected_columns,
        resource_type="model"
    )
    
    # Check that we got an empty result for the column
    assert lineage_map == {"customer_id": []}

def test_full_lineage_map_build(dbt_valid_test_data_dir):
    """Test building the complete lineage map for selected models."""
    # Use a subset of models for faster testing
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    
    tmp_extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    some_model = next((
        node_id for node_id, node in tmp_extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    ), None)
    if not some_model:
        pytest.skip("No model nodes found")
    selected_models = [some_model]
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json",
        selected_models=selected_models
    )
    
    # Build the lineage map
    lineage_map = extractor.build_lineage_map()
    
    # Verify the result
    assert lineage_map
    assert isinstance(lineage_map, dict)
    assert selected_models[0] in lineage_map
    
    # Verify the model has columns
    model_columns = lineage_map[selected_models[0]]
    assert model_columns
    assert isinstance(model_columns, dict)
    assert len(model_columns) > 0
    
    # Get actual column names from catalog
    columns = extractor._get_list_of_columns_for_a_dbt_node(selected_models[0])
    
    # Verify all expected columns are in the lineage map
    for column in columns:
        assert column in model_columns
    
    # Ensure the lineage map is a dict of columns -> list
    assert isinstance(model_columns, dict)

def test_column_lineage_with_real_data(dbt_valid_test_data_dir):
    """Test the full column lineage extraction process with real data."""
    # Use a real model from test data
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    
    tmp_extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    some_model = next((
        node_id for node_id, node in tmp_extractor.manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    ), None)
    if not some_model:
        pytest.skip("No model nodes found")
    selected_models = [some_model]
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json",
        selected_models=selected_models
    )
    
    # First build the lineage map
    lineage_map = extractor.build_lineage_map()
    
    # Now extract column lineage from the lineage map
    columns_lineage = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
    
    # Verify the result
    assert columns_lineage
    assert selected_models[0].lower() in columns_lineage
    
    model_columns = columns_lineage[selected_models[0].lower()]
    assert model_columns
    assert isinstance(model_columns, dict)
    
    # Verify parent format if present, but don't enforce presence as datasets vary
    for parents in model_columns.values():
        for parent in parents:
            assert "column" in parent
            assert "dbt_node" in parent

def test_find_all_related():
    """Test finding all related columns."""
    # Set up test data
    # This is a parent-to-child lineage map (parent -> children who reference it)
    direct_children_lineage = {
        "model.test.parent": {
            "id": [
                {"column": "id", "dbt_node": "model.test.child"},
                {"column": "parent_id", "dbt_node": "model.test.grandchild"}
            ],
            "name": [
                {"column": "name", "dbt_node": "model.test.child"}
            ]
        },
        "model.test.child": {
            "id": [
                {"column": "child_id", "dbt_node": "model.test.grandchild"}
            ]
        }
    }
    
    # Find all related columns for parent.id (should find columns that reference it)
    related = DbtColumnLineageExtractor.find_all_related(
        direct_children_lineage, "model.test.parent", "id"
    )
    
    # Verify the result
    assert related
    assert "model.test.child" in related
    assert "model.test.grandchild" in related
    assert "id" in related["model.test.child"]
    assert "parent_id" in related["model.test.grandchild"]
    assert "child_id" in related["model.test.grandchild"]

def test_find_all_related_with_structure():
    """Test finding all related columns with structure."""
    # Set up test data - parent-to-child lineage
    direct_children_lineage = {
        "model.test.parent": {
            "id": [
                {"column": "id", "dbt_node": "model.test.child"}
            ],
            "name": [
                {"column": "name", "dbt_node": "model.test.child"}
            ]
        },
        "model.test.child": {
            "id": [
                {"column": "child_id", "dbt_node": "model.test.grandchild"}
            ]
        }
    }
    
    # Find all related columns with structure for parent.id
    related_structure = DbtColumnLineageExtractor.find_all_related_with_structure(
        direct_children_lineage, "model.test.parent", "id"
    )
    
    # Verify the result
    assert related_structure
    assert "model.test.child" in related_structure
    assert "id" in related_structure["model.test.child"]
    assert "+" in related_structure["model.test.child"]["id"]
    
    # Verify the nested structure
    assert "model.test.grandchild" in related_structure["model.test.child"]["id"]["+"]
    assert "child_id" in related_structure["model.test.child"]["id"]["+"]["model.test.grandchild"]

def test_python_model_handling():
    """Test handling of Python models during lineage map building."""
    # Create a mock manifest with a Python model
    manifest = {
        "metadata": {
            "adapter_type": "snowflake"
        },
        "nodes": {
            "model.test.python_model": {
                "path": "models/python_model.py",
                "resource_type": "model",
                "compiled_code": "# This is a Python model",
                "depends_on": {"nodes": []},
                "database": "test_db",
                "schema": "test_schema",
                "name": "python_model",
                "columns": {},
                "relation_name": "test_db.test_schema.python_model",
                "config": { "materialized": "table" }
            }
        },
        "sources": {}
    }
    
    # Mock catalog to match the manifest
    catalog = {
        "nodes": {},
        "sources": {}
    }
    
    # Patch the read_json method to return our mock manifest and catalog
    with patch('dbt_colibri.utils.json_utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]
        
        with patch.object(DbtColumnLineageExtractor, '_generate_schema_dict_from_catalog') as mock_schema:
            mock_schema.return_value = {}
            
            with patch.object(DbtColumnLineageExtractor, '_get_dict_mapping_full_table_name_to_dbt_node') as mock_mapping:
                mock_mapping.return_value = {}
                
                extractor = DbtColumnLineageExtractor(
                    manifest_path="dummy_path",
                    catalog_path="dummy_path",
                    selected_models=["model.test.python_model"],
                )
                
                # Build the lineage map
                lineage_map = extractor.build_lineage_map()
                
                # Verify that the Python model was skipped
                assert lineage_map == {}


# Minimal mocks to mimic the sqlglot node shape expected by the method
class MockSource:
    def __init__(self, catalog, db, name, key="table"):
        self.catalog = catalog
        self.db = db
        self.name = name
        self.key = key

class MockSqlglotNode:
    def __init__(self, source, full_name_for_column):
        self.source = source
        # get_dbt_node_from_sqlglot_table_node uses node.name.split(".")[-1]
        self.name = full_name_for_column


def make_extractor(manifest_nodes=None, nodes_with_columns=None):
    """Create an extractor-like object without running __init__ (avoids file IO)."""
    extractor = object.__new__(DbtColumnLineageExtractor)
    extractor.logger = logging.getLogger("colibri_test")
    # manifest should be a dict with "nodes" key (as the real class expects)
    extractor.manifest = {"nodes": manifest_nodes or {}}
    extractor.nodes_with_columns = nodes_with_columns or {}
    extractor.dialect = 'clickhouse'  # Set dialect to clickhouse for the test
    return extractor


def test_clickhouse_leading_dot_table_matches():
    """
    Simulate ClickHouse where only db and table are used (no catalog),
    """
    model_node = "model.jaffle_shop.products"
    
    # Real manifest structure from example
    manifest_nodes = {
        model_node: {
            "database": "",
            "schema": "jaffle_shop",
            "name": "products",
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.products",
            "alias": "products",
            "relation_name": "`jaffle_shop`.`products`",
            "raw_code": "with\n\nproducts as (\n\n    select * from {{ ref('stg_products') }}\n\n)\n\nselect * from products",
            "compiled_code": "with\n\nproducts as (\n\n    select * from `jaffle_shop`.`stg_products`\n\n)\n\nselect * from products",
            "refs": [
                {
                    "name": "stg_products",
                    "package": None,
                    "version": None
                }
            ],
            "depends_on": {
                "nodes": ["model.jaffle_shop.stg_products"]
            }
        }
    }
    
    # Real nodes_with_columns structure from your example
    nodes_with_columns = {
        "jaffle_shop.stg_products": {
            "unique_id": "model.jaffle_shop.stg_products",
            "database": "",
            "schema": "jaffle_shop",
            "name": "stg_products",
            "resource_type": "model",
            "columns": {
                "product_id": {
                    "type": "String",
                    "index": 1,
                    "name": "product_id",
                    "comment": None
                },
                "is_drink_item": {
                    "type": "UInt8",
                    "index": 7,
                    "name": "is_drink_item",
                    "comment": None
                }
            }
        }
    }
        
    extractor = make_extractor(manifest_nodes=manifest_nodes, nodes_with_columns=nodes_with_columns)

    # Simulate sqlglot table node for ClickHouse with empty catalog
    src = MockSource(catalog="", db="jaffle_shop", name="stg_products")
    node = MockSqlglotNode(src, "stg_products.product_id")

    result = extractor.get_dbt_node_from_sqlglot_table_node(node, model_node=model_node)

    # Should find the table in nodes_with_columns mapping
    assert result["column"] == "product_id"
    assert result["dbt_node"] == "model.jaffle_shop.stg_products"
    assert not result["dbt_node"].startswith("_HARDCODED_REF___")
    assert not result["dbt_node"].startswith("_NOT_FOUND___")


def test_nodes_with_null_relation_name_are_skipped():
    """
    Test that nodes with null relation_name (e.g., dbt operations like on-run-end hooks)
    are skipped and don't cause errors during build_nodes_with_columns.

    This prevents the error: "expected string or bytes-like object" when
    normalize_table_relation_name receives None instead of a string.
    """
    # Create a manifest with a node that has null relation_name (like dbt operations)
    manifest = {
        "metadata": {
            "adapter_type": "snowflake"
        },
        "nodes": {
            "operation.test_project.test-on-run-end-0": {
                "path": "hooks/on-run-end.sql",
                "resource_type": "model",  # Operations can be misclassified as models
                "compiled_code": None,
                "depends_on": {"nodes": []},
                "database": "test_db",
                "schema": "test_schema",
                "name": "test-on-run-end-0",
                "columns": {},
                "relation_name": None,  # Operations have null relation_name
                "config": {"materialized": "view"}
            },
            "model.test_project.valid_model": {
                "path": "models/valid_model.sql",
                "resource_type": "model",
                "compiled_code": "SELECT 1 as id",
                "depends_on": {"nodes": []},
                "database": "test_db",
                "schema": "test_schema",
                "name": "valid_model",
                "columns": {},
                "relation_name": "test_db.test_schema.valid_model",
                "config": {"materialized": "table"}
            }
        },
        "sources": {},
        "parent_map": {},
        "child_map": {}
    }

    catalog = {
        "nodes": {
            "model.test_project.valid_model": {
                "unique_id": "model.test_project.valid_model",
                "metadata": {
                    "database": "test_db",
                    "schema": "test_schema",
                    "name": "valid_model",
                    "type": "table"
                },
                "columns": {
                    "id": {"type": "INTEGER", "name": "id", "index": 1}
                }
            }
        },
        "sources": {}
    }

    with patch('dbt_colibri.utils.json_utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]

        # This should NOT raise "expected string or bytes-like object" error
        extractor = DbtColumnLineageExtractor(
            manifest_path="dummy_path",
            catalog_path="dummy_path"
        )

        # Verify the null relation_name node was skipped
        nodes_with_columns = extractor.nodes_with_columns

        # The valid model should be present
        assert any("valid_model" in key for key in nodes_with_columns.keys())

        # The operation with null relation_name should NOT be present
        assert not any("on-run-end" in key for key in nodes_with_columns.keys())
