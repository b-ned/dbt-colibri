import pytest
import os
import json
import tempfile
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from unittest.mock import patch, MagicMock
from sqlglot.lineage import SqlglotError


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

def test_node_mapping(dbt_valid_test_data_dir):
    """Test the node mapping dictionary generation."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Verify node_mapping structure
    assert extractor.node_mapping
    assert isinstance(extractor.node_mapping, dict)
    assert len(extractor.node_mapping) > 0
    
    # Verify some mappings exist (format should be "catalog.schema.table" -> "model.package.name")
    for table_name, dbt_node in extractor.node_mapping.items():
        assert "." in table_name
        assert dbt_node.startswith(("model.", "source.", "seed.", "snapshot."))

def test_get_list_of_columns(dbt_valid_test_data_dir):
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
    
    # Test with a non-existent node
    with pytest.warns(UserWarning):
        no_columns = extractor._get_list_of_columns_for_a_dbt_node("model.does_not_exist")
        assert no_columns == []

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
        selected_columns=selected_columns
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
        selected_columns=columns
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
        selected_columns=columns
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
        selected_columns=selected_columns
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

def test_get_dbt_node_from_sqlglot_table_node(dbt_valid_test_data_dir):
    """Test converting sqlglot table nodes to dbt nodes."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Mock a sqlglot node
    class MockNode:
        def __init__(self):
            self.name = "customer_id"
            self.source = MagicMock()
            self.source.key = "table"
            self.source.catalog = "test_catalog"
            self.source.db = "test_schema"
            self.source.name = "customers"
    
    # Add a mapping to the extractor
    test_table = "test_catalog.test_schema.customers"
    extractor.node_mapping[test_table] = "model.test.customers"
    
    # Add the fully qualified table name to node mapping
    fq_table_name = "test_catalog.test_schema.customers"
    dbt_node_id = "model.test.customers"
    extractor.node_mapping[fq_table_name] = dbt_node_id

    # Coherent mock model_node (as from DBT manifest["nodes"])
    mock_model_node = {
        "unique_id": dbt_node_id,
        "resource_type": "model",
        "name": "customers",
        "database": "test_catalog",
        "schema": "test_schema",
        "raw_code": "some sql code",
        "columns": {
            "customer_id": {
                "name": "customer_id",
                "description": "Unique customer identifier"
            }
        }
    }

    # Test the conversion
    result = extractor.get_dbt_node_from_sqlglot_table_node(MockNode(), mock_model_node)
    
    # Verify the result
    assert result
    assert isinstance(result, dict)
    assert "column" in result
    assert "dbt_node" in result
    assert result["column"] == "customer_id"
    assert result["dbt_node"] == "model.test.customers"

def test_get_columns_lineage_from_sqlglot_lineage_map(dbt_valid_test_data_dir):
    """Test extracting column lineage from the sqlglot lineage map."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json",
        selected_models=["model.test.child"]
    )
    
    # Mock a lineage map
    class MockNode:
        def __init__(self, source_key="table", table_name="parent", column_name="id"):
            self.name = column_name
            self.source = MagicMock()
            self.source.key = source_key
            self.source.catalog = "test_catalog"
            self.source.db = "test_schema"
            self.source.name = table_name
        
        def walk(self):
            return [self]
    
    lineage_map = {
        "model.test.child": {
            "id": MockNode()
        }
    }
    
    # Add the mapping
    extractor.node_mapping["test_catalog.test_schema.parent"] = "model.test.parent"
    
    # Get the columns lineage
    columns_lineage = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
    
    # Verify the result: since the model is not in the manifest graphs, it should be skipped
    assert columns_lineage == {}

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

def test_get_lineage_to_direct_children(dbt_valid_test_data_dir):
    """Test getting lineage to direct children from lineage to direct parents."""
    # Set up test data
    lineage_to_direct_parents = {
        "model.test.child": {
            "id": [
                {"column": "id", "dbt_node": "model.test.parent"}
            ],
            "name": [
                {"column": "full_name", "dbt_node": "model.test.parent"}
            ]
        },
        "model.test.grandchild": {
            "child_id": [
                {"column": "id", "dbt_node": "model.test.child"}
            ]
        }
    }
    
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Get lineage to direct children
    children_lineage = extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(
        lineage_to_direct_parents
    )
    
    # Verify the result
    assert children_lineage
    assert "model.test.parent" in children_lineage
    assert "id" in children_lineage["model.test.parent"]
    assert "full_name" in children_lineage["model.test.parent"]
    
    # Verify parent.id points to child.id
    assert len(children_lineage["model.test.parent"]["id"]) == 1
    assert children_lineage["model.test.parent"]["id"][0]["dbt_node"] == "model.test.child"
    assert children_lineage["model.test.parent"]["id"][0]["column"] == "id"
    
    # Verify parent.full_name points to child.name
    assert len(children_lineage["model.test.parent"]["full_name"]) == 1
    assert children_lineage["model.test.parent"]["full_name"][0]["dbt_node"] == "model.test.child"
    assert children_lineage["model.test.parent"]["full_name"][0]["column"] == "name"
    
    # Verify child.id points to grandchild.child_id
    assert "model.test.child" in children_lineage
    assert "id" in children_lineage["model.test.child"]
    assert len(children_lineage["model.test.child"]["id"]) == 1
    assert children_lineage["model.test.child"]["id"][0]["dbt_node"] == "model.test.grandchild"
    assert children_lineage["model.test.child"]["id"][0]["column"] == "child_id"

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
                "columns": {}
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



def test_source_identifier_handling():
    """Test that source identifiers are correctly handled in node mapping."""
    # Create a mock manifest with a source that has an identifier
    manifest = {
        "metadata": {
            "adapter_type": "snowflake"
        },
        "nodes": {},
        "sources": {
            "source.project_name.source_name.some_name_table_name": {
                "database": "DATA_BASE",
                "schema": "SOME_SCHEMA",
                "name": "some_name_table_name",
                "identifier": "TABLE_NAME",  # The actual table name
                "columns": {}
            }
        }
    }

    # Mock catalog to match the manifest
    catalog = {
        "nodes": {},
        "sources": {
            "source.project_name.source_name.some_name_table_name": {
                "metadata": {
                    "type": "BASE TABLE",
                    "schema": "SOME_SCHEMA",
                    "name": "TABLE_NAME",  # Note: this is the actual table name
                    "database": "DATA_BASE"
                },
                "columns": {}
            }
        }
    }

    # Patch the read_json method to return our mock manifest and catalog
    with patch('dbt_colibri.utils.json_utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]

        with patch.object(DbtColumnLineageExtractor, '_generate_schema_dict_from_catalog') as mock_schema:
            mock_schema.return_value = {}

            extractor = DbtColumnLineageExtractor(
                manifest_path="dummy_path",
                catalog_path="dummy_path",
            )

            # Get the node mapping
            node_mapping = extractor._get_dict_mapping_full_table_name_to_dbt_node()

            # The key should use the identifier (actual table name) not the source name
            expected_key = "data_base.some_schema.table_name"  # all lowercase as per implementation
            expected_value = "source.project_name.source_name.some_name_table_name"

            assert expected_key in node_mapping, f"Expected {expected_key} in node mapping but got keys: {list(node_mapping.keys())}"
            assert node_mapping[expected_key] == expected_value, f"Expected {expected_value} but got {node_mapping[expected_value]}"

            # Verify the source name is not used as the key
            incorrect_key = "data_base.some_schema.some_name_table_name"
            assert incorrect_key not in node_mapping, f"Found source name {incorrect_key} in mapping when it should use the identifier instead"


# Note: Adapter type validation tests are now above, testing auto-detection from manifest

