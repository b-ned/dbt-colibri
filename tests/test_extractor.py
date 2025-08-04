import pytest
import os
import json
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from unittest.mock import patch, MagicMock
from sqlglot.lineage import SqlglotError

def test_extractor_initialization():
    """Test that the extractor can be initialized with valid parameters."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=[],
        dialect="snowflake"
    )
    assert isinstance(extractor, DbtColumnLineageExtractor)
    assert extractor.dialect == "snowflake"
    # When selected_models is empty, it automatically selects all models from manifest
    assert len(extractor.selected_models) > 0
    assert all(model.startswith("model.") for model in extractor.selected_models)

def test_extractor_with_specific_models():
    """Test that the extractor can be initialized with specific models."""
    specific_models = ["model.jaffle_shop.customers"]
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=specific_models,
        dialect="snowflake"
    )
    assert extractor.selected_models == specific_models

def test_dialect_support():
    """Test that the extractor supports different dialects."""
    # Test with supported dialects
    for dialect in ["snowflake", "postgres", "bigquery", "mysql"]:
        extractor = DbtColumnLineageExtractor(
            manifest_path="tests/test_data/inputs/manifest.json",
            catalog_path="tests/test_data/inputs/catalog.json",
            selected_models=[],
            dialect=dialect
        )
        assert extractor.dialect == dialect

def test_schema_dict_generation():
    """Test schema dictionary generation from catalog."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=["model.jaffle_shop.customers"],
        dialect="snowflake"
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

def test_node_mapping():
    """Test the node mapping dictionary generation."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=["model.jaffle_shop.customers"],
        dialect="snowflake"
    )
    
    # Verify node_mapping structure
    assert extractor.node_mapping
    assert isinstance(extractor.node_mapping, dict)
    assert len(extractor.node_mapping) > 0
    
    # Verify some mappings exist (format should be "catalog.schema.table" -> "model.package.name")
    for table_name, dbt_node in extractor.node_mapping.items():
        assert "." in table_name
        assert dbt_node.startswith(("model.", "source.", "seed."))

def test_get_list_of_columns():
    """Test retrieving columns for a dbt node."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
    )
    
    # Try with a known model
    model_node = "model.jaffle_shop.customers"
    columns = extractor._get_list_of_columns_for_a_dbt_node(model_node)
    
    # Verify columns were returned
    assert columns
    assert isinstance(columns, list)
    assert len(columns) > 0
    assert "customer_id" in columns  # This assumes customer_id exists in the model
    
    # Test with a non-existent node
    with pytest.warns(UserWarning):
        no_columns = extractor._get_list_of_columns_for_a_dbt_node("model.does_not_exist")
        assert no_columns == []

def test_get_parent_nodes_catalog():
    """Test getting parent nodes catalog."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
    )
    
    # Get a model that has dependencies
    model_node = "model.jaffle_shop.customers"
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

@patch('dbt_colibri.lineage_extractor.extractor.lineage')
def test_extract_lineage_for_model(mock_lineage):
    """Test extracting lineage for a model."""
    # Mock the lineage function to return a predictable result
    mock_lineage.return_value = [MagicMock()]
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
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

def test_extract_lineage_with_real_data():
    """Test extracting lineage for a model using actual test data."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
    )
    
    # Get a real model from the manifest
    model_node = "model.jaffle_shop.customers"
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
def test_extract_lineage_error_handling(mock_lineage):
    """Test error handling during lineage extraction."""
    # Mock the lineage function to raise an error
    mock_lineage.side_effect = SqlglotError("Test error")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
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

def test_full_lineage_map_build():
    """Test building the complete lineage map for selected models."""
    # Use a subset of models for faster testing
    selected_models = ["model.jaffle_shop.customers"]
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=selected_models,
        dialect="snowflake"
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
    
    # Verify that processed columns have some lineage information
    # For our test model, we expect at least one column to have lineage data
    has_lineage = False
    for column, lineage_data in model_columns.items():
        if lineage_data:  # If not empty
            has_lineage = True
            break
    
    assert has_lineage, "No lineage information found for any column"

def test_get_dbt_node_from_sqlglot_table_node():
    """Test converting sqlglot table nodes to dbt nodes."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
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

def test_get_columns_lineage_from_sqlglot_lineage_map():
    """Test extracting column lineage from the sqlglot lineage map."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=["model.test.child"],
        dialect="snowflake"
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
    
    # Verify the result
    assert columns_lineage
    assert "model.test.child" in columns_lineage
    assert "id" in columns_lineage["model.test.child"]
    assert len(columns_lineage["model.test.child"]["id"]) == 1
    assert columns_lineage["model.test.child"]["id"][0]["dbt_node"] == "model.test.parent"
    assert columns_lineage["model.test.child"]["id"][0]["column"] == "id"

def test_column_lineage_with_real_data():
    """Test the full column lineage extraction process with real data."""
    # Use a real model from test data
    selected_models = ["model.jaffle_shop.customers"]
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=selected_models,
        dialect="snowflake"
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
    
    # The customer model includes data from stg_customers, stg_orders, and stg_payments
    # Verify that at least one column has parent nodes
    has_parents = False
    for column, parents in model_columns.items():
        if parents:  # If not empty
            has_parents = True
            # Verify parent format
            for parent in parents:
                assert "column" in parent
                assert "dbt_node" in parent
                # In this example, it should reference one of the staging models
                assert parent["dbt_node"].startswith(("model.jaffle_shop.stg_", "source."))
            break
    
    assert has_parents, "No parent information found for any column"

def test_get_lineage_to_direct_children():
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
    
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        dialect="snowflake"
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
    with patch('dbt_colibri.lineage_extractor.utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]
        
        with patch.object(DbtColumnLineageExtractor, '_generate_schema_dict_from_catalog') as mock_schema:
            mock_schema.return_value = {}
            
            with patch.object(DbtColumnLineageExtractor, '_get_dict_mapping_full_table_name_to_dbt_node') as mock_mapping:
                mock_mapping.return_value = {}
                
                extractor = DbtColumnLineageExtractor(
                    manifest_path="dummy_path",
                    catalog_path="dummy_path",
                    selected_models=["model.test.python_model"],
                    dialect="snowflake"
                )
                
                # Build the lineage map
                lineage_map = extractor.build_lineage_map()
                
                # Verify that the Python model was skipped
                assert lineage_map == {}

def test_non_model_resource_handling():
    """Test handling of non-model resources during lineage map building."""
    # Create a mock manifest with a non-model resource
    manifest = {
        "nodes": {
            "snapshot.test.snapshot_model": {
                "path": "snapshots/snapshot_model.sql",
                "resource_type": "snapshot",
                "compiled_code": "SELECT * FROM source",
                "depends_on": {"nodes": []},
                "database": "test_db",
                "schema": "test_schema",
                "name": "snapshot_model",
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
    with patch('dbt_colibri.lineage_extractor.utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]
        
        with patch.object(DbtColumnLineageExtractor, '_generate_schema_dict_from_catalog') as mock_schema:
            mock_schema.return_value = {}
            
            with patch.object(DbtColumnLineageExtractor, '_get_dict_mapping_full_table_name_to_dbt_node') as mock_mapping:
                mock_mapping.return_value = {}
                
                extractor = DbtColumnLineageExtractor(
                    manifest_path="dummy_path",
                    catalog_path="dummy_path",
                    selected_models=["snapshot.test.snapshot_model"],
                    dialect="snowflake"
                )
                
                # Build the lineage map
                lineage_map = extractor.build_lineage_map()
                
                # Verify that the non-model resource was skipped
                assert lineage_map == {}


def test_source_identifier_handling():
    """Test that source identifiers are correctly handled in node mapping."""
    # Create a mock manifest with a source that has an identifier
    manifest = {
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
    with patch('dbt_colibri.lineage_extractor.utils.read_json') as mock_read_json:
        mock_read_json.side_effect = [manifest, catalog]
        
        with patch.object(DbtColumnLineageExtractor, '_generate_schema_dict_from_catalog') as mock_schema:
            mock_schema.return_value = {}
            
            extractor = DbtColumnLineageExtractor(
                manifest_path="dummy_path",
                catalog_path="dummy_path",
                dialect="snowflake"
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

