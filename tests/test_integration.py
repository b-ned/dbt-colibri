import os
import json
import pytest
import tempfile
from lineage_extractor import DbtColumnLineageExtractor
from lineage_extractor.cli_direct import main as direct_main
from lineage_extractor.cli_recursive import main as recursive_main
from unittest.mock import patch

@pytest.fixture
def test_data_dir():
    return os.path.join("tests", "test_data")

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

def test_end_to_end_workflow(test_data_dir, temp_output_dir):
    """Test the full workflow from extractor initialization to recursive lineage."""
    
    # Step 1: Initialize the extractor
    extractor = DbtColumnLineageExtractor(
        manifest_path=os.path.join(test_data_dir, "inputs", "manifest.json"),
        catalog_path=os.path.join(test_data_dir, "inputs", "catalog.json"),
        selected_models=["model.jaffle_shop.customers", "model.jaffle_shop.orders"],
        dialect="snowflake"
    )
    
    # Step 2: Build the lineage map
    lineage_map = extractor.build_lineage_map()
    assert lineage_map
    assert "model.jaffle_shop.customers" in lineage_map
    assert "model.jaffle_shop.orders" in lineage_map
    
    # Step 3: Get columns lineage from the sqlglot lineage map
    columns_lineage = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
    assert columns_lineage
    assert "model.jaffle_shop.customers" in columns_lineage
    assert "model.jaffle_shop.orders" in columns_lineage
    
    # Step 4: Get lineage to direct children
    children_lineage = extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(columns_lineage)
    assert children_lineage
    
    # Step 5: Write outputs to files
    parents_file = os.path.join(temp_output_dir, "lineage_to_direct_parents.json")
    children_file = os.path.join(temp_output_dir, "lineage_to_direct_children.json")
    
    with open(parents_file, "w") as f:
        json.dump(columns_lineage, f, indent=4)
    
    with open(children_file, "w") as f:
        json.dump(children_lineage, f, indent=4)
    
    # Verify files were created
    assert os.path.exists(parents_file)
    assert os.path.exists(children_file)
    
    # Step 6: Find recursive lineage for a specific column
    model = "model.jaffle_shop.customers"
    column = "customer_id"
    
    # Find all ancestors with structure
    ancestors = DbtColumnLineageExtractor.find_all_related_with_structure(
        columns_lineage, model, column
    )
    assert ancestors or ancestors == {}  # Could be empty if no ancestors
    
    # Find all descendants with structure
    descendants = DbtColumnLineageExtractor.find_all_related_with_structure(
        children_lineage, model, column
    )
    assert descendants or descendants == {}  # Could be empty if no descendants
    
    # Step 7: Serialize and verify the output
    ancestors_file = os.path.join(temp_output_dir, f"{model}_{column}_ancestors.json")
    descendants_file = os.path.join(temp_output_dir, f"{model}_{column}_descendants.json")
    
    with open(ancestors_file, "w") as f:
        json.dump(ancestors, f, indent=4)
    
    with open(descendants_file, "w") as f:
        json.dump(descendants, f, indent=4)
    
    # Verify files were created
    assert os.path.exists(ancestors_file)
    assert os.path.exists(descendants_file)

def test_cli_direct_to_recursive_workflow(test_data_dir, temp_output_dir):
    """Test the complete CLI workflow from direct to recursive lineage."""
    
    # Mock CLI arguments for direct lineage
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = ["model.jaffle_shop.customers", "model.jaffle_shop.orders"]
        mock_args.return_value.model_list_json = None
        mock_args.return_value.output_dir = temp_output_dir
        mock_args.return_value.show_ui = False
        
        # Run direct lineage CLI
        direct_main()
        
        # Verify output files exist
        parents_file = os.path.join(temp_output_dir, "lineage_to_direct_parents.json")
        children_file = os.path.join(temp_output_dir, "lineage_to_direct_children.json")
        assert os.path.exists(parents_file)
        assert os.path.exists(children_file)
        
        # Mock CLI arguments for recursive lineage
        with patch('argparse.ArgumentParser.parse_args') as mock_args_recursive:
            mock_args_recursive.return_value.model = "model.jaffle_shop.customers"
            mock_args_recursive.return_value.column = "customer_id"
            mock_args_recursive.return_value.lineage_parents_file = parents_file
            mock_args_recursive.return_value.lineage_children_file = children_file
            mock_args_recursive.return_value.output_dir = temp_output_dir
            mock_args_recursive.return_value.show_ui = False
            mock_args_recursive.return_value.output_format = 'json'
            mock_args_recursive.return_value.show_details = False
            
            # Run recursive lineage CLI
            recursive_main()
            
            # Verify recursive output files exist
            model = "model.jaffle_shop.customers".replace('.', '_')
            column = "customer_id"
            ancestors_file = os.path.join(temp_output_dir, f"{model}_{column}_ancestors.json")
            descendants_file = os.path.join(temp_output_dir, f"{model}_{column}_descendants.json")
            
            assert os.path.exists(ancestors_file)
            assert os.path.exists(descendants_file)
            
            # Verify content of output files
            with open(ancestors_file, "r") as f:
                ancestors = json.load(f)
                assert isinstance(ancestors, dict)
            
            with open(descendants_file, "r") as f:
                descendants = json.load(f)
                assert isinstance(descendants, dict)

def test_complex_model_relationships(test_data_dir, temp_output_dir):
    """Test more complex model relationships and lineage extraction."""
    
    # Initialize extractor with all available models
    extractor = DbtColumnLineageExtractor(
        manifest_path=os.path.join(test_data_dir, "inputs", "manifest.json"),
        catalog_path=os.path.join(test_data_dir, "inputs", "catalog.json"),
        selected_models=[],  # Select all models
        dialect="snowflake"
    )
    
    # Build the lineage map
    lineage_map = extractor.build_lineage_map()
    
    # Get columns lineage
    columns_lineage = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
    
    # Get children lineage
    children_lineage = extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(columns_lineage)
    
    # Write outputs
    parents_file = os.path.join(temp_output_dir, "full_lineage_to_direct_parents.json")
    children_file = os.path.join(temp_output_dir, "full_lineage_to_direct_children.json")
    
    with open(parents_file, "w") as f:
        json.dump(columns_lineage, f, indent=4)
    
    with open(children_file, "w") as f:
        json.dump(children_lineage, f, indent=4)
    
    # Verify the files exist and have content
    assert os.path.exists(parents_file)
    assert os.path.exists(children_file)
    
    # Check file sizes to ensure they have meaningful content
    assert os.path.getsize(parents_file) > 100
    assert os.path.getsize(children_file) > 100
    
    # Test finding all related columns for a "leaf" model
    # Find a leaf model (one that has no children)
    leaf_models = []
    for model in columns_lineage:
        if model not in children_lineage:
            leaf_models.append(model)
    
    # If we found leaf models, test one
    if leaf_models:
        leaf_model = leaf_models[0]
        
        # Find the first column
        first_column = next(iter(columns_lineage[leaf_model]))
        
        # Find all ancestors
        ancestors = DbtColumnLineageExtractor.find_all_related_with_structure(
            columns_lineage, leaf_model, first_column
        )
        
        # Verify ancestors structure
        assert isinstance(ancestors, dict)
        
        # Write ancestors to file
        ancestors_file = os.path.join(temp_output_dir, f"{leaf_model}_{first_column}_ancestors.json")
        with open(ancestors_file, "w") as f:
            json.dump(ancestors, f, indent=4)
        
        assert os.path.exists(ancestors_file)
    
    # Test finding all related columns for a "root" model
    # Find a root model (one that has no parents)
    root_models = []
    for model in children_lineage:
        is_root = True
        for parent_model in columns_lineage:
            for column in columns_lineage[parent_model]:
                for parent in columns_lineage[parent_model][column]:
                    if parent["dbt_node"] == model:
                        is_root = False
                        break
                if not is_root:
                    break
            if not is_root:
                break
        if is_root:
            root_models.append(model)
    
    # If we found root models, test one
    if root_models:
        root_model = root_models[0]
        
        # Find the first column
        if root_model in children_lineage:
            first_column = next(iter(children_lineage[root_model]))
            
            # Find all descendants
            descendants = DbtColumnLineageExtractor.find_all_related_with_structure(
                children_lineage, root_model, first_column
            )
            
            # Verify descendants structure
            assert isinstance(descendants, dict)
            
            # Write descendants to file
            descendants_file = os.path.join(temp_output_dir, f"{root_model}_{first_column}_descendants.json")
            with open(descendants_file, "w") as f:
                json.dump(descendants, f, indent=4)
            
            assert os.path.exists(descendants_file) 