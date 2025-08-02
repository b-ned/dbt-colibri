import os
import json
import pytest
from unittest.mock import patch
from lineage_extractor.cli_direct import main as direct_main
from lineage_extractor.cli_recursive import main as recursive_main

@pytest.fixture
def test_data_dir():
    return os.path.join("tests", "test_data")

@pytest.fixture
def test_output_dir(tmp_path):
    """Create a temporary directory for test outputs"""
    return tmp_path

def test_cli_direct_basic(test_data_dir, test_output_dir):
    """Test basic functionality of direct CLI with default arguments"""
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = []
        mock_args.return_value.model_list_json = None
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.continue_on_error = False
        
        direct_main()
        
        # Check if output files were created
        assert os.path.exists(os.path.join(test_output_dir, "lineage_to_direct_parents.json"))
        assert os.path.exists(os.path.join(test_output_dir, "lineage_to_direct_children.json"))

def test_cli_direct_with_specific_models(test_data_dir, test_output_dir):
    """Test direct CLI with specific model selection"""
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = ["model.jaffle_shop.customers"]
        mock_args.return_value.model_list_json = None
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.continue_on_error = False
        
        direct_main()
        
        # Verify outputs contain only the specified model
        with open(os.path.join(test_output_dir, "lineage_to_direct_parents.json")) as f:
            parents = json.load(f)
            assert list(parents.keys())[0].startswith("model.jaffle_shop.customers")

def test_cli_recursive(test_data_dir, test_output_dir):
    """Test recursive CLI functionality"""
    # First run direct CLI to generate required input files
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = []
        mock_args.return_value.model_list_json = None
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.continue_on_error = False
        
        direct_main()
    
    # Then test recursive CLI
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.model = "model.jaffle_shop.customers"
        mock_args.return_value.column = "customer_id"
        mock_args.return_value.lineage_parents_file = os.path.join(test_output_dir, "lineage_to_direct_parents.json")
        mock_args.return_value.lineage_children_file = os.path.join(test_output_dir, "lineage_to_direct_children.json")
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.no_ui = True
        mock_args.return_value.output_format = "both"
        mock_args.return_value.show_details = False
        
        recursive_main()

def test_cli_direct_with_invalid_model_list(test_data_dir, test_output_dir):
    """Test direct CLI with invalid model list JSON"""
    invalid_model_list = os.path.join(test_output_dir, "invalid_models.json")
    with open(invalid_model_list, 'w') as f:
        json.dump({"not_a_list": "this should fail"}, f)
    
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = []
        mock_args.return_value.model_list_json = invalid_model_list
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.continue_on_error = False
        
        direct_main()  # Should handle the error gracefully

def test_cli_direct_with_model_list_json(test_data_dir, test_output_dir):
    """Test direct CLI with model list from JSON file"""
    model_list_path = os.path.join(test_output_dir, "model_list.json")
    models = ["model.jaffle_shop.customers"]
    
    # Create model list JSON file
    with open(model_list_path, 'w') as f:
        json.dump(models, f)
    
    with patch('argparse.ArgumentParser.parse_args') as mock_args:
        mock_args.return_value.manifest = os.path.join(test_data_dir, "inputs", "manifest.json")
        mock_args.return_value.catalog = os.path.join(test_data_dir, "inputs", "catalog.json")
        mock_args.return_value.dialect = "snowflake"
        mock_args.return_value.model = []  # Empty list as this should be overridden by model_list_json
        mock_args.return_value.model_list_json = model_list_path
        mock_args.return_value.output_dir = str(test_output_dir)
        mock_args.return_value.continue_on_error = False
        
        direct_main()
        
        # Verify outputs contain only the specified model
        with open(os.path.join(test_output_dir, "lineage_to_direct_parents.json")) as f:
            parents = json.load(f)
            assert list(parents.keys())[0].startswith("model.jaffle_shop.customers") 