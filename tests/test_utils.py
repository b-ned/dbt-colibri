import os
import json
import pytest
import tempfile
from lineage_extractor.utils import (
    read_json, 
    write_dict_to_file, 
    read_dict_from_file,
    pretty_print_dict
)
from unittest.mock import patch, MagicMock

@pytest.fixture
def temp_file():
    """Create a temporary file for test outputs"""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmpfile:
        yield tmpfile.name
    # Clean up file after test
    if os.path.exists(tmpfile.name):
        os.unlink(tmpfile.name)

def test_read_json_valid():
    """Test reading a valid JSON file."""
    # Get path to test manifest file
    manifest_path = os.path.join("tests", "test_data", "inputs", "manifest.json")
    
    # Read the JSON
    data = read_json(manifest_path)
    
    # Verify it's a valid dict
    assert data
    assert isinstance(data, dict)
    assert "nodes" in data
    assert "sources" in data

def test_read_json_nonexistent_file():
    """Test reading a non-existent JSON file."""
    with pytest.raises(FileNotFoundError):
        read_json("nonexistent_file.json")

def test_read_json_invalid_json():
    """Test reading an invalid JSON file."""
    # Create a temporary file with invalid JSON
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmpfile:
        tmpfile.write(b"This is not JSON")
    
    # Try to read it
    with pytest.raises(json.JSONDecodeError):
        read_json(tmpfile.name)
    
    # Clean up
    os.unlink(tmpfile.name)

def test_write_and_read_dict(temp_file):
    """Test writing and reading a dictionary to/from a file."""
    # Create a test dictionary
    test_dict = {
        "model": "test_model",
        "columns": {
            "id": "integer",
            "name": "string"
        },
        "nested": {
            "level1": {
                "level2": "value"
            }
        }
    }
    
    # Write the dictionary to a file
    write_dict_to_file(test_dict, temp_file)
    
    # Verify the file exists
    assert os.path.exists(temp_file)
    
    # Read the dictionary back
    read_dict = read_dict_from_file(temp_file)
    
    # Verify it's the same
    assert read_dict == test_dict
    assert read_dict["model"] == "test_model"
    assert read_dict["columns"]["id"] == "integer"
    assert read_dict["nested"]["level1"]["level2"] == "value"

def test_pretty_print_dict(capsys):
    """Test pretty printing a dictionary."""
    # Create a test dictionary
    test_dict = {
        "key1": "value1",
        "key2": {
            "nested_key": "nested_value"
        }
    }
    
    # Pretty print it
    pretty_print_dict(test_dict)
    
    # Capture the output
    captured = capsys.readouterr()
    
    # Verify the output contains the keys and values
    assert "key1" in captured.out
    assert "value1" in captured.out
    assert "key2" in captured.out
    assert "nested_key" in captured.out
    assert "nested_value" in captured.out

@patch('os.system')
def test_clear_screen(mock_system):
    """Test the clear screen function."""
    # Import the function locally to avoid early execution
    from lineage_extractor.utils import clear_screen
    
    # Call the function
    clear_screen()
    
    # Verify os.system was called with the correct command
    if os.name == "nt":
        mock_system.assert_called_once_with("cls")
    else:
        mock_system.assert_called_once_with("clear")

def test_error_handling_read_dict_from_file():
    """Test error handling when reading dict from non-existent file."""
    with pytest.raises(FileNotFoundError):
        read_dict_from_file("non_existent_file.json")

def test_error_handling_read_dict_from_invalid_file():
    """Test error handling when reading dict from invalid JSON file."""
    # Create a file with invalid JSON
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmpfile:
        tmpfile.write(b"This is not JSON")
    
    # Try to read it
    with pytest.raises(json.JSONDecodeError):
        read_dict_from_file(tmpfile.name)
    
    # Clean up
    os.unlink(tmpfile.name)

def test_write_dict_to_file_permissions(temp_file):
    """Test writing dict to file with insufficient permissions."""
    # Create a test dictionary
    test_dict = {"key": "value"}
    
    # Make the file read-only (not writable)
    os.chmod(temp_file, 0o444)
    
    # Try to write to it
    with pytest.raises(PermissionError):
        write_dict_to_file(test_dict, temp_file)
    
    # Reset permissions for cleanup
    os.chmod(temp_file, 0o666) 