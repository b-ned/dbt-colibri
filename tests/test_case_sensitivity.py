import pytest
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor

def test_locations_column_lineage_case_sensitivity(dbt_valid_test_data_dir):
    """Test that model.jaffle_shop.locations has proper column lineage with correct case sensitivity."""
    if dbt_valid_test_data_dir is None:
        pytest.skip("No valid versioned test data present")
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=f"{dbt_valid_test_data_dir}/manifest.json",
        catalog_path=f"{dbt_valid_test_data_dir}/catalog.json"
    )
    
    # Extract the project lineage
    result = extractor.extract_project_lineage()
    
    # Get the lineage for model.jaffle_shop.locations
    locations_lineage = result['lineage']['parents']['model.jaffle_shop.locations']
    
    # Print the actual column lineage for debugging
    print("\nActual column lineage for model.jaffle_shop.locations:")
    for column_name, lineage_data in locations_lineage.items():
        print(f"  '{column_name}': {lineage_data}")
    print()
    
    # Expected columns (we care about having specific column names, not wildcards)
    expected_column_names = {'location_id', 'location_name', 'tax_rate', 'opened_date'}
    
    # Assert that all expected columns are present
    actual_column_names = set(locations_lineage.keys())
    assert actual_column_names == expected_column_names, \
        f"Expected columns {expected_column_names}, but got {actual_column_names}"
    
    # Assert each column has proper lineage (not wildcards) and reasonable structure
    for column_name in expected_column_names:
        assert column_name in locations_lineage, f"Column '{column_name}' not found in lineage"
        actual_lineage = locations_lineage[column_name]
        
        # Ensure we have lineage data (not empty)
        assert len(actual_lineage) > 0, f"Column '{column_name}' has empty lineage"
        
        # Check each lineage entry
        for lineage_entry in actual_lineage:
            # Ensure we have the correct column name (no wildcards like '*')
            assert lineage_entry['column'] == column_name, \
                f"Column '{column_name}' lineage points to wrong column: {lineage_entry['column']}"
            
            # Ensure we don't have wildcard columns
            assert lineage_entry['column'] != '*', \
                f"Column '{column_name}' has wildcard lineage - this indicates case sensitivity issues"
            
            # Ensure dbt_node points to a staging model (case insensitive check)
            dbt_node = lineage_entry['dbt_node'].lower()
            assert 'model.jaffle_shop.stg_locations' in dbt_node, \
                f"Column '{column_name}' should trace back to stg_locations model, got: {lineage_entry['dbt_node']}"
    
    # Additional assertion to ensure we have exactly 4 columns
    assert len(locations_lineage) == 4, \
        f"Expected 4 columns in locations lineage, but got {len(locations_lineage)}"
    
    print(f"✓ Column lineage test passed for model.jaffle_shop.locations with {len(locations_lineage)} columns")