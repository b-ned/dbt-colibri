"""Tests for PIVOT column lineage support."""

import pytest
import json
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor


def create_pivot_test_manifest():
    """Create a minimal manifest with a PIVOT model for testing."""
    return {
        "metadata": {
            "adapter_type": "snowflake",
            "dbt_version": "1.9.0"
        },
        "nodes": {
            "model.test_project.base_data": {
                "unique_id": "model.test_project.base_data",
                "resource_type": "model",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "base_data",
                "alias": "base_data",
                "relation_name": "TEST_DB.TEST_SCHEMA.base_data",
                "columns": {
                    "location_id": {},
                    "value": {},
                    "category": {},
                    "date": {}
                },
                "config": {"materialized": "table"},
                "depends_on": {"nodes": []},
                "compiled_code": "SELECT location_id, value, category, date FROM source_table",
                "raw_code": "SELECT location_id, value, category, date FROM source_table",
                "path": "base_data.sql"
            },
            "model.test_project.pivot_model": {
                "unique_id": "model.test_project.pivot_model",
                "resource_type": "model",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "pivot_model",
                "alias": "pivot_model",
                "relation_name": "TEST_DB.TEST_SCHEMA.pivot_model",
                "columns": {
                    "location_id": {},
                    "date": {},
                    "sales": {},
                    "returns": {},
                    "profit": {}
                },
                "config": {"materialized": "table"},
                "depends_on": {"nodes": ["model.test_project.base_data"]},
                "compiled_code": """
                    WITH prep AS (
                        SELECT location_id, value, category, date
                        FROM TEST_DB.TEST_SCHEMA.base_data
                    )
                    SELECT 
                        location_id,
                        date,
                        sales,
                        returns,
                        profit
                    FROM prep
                    PIVOT (SUM(value) FOR category IN ('Sales', 'Returns', 'Profit'))
                """,
                "raw_code": """
                    WITH prep AS (
                        SELECT location_id, value, category, date
                        FROM {{ ref('base_data') }}
                    )
                    SELECT 
                        location_id,
                        date,
                        sales,
                        returns,
                        profit
                    FROM prep
                    PIVOT (SUM(value) FOR category IN ('Sales', 'Returns', 'Profit'))
                """,
                "path": "pivot_model.sql"
            }
        },
        "sources": {},
        "parent_map": {
            "model.test_project.pivot_model": ["model.test_project.base_data"],
            "model.test_project.base_data": []
        },
        "child_map": {
            "model.test_project.base_data": ["model.test_project.pivot_model"],
            "model.test_project.pivot_model": []
        }
    }


def create_pivot_test_catalog():
    """Create a minimal catalog for the PIVOT test."""
    return {
        "nodes": {
            "model.test_project.base_data": {
                "unique_id": "model.test_project.base_data",
                "metadata": {
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "name": "base_data"
                },
                "columns": {
                    "location_id": {"type": "VARCHAR"},
                    "value": {"type": "NUMBER"},
                    "category": {"type": "VARCHAR"},
                    "date": {"type": "DATE"}
                }
            },
            "model.test_project.pivot_model": {
                "unique_id": "model.test_project.pivot_model",
                "metadata": {
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "name": "pivot_model"
                },
                "columns": {
                    "location_id": {"type": "VARCHAR"},
                    "date": {"type": "DATE"},
                    "sales": {"type": "NUMBER"},
                    "returns": {"type": "NUMBER"},
                    "profit": {"type": "NUMBER"}
                }
            }
        },
        "sources": {}
    }


@pytest.fixture
def pivot_test_files(tmp_path):
    """Create temporary manifest and catalog files with PIVOT test data."""
    manifest = create_pivot_test_manifest()
    catalog = create_pivot_test_catalog()
    
    manifest_path = tmp_path / "manifest.json"
    catalog_path = tmp_path / "catalog.json"
    
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f)
    
    with open(catalog_path, 'w') as f:
        json.dump(catalog, f)
    
    return str(manifest_path), str(catalog_path)


def test_pivot_column_lineage_traces_through_cte(pivot_test_files):
    """Test that PIVOT columns trace back through CTEs to the actual source columns."""
    manifest_path, catalog_path = pivot_test_files
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )
    
    result = extractor.extract_project_lineage()
    parents = result['lineage']['parents']
    
    pivot_model = 'model.test_project.pivot_model'
    assert pivot_model in parents, "PIVOT model should be in lineage"
    
    pivot_columns = parents[pivot_model]
    
    # Test that pivoted columns (sales, returns, profit) trace back to base_data.value
    for pivoted_col in ['sales', 'returns', 'profit']:
        assert pivoted_col in pivot_columns, f"Column {pivoted_col} should have lineage"
        
        lineage = pivot_columns[pivoted_col]
        assert len(lineage) > 0, f"Column {pivoted_col} should have parent columns"
        
        # Should trace back to base_data.value (the column being summed in PIVOT)
        parent_nodes = {edge['dbt_node'] for edge in lineage}
        assert 'model.test_project.base_data' in parent_nodes, \
            f"Column {pivoted_col} should trace to base_data model"
        
        # Find the edge pointing to base_data
        base_edges = [e for e in lineage if e['dbt_node'] == 'model.test_project.base_data']
        parent_columns = {e['column'] for e in base_edges}
        
        # Should include 'value' (the aggregated column)
        assert 'value' in parent_columns, \
            f"Column {pivoted_col} should trace to base_data.value"
    
    # Test that pass-through columns also work correctly
    assert 'location_id' in pivot_columns, "location_id should have lineage"
    location_lineage = pivot_columns['location_id']
    assert len(location_lineage) > 0, "location_id should have parent columns"
    
    # location_id should trace to base_data.location_id
    location_parents = {e['dbt_node'] for e in location_lineage}
    assert 'model.test_project.base_data' in location_parents


def test_pivot_without_hardcoded_refs(pivot_test_files):
    """Test that PIVOT lineage doesn't generate _HARDCODED_REF___ entries."""
    manifest_path, catalog_path = pivot_test_files
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )
    
    result = extractor.extract_project_lineage()
    parents = result['lineage']['parents']
    
    pivot_model = 'model.test_project.pivot_model'
    pivot_columns = parents[pivot_model]
    
    # Check that no lineage edges point to _HARDCODED_REF___ or _NOT_FOUND___
    for col, lineage in pivot_columns.items():
        for edge in lineage:
            dbt_node = edge['dbt_node']
            assert not dbt_node.startswith('_HARDCODED_REF___'), \
                f"Column {col} has hardcoded ref: {dbt_node}"
            assert not dbt_node.startswith('_NOT_FOUND___'), \
                f"Column {col} has not found ref: {dbt_node}"


def test_pivot_lineage_type(pivot_test_files):
    """Test that PIVOT columns have appropriate lineage types."""
    manifest_path, catalog_path = pivot_test_files
    
    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )
    
    result = extractor.extract_project_lineage()
    parents = result['lineage']['parents']
    
    pivot_model = 'model.test_project.pivot_model'
    pivot_columns = parents[pivot_model]
    
    # Pivoted columns should have transformation lineage (SUM aggregation)
    for pivoted_col in ['sales', 'returns', 'profit']:
        lineage = pivot_columns[pivoted_col]
        lineage_types = {edge['lineage_type'] for edge in lineage}
        assert 'transformation' in lineage_types, \
            f"Column {pivoted_col} should have transformation lineage"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
