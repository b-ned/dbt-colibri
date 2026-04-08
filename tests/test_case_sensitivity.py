import pytest
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator
from dbt_test_factory import (
    ColumnDef,
    CASE_SENSITIVE_QUOTE_DIALECTS,
    DIALECTS,
    make_extractor,
)

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
        
        if extractor.dialect == "oracle":
            # Oracle may legitimately return empty lineage
            assert isinstance(actual_lineage, list), f"Column '{column_name}' lineage should be a list, got: {type(actual_lineage)}"
        else:
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


# ---------------------------------------------------------------------------
# Cross-dialect quoted-column tests using the synthetic fixture factory
# ---------------------------------------------------------------------------

COLUMNS = [
    ColumnDef("quotedMixedCase", "VARCHAR", quote=True),
    ColumnDef("normal_col", "NUMBER"),
]

MODEL_ID = "model.test_project.my_model"
SOURCE_ID = "source.test_project.raw.source_table"


@pytest.mark.parametrize("dialect", sorted(CASE_SENSITIVE_QUOTE_DIALECTS))
class TestQuotedColumnLineageByDialect:
    """
    Verify that quoted columns preserve casing through the entire pipeline
    for every dialect where quoting is case-sensitive.
    """

    def test_extract_lineage_preserves_quoted_column_key(self, dialect):
        """The parents map should use the original-case column name as the key."""
        extractor = make_extractor(dialect, COLUMNS)
        parents = extractor.extract_project_lineage()["lineage"]["parents"]

        model_lineage = parents[MODEL_ID]
        assert "quotedMixedCase" in model_lineage, \
            f"[{dialect}] Expected 'quotedMixedCase' in parents, got: {list(model_lineage.keys())}"
        assert "quotedmixedcase" not in model_lineage, \
            f"[{dialect}] Lowercased key should not exist"

    def test_extract_lineage_parent_column_preserves_case(self, dialect):
        """The parent entry's column name should preserve original casing."""
        extractor = make_extractor(dialect, COLUMNS)
        parents = extractor.extract_project_lineage()["lineage"]["parents"]

        entries = parents[MODEL_ID]["quotedMixedCase"]
        assert len(entries) > 0, f"[{dialect}] No lineage for quoted column"
        assert entries[0]["column"] == "quotedMixedCase", \
            f"[{dialect}] Parent column should be 'quotedMixedCase', got: {entries[0]['column']!r}"
        assert entries[0]["dbt_node"] == SOURCE_ID

    def test_children_map_preserves_quoted_column_key(self, dialect):
        """The children map entry for the source should use original-case column."""
        extractor = make_extractor(dialect, COLUMNS)
        children = extractor.extract_project_lineage()["lineage"]["children"]

        src_children = children.get(SOURCE_ID, {})
        assert "quotedMixedCase" in src_children, \
            f"[{dialect}] Expected 'quotedMixedCase' in children, got: {list(src_children.keys())}"

    def test_unquoted_column_still_lowercased(self, dialect):
        """Unquoted columns should still be lowercased as before."""
        extractor = make_extractor(dialect, COLUMNS)
        parents = extractor.extract_project_lineage()["lineage"]["parents"]

        model_lineage = parents[MODEL_ID]
        assert "normal_col" in model_lineage, \
            f"[{dialect}] Expected 'normal_col' in parents, got: {list(model_lineage.keys())}"

    def test_report_output_preserves_quoted_columns(self, dialect):
        """build_full_lineage output should have correct column names and quote flags."""
        extractor = make_extractor(dialect, COLUMNS)
        generator = DbtColibriReportGenerator(extractor)
        result = generator.build_full_lineage()

        model_node = result["nodes"][MODEL_ID]
        assert "quotedMixedCase" in model_node["columns"], \
            f"[{dialect}] Expected 'quotedMixedCase' in output columns, got: {list(model_node['columns'].keys())}"
        assert model_node["columns"]["quotedMixedCase"].get("quote") is True
        assert model_node["columns"]["quotedMixedCase"].get("hasLineage") is True

    def test_report_edges_reference_correct_column_name(self, dialect):
        """Lineage edges should use the original-case column name."""
        extractor = make_extractor(dialect, COLUMNS)
        generator = DbtColibriReportGenerator(extractor)
        result = generator.build_full_lineage()

        edges = [
            e for e in result["lineage"]["edges"]
            if e["target"] == MODEL_ID and e["targetColumn"] == "quotedMixedCase"
        ]
        assert len(edges) > 0, f"[{dialect}] No edge targeting 'quotedMixedCase'"
        assert edges[0]["sourceColumn"] == "quotedMixedCase"

    def test_find_all_related_with_quoted_columns(self, dialect):
        """find_all_related should handle mixed-case keys via case-insensitive lookup."""
        extractor = make_extractor(dialect, COLUMNS)
        lineage_data = extractor.extract_project_lineage()

        # Build a lineage_map in the format find_all_related expects
        lineage_map = lineage_data["lineage"]["parents"]

        # Should work with the exact-case column name
        related = DbtColumnLineageExtractor.find_all_related(
            lineage_map, MODEL_ID, "quotedMixedCase"
        )
        assert SOURCE_ID.lower() in related or SOURCE_ID in related, \
            f"[{dialect}] find_all_related should trace to source, got: {related}"

    def test_find_all_related_with_structure_quoted_columns(self, dialect):
        """find_all_related_with_structure should handle mixed-case keys."""
        extractor = make_extractor(dialect, COLUMNS)
        lineage_map = extractor.extract_project_lineage()["lineage"]["parents"]

        structure = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_map, MODEL_ID, "quotedMixedCase"
        )
        assert len(structure) > 0, \
            f"[{dialect}] Expected non-empty structure, got: {structure}"


@pytest.mark.parametrize("dialect", sorted(set(DIALECTS.keys()) - CASE_SENSITIVE_QUOTE_DIALECTS))
class TestCaseInsensitiveDialects:
    """
    For dialects where quoting doesn't preserve case (BigQuery, DuckDB, etc.),
    quoted columns should be lowercased like everything else.
    """

    def test_quoted_column_is_lowercased(self, dialect):
        """On case-insensitive dialects, even quoted columns appear lowercase."""
        extractor = make_extractor(dialect, COLUMNS)
        parents = extractor.extract_project_lineage()["lineage"]["parents"]

        model_lineage = parents.get(MODEL_ID, {})
        # The column key should be lowercased
        col_keys = list(model_lineage.keys())
        assert all(k == k.lower() for k in col_keys), \
            f"[{dialect}] All column keys should be lowercase, got: {col_keys}"