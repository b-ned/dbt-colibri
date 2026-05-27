import pytest
from unittest.mock import patch
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

    # This assertion is jaffle-shop-specific; skip fixtures that don't include it.
    if "model.jaffle_shop.locations" not in extractor.manifest.get("nodes", {}):
        pytest.skip("locations model not present in this fixture")

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


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
def test_quoted_column_with_spaces_rename_by_dialect(dialect):
    """The factory exercises each adapter's native quoting style for spaces."""
    source_columns = [
        ColumnDef("order item_id", "VARCHAR", quote=True),
        ColumnDef("order_id", "NUMBER"),
        ColumnDef("product_id", "VARCHAR"),
    ]
    model_columns = [
        ColumnDef("orderitem id", "VARCHAR", quote=True),
        ColumnDef("order_id", "NUMBER"),
        ColumnDef("product_id", "VARCHAR"),
    ]

    extractor = make_extractor(
        dialect,
        source_columns,
        model_columns=model_columns,
        model_column_sources={"orderitem id": "order item_id"},
    )
    parents = extractor.extract_project_lineage()["lineage"]["parents"]
    model_lineage = parents[MODEL_ID]

    assert model_lineage["orderitem id"] == [
        {
            "column": "order item_id",
            "dbt_node": SOURCE_ID,
            "lineage_type": "rename",
        }
    ]


def test_starrocks_double_quoted_column_with_spaces_has_lineage():
    """StarRocks SQL can contain double-quoted dbt identifiers with spaces."""
    raw_source_id = "source.test_project.raw.raw_items"
    first_model_id = "model.test_project.model_with_white_space_in_col"
    second_model_id = "model.test_project.renamed_white_space_columns"

    def manifest_columns(*cols):
        return {
            name: {
                "name": name,
                "data_type": "VARCHAR",
                "description": "",
                "tags": [],
                **({"quote": True} if quote else {}),
            }
            for name, quote in cols
        }

    def catalog_columns(*names):
        return {
            name: {
                "name": name,
                "type": "VARCHAR",
                "index": i,
                "comment": None,
            }
            for i, name in enumerate(names, 1)
        }

    manifest = {
        "metadata": {
            "adapter_type": "starrocks",
            "dbt_version": "1.11.6",
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "invocation_id": "test-fixture",
            "project_name": "test_project",
        },
        "nodes": {
            first_model_id: {
                "path": "models/model_with_white_space_in_col.sql",
                "original_file_path": "models/model_with_white_space_in_col.sql",
                "resource_type": "model",
                "compiled_code": """
                    with source as (
                        select * from `raw`.`raw_items`
                    ),
                    renamed as (
                        select
                            id as "order item_id",
                            order_id,
                            sku as product_id
                        from source
                    )
                    select * from renamed
                """,
                "raw_code": "select ...",
                "depends_on": {"nodes": [raw_source_id]},
                "database": "test_db",
                "schema": "jaffle_shop",
                "name": "model_with_white_space_in_col",
                "alias": "model_with_white_space_in_col",
                "columns": manifest_columns(
                    ("order item_id", True),
                    ("order_id", False),
                    ("product_id", False),
                ),
                "relation_name": "`jaffle_shop`.`model_with_white_space_in_col`",
                "config": {"materialized": "table"},
                "refs": [],
                "tags": [],
                "fqn": ["test_project", "model_with_white_space_in_col"],
            },
            second_model_id: {
                "path": "models/renamed_white_space_columns.sql",
                "original_file_path": "models/renamed_white_space_columns.sql",
                "resource_type": "model",
                "compiled_code": """
                    with source as (
                        select * from `jaffle_shop`.`model_with_white_space_in_col`
                    ),
                    renamed as (
                        select
                            "order item_id" as "orderitem id",
                            order_id,
                            product_id
                        from source
                    )
                    select * from renamed
                """,
                "raw_code": "select ...",
                "depends_on": {"nodes": [first_model_id]},
                "database": "test_db",
                "schema": "jaffle_shop",
                "name": "renamed_white_space_columns",
                "alias": "renamed_white_space_columns",
                "columns": manifest_columns(
                    ("orderitem id", True),
                    ("order_id", False),
                    ("product_id", False),
                ),
                "relation_name": "`jaffle_shop`.`renamed_white_space_columns`",
                "config": {"materialized": "table"},
                "refs": [],
                "tags": [],
                "fqn": ["test_project", "renamed_white_space_columns"],
            },
        },
        "sources": {
            raw_source_id: {
                "path": "models/sources.yml",
                "original_file_path": "models/sources.yml",
                "resource_type": "source",
                "database": "test_db",
                "schema": "raw",
                "name": "raw_items",
                "identifier": "raw_items",
                "columns": manifest_columns(
                    ("id", False),
                    ("order_id", False),
                    ("sku", False),
                ),
                "relation_name": "`raw`.`raw_items`",
                "config": {"materialized": None},
                "fqn": ["test_project", "raw", "raw_items"],
                "tags": [],
            },
        },
        "exposures": {},
        "parent_map": {
            first_model_id: [raw_source_id],
            second_model_id: [first_model_id],
            raw_source_id: [],
        },
        "child_map": {
            raw_source_id: [first_model_id],
            first_model_id: [second_model_id],
            second_model_id: [],
        },
    }
    catalog = {
        "nodes": {
            first_model_id: {
                "unique_id": first_model_id,
                "metadata": {
                    "database": "test_db",
                    "schema": "jaffle_shop",
                    "name": "model_with_white_space_in_col",
                    "type": "table",
                },
                "columns": catalog_columns("order item_id", "order_id", "product_id"),
            },
            second_model_id: {
                "unique_id": second_model_id,
                "metadata": {
                    "database": "test_db",
                    "schema": "jaffle_shop",
                    "name": "renamed_white_space_columns",
                    "type": "table",
                },
                "columns": catalog_columns("orderitem id", "order_id", "product_id"),
            },
        },
        "sources": {
            raw_source_id: {
                "unique_id": raw_source_id,
                "metadata": {
                    "database": "test_db",
                    "schema": "raw",
                    "name": "raw_items",
                    "type": "table",
                },
                "columns": catalog_columns("id", "order_id", "sku"),
            },
        },
    }

    with patch("dbt_colibri.utils.json_utils.read_json") as mock:
        mock.side_effect = [manifest, catalog]
        extractor = DbtColumnLineageExtractor(
            manifest_path="dummy",
            catalog_path="dummy",
        )

    lineage = extractor.extract_project_lineage()["lineage"]
    second_parents = lineage["parents"][second_model_id]

    assert second_parents["orderitem id"] == [
        {
            "column": "order item_id",
            "dbt_node": first_model_id,
            "lineage_type": "rename",
        }
    ]
    assert lineage["children"][first_model_id]["order item_id"] == [
        {"column": "orderitem id", "dbt_node": second_model_id}
    ]


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
