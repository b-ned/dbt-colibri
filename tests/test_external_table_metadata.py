"""Tests for Snowflake external table metadata pseudo-columns (issue #122).

Snowflake external tables expose METADATA$FILENAME / METADATA$FILE_ROW_NUMBER
pseudo-columns that are not present in the dbt catalog. Models that select them
must not crash lineage extraction with "Unknown column: METADATA$FILENAME".
"""

import pytest
import json
import logging
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor


def create_metadata_test_manifest():
    """Minimal manifest where a model selects external-table metadata columns."""
    return {
        "metadata": {
            "adapter_type": "snowflake",
            "dbt_version": "1.9.0"
        },
        "nodes": {
            "model.test_project.ext_source": {
                "unique_id": "model.test_project.ext_source",
                "resource_type": "model",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "ext_source",
                "alias": "ext_source",
                "relation_name": "TEST_DB.TEST_SCHEMA.ext_source",
                "columns": {"id": {}},
                "config": {"materialized": "table"},
                "depends_on": {"nodes": []},
                "compiled_code": "SELECT id FROM raw_external_table",
                "raw_code": "SELECT id FROM raw_external_table",
                "path": "ext_source.sql"
            },
            "model.test_project.metadata_model": {
                "unique_id": "model.test_project.metadata_model",
                "resource_type": "model",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "metadata_model",
                "alias": "metadata_model",
                "relation_name": "TEST_DB.TEST_SCHEMA.metadata_model",
                "columns": {
                    "id": {},
                    "filename": {},
                    "file_row_number": {}
                },
                "config": {"materialized": "table"},
                "depends_on": {"nodes": ["model.test_project.ext_source"]},
                "compiled_code": """
                    SELECT
                        ext_source.id,
                        ext_source.metadata$filename AS filename,
                        ext_source.metadata$file_row_number AS file_row_number
                    FROM TEST_DB.TEST_SCHEMA.ext_source AS ext_source
                """,
                "raw_code": """
                    SELECT
                        ext_source.id,
                        ext_source.metadata$filename AS filename,
                        ext_source.metadata$file_row_number AS file_row_number
                    FROM {{ ref('ext_source') }} AS ext_source
                """,
                "path": "metadata_model.sql"
            }
        },
        "sources": {},
        "parent_map": {
            "model.test_project.metadata_model": ["model.test_project.ext_source"],
            "model.test_project.ext_source": []
        },
        "child_map": {
            "model.test_project.ext_source": ["model.test_project.metadata_model"],
            "model.test_project.metadata_model": []
        }
    }


def create_metadata_test_catalog():
    """Catalog deliberately omits the METADATA$ pseudo-columns, as dbt does."""
    return {
        "nodes": {
            "model.test_project.ext_source": {
                "unique_id": "model.test_project.ext_source",
                "metadata": {
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "name": "ext_source"
                },
                "columns": {"id": {"type": "NUMBER"}}
            },
            "model.test_project.metadata_model": {
                "unique_id": "model.test_project.metadata_model",
                "metadata": {
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "name": "metadata_model"
                },
                "columns": {
                    "id": {"type": "NUMBER"},
                    "filename": {"type": "VARCHAR"},
                    "file_row_number": {"type": "NUMBER"}
                }
            }
        },
        "sources": {}
    }


@pytest.fixture
def metadata_test_files(tmp_path):
    manifest = create_metadata_test_manifest()
    catalog = create_metadata_test_catalog()

    manifest_path = tmp_path / "manifest.json"
    catalog_path = tmp_path / "catalog.json"

    with open(manifest_path, 'w') as f:
        json.dump(manifest, f)

    with open(catalog_path, 'w') as f:
        json.dump(catalog, f)

    return str(manifest_path), str(catalog_path)


def test_external_table_metadata_columns_do_not_crash(metadata_test_files):
    """Selecting METADATA$ pseudo-columns should not raise 'Unknown column'."""
    manifest_path, catalog_path = metadata_test_files

    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )

    result = extractor.extract_project_lineage()
    parents = result['lineage']['parents']

    model = 'model.test_project.metadata_model'
    assert model in parents, "metadata model should be in lineage"

    model_columns = parents[model]

    # The pass-through, real column still resolves to its source.
    assert 'id' in model_columns
    id_parents = {e['dbt_node'] for e in model_columns['id']}
    assert 'model.test_project.ext_source' in id_parents

    # The metadata pseudo-columns are present (lineage extraction did not crash).
    for meta_col in ['filename', 'file_row_number']:
        assert meta_col in model_columns, \
            f"Column {meta_col} should be present in lineage"


def test_external_table_metadata_columns_emit_warning(metadata_test_files, caplog):
    """A column absent from the catalog should be logged, not silently dropped."""
    manifest_path, catalog_path = metadata_test_files

    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )

    with caplog.at_level(logging.WARNING, logger="colibri"):
        extractor.extract_project_lineage()

    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("METADATA$FILENAME" in m and "not in the catalog" in m for m in warnings), \
        "Expected a warning for the unresolved METADATA$FILENAME column"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
