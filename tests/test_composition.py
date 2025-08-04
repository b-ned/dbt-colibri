"""
Tests for the composition-based approach between DbtColumnLineageExtractor and DbtColibriReportGenerator.
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator


class TestCompositionApproach:
    """Test the composition-based approach."""
    
    def test_report_generator_initialization(self):
        """Test that DbtColibriReportGenerator can be initialized with an extractor."""
        # Mock the extractor
        mock_extractor = Mock(spec=DbtColumnLineageExtractor)
        mock_extractor.manifest = {"nodes": {}, "sources": {}}
        mock_extractor.catalog = {"nodes": {}, "sources": {}}
        
        # Create report generator
        report_generator = DbtColibriReportGenerator(mock_extractor)
        
        # Verify composition
        assert report_generator.extractor is mock_extractor
        assert report_generator.manifest is mock_extractor.manifest
        assert report_generator.catalog is mock_extractor.catalog
    
    def test_detect_model_type(self):
        """Test model type detection logic."""
        mock_extractor = Mock(spec=DbtColumnLineageExtractor)
        mock_extractor.manifest = {"nodes": {}, "sources": {}}
        mock_extractor.catalog = {"nodes": {}, "sources": {}}
        
        report_generator = DbtColibriReportGenerator(mock_extractor)
        
        # Test different model naming patterns
        assert report_generator.detect_model_type("model.package.dim_customers") == "dimension"
        assert report_generator.detect_model_type("model.package.fact_orders") == "fact"
        assert report_generator.detect_model_type("model.package.int_daily_metrics") == "intermediate"
        assert report_generator.detect_model_type("model.package.stg_raw_data") == "staging"
        assert report_generator.detect_model_type("model.package.custom_model") == "unknown"
    
    def test_build_manifest_node_data(self):
        """Test manifest node data building."""
        mock_extractor = Mock(spec=DbtColumnLineageExtractor)
        mock_extractor.manifest = {
            "nodes": {
                "model.package.test_model": {
                    "resource_type": "model",
                    "raw_code": "SELECT * FROM source",
                    "compiled_code": "SELECT * FROM compiled_source",
                    "schema": "public",
                    "description": "Test model",
                    "config": {"contract": {"enforced": True}},
                    "refs": [{"name": "other_model"}],
                    "columns": {
                        "id": {"data_type": "integer", "description": "Primary key"}
                    }
                }
            },
            "sources": {}
        }
        mock_extractor.catalog = {
            "nodes": {
                "model.package.test_model": {
                    "columns": {
                        "id": {"type": "INTEGER"}
                    }
                }
            },
            "sources": {}
        }
        
        report_generator = DbtColibriReportGenerator(mock_extractor)
        
        node_data = report_generator.build_manifest_node_data("model.package.test_model")
        
        assert node_data["nodeType"] == "model"
        assert node_data["rawCode"] == "SELECT * FROM source"
        assert node_data["compiledCode"] == "SELECT * FROM compiled_source"
        assert node_data["schema"] == "public"
        assert node_data["description"] == "Test model"
        assert node_data["contractEnforced"] is True
        assert node_data["refs"] == [{"name": "other_model"}]
        assert "id" in node_data["columns"]
        assert node_data["columns"]["id"]["contractType"] == "integer"
        assert node_data["columns"]["id"]["dataType"] == "INTEGER"
    
    @patch('dbt_colibri.report.generator.Path')
    @patch('dbt_colibri.report.generator.inject_data_into_html')
    def test_generate_report(self, mock_inject_html, mock_path):
        """Test report generation with both JSON and HTML output."""
        # Mock the extractor and its lineage data
        mock_extractor = Mock(spec=DbtColumnLineageExtractor)
        mock_extractor.manifest = {"nodes": {}, "sources": {}}
        mock_extractor.catalog = {"nodes": {}, "sources": {}}
        mock_extractor.extract_project_lineage.return_value = {
            "lineage": {
                "parents": {},
                "children": {}
            }
        }
        
        report_generator = DbtColibriReportGenerator(mock_extractor)
        
        # Test report generation
        result = report_generator.generate_report(target_dir="test_dist")
        
        # Verify the new structure
        assert "metadata" in result
        assert "nodes" in result
        assert "lineage" in result
        assert "colibri_version" in result["metadata"]
        assert "generated_at" in result["metadata"]
        assert isinstance(result["nodes"], dict)  # Now a dictionary keyed by node_id
        assert "edges" in result["lineage"]
        assert "parents" in result["lineage"]
        assert "children" in result["lineage"]
        assert isinstance(result["lineage"]["edges"], list)
        
        # Verify extractor was called
        mock_extractor.extract_project_lineage.assert_called_once()
        
        # Verify HTML injection was called
        mock_inject_html.assert_called_once()
    
    def test_new_structure_format(self):
        """Test that the new structure format is correct."""
        # Mock the extractor with sample data
        mock_extractor = Mock(spec=DbtColumnLineageExtractor)
        mock_extractor.manifest = {
            "nodes": {
                "model.package.test_model": {
                    "resource_type": "model",
                    "raw_code": "SELECT id, name FROM source",
                    "compiled_code": "SELECT id, name FROM compiled_source",
                    "schema": "public",
                    "description": "Test model",
                    "config": {"contract": {"enforced": True}},
                    "refs": [{"name": "other_model"}],
                    "columns": {
                        "id": {"data_type": "integer", "description": "Primary key"},
                        "name": {"data_type": "string", "description": "Name"}
                    }
                }
            },
            "sources": {}
        }
        mock_extractor.catalog = {
            "nodes": {
                "model.package.test_model": {
                    "columns": {
                        "id": {"type": "INTEGER"},
                        "name": {"type": "VARCHAR"}
                    }
                }
            },
            "sources": {}
        }
        mock_extractor.extract_project_lineage.return_value = {
            "lineage": {
                "parents": {
                    "model.package.test_model": {
                        "id": [{"dbt_node": "model.package.source_model", "column": "id"}],
                        "name": [{"dbt_node": "model.package.source_model", "column": "name"}]
                    }
                },
                "children": {}
            }
        }
        
        report_generator = DbtColibriReportGenerator(mock_extractor)
        result = report_generator.build_full_lineage()
        
        # Verify the structure matches the expected format
        assert "metadata" in result
        assert "nodes" in result
        assert "lineage" in result
        
        # Check metadata
        assert "version" in result["metadata"]
        assert "generated_at" in result["metadata"]
        
        # Check nodes structure
        nodes = result["nodes"]
        assert "model.package.test_model" in nodes
        node = nodes["model.package.test_model"]
        assert node["id"] == "model.package.test_model"
        assert node["name"] == "test_model"
        assert node["fullName"] == "model.package.test_model"
        assert node["nodeType"] == "model"
        assert "columns" in node
        assert isinstance(node["columns"], dict)  # Keyed by column name
        
        # Check columns structure
        columns = node["columns"]
        assert "id" in columns
        assert "name" in columns
        assert columns["id"]["columnName"] == "id"
        assert columns["id"]["contractType"] == "integer"
        assert columns["id"]["dataType"] == "INTEGER"
        assert columns["id"]["hasLineage"] is True  # Should be True due to lineage
        
        # Check lineage structure
        lineage = result["lineage"]
        assert "edges" in lineage
        assert "parents" in lineage
        assert "children" in lineage
        assert isinstance(lineage["edges"], list)
        assert isinstance(lineage["parents"], dict)
        assert isinstance(lineage["children"], dict)


if __name__ == "__main__":
    pytest.main([__file__]) 