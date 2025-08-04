import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timezone
from importlib.metadata import version, PackageNotFoundError

from ..lineage_extractor.extractor import DbtColumnLineageExtractor

import base64
import sys
import os


class DbtColibriReportGenerator:
    """
    Generates dbt-colibri report data from lineage extraction results.
    
    Uses composition with DbtColumnLineageExtractor to separate concerns:
    - Lineage extraction (DbtColumnLineageExtractor)
    - Report generation (DbtColibriReportGenerator)
    """
    
    def __init__(self, extractor: DbtColumnLineageExtractor):
        self.extractor = extractor
        self.manifest = extractor.manifest
        self.catalog = extractor.catalog
        self.colibri_version = self._get_colibri_version()
    
    def _get_colibri_version(self):
        try:
            return version("dbt-colibri")
        except PackageNotFoundError:
            return "unknown"

    def detect_model_type(self, node_id: str) -> str:
        """Detect model type based on naming conventions."""
        slug = node_id.split('.')[-1]
        if slug.startswith("dim_"): return "dimension"
        if slug.startswith("fact_"): return "fact"
        if slug.startswith("int_"): return "intermediate"
        if slug.startswith("stg_"): return "staging"
        return "unknown"
    
    def build_manifest_node_data(self, node_id: str) -> dict:
        """Build node metadata from manifest and catalog data."""
        node_data = (
            self.manifest.get("nodes", {}).get(node_id) or 
            self.manifest.get("sources", {}).get(node_id)
        )
        catalog_data = (
            self.catalog.get("nodes", {}).get(node_id) or 
            self.catalog.get("sources", {}).get(node_id)
        )

        columns = {}
        if node_data and node_data.get("columns"):
            for col, val in node_data["columns"].items():
                columns[col.lower()] = {
                    "contractType": val.get("data_type"),
                    "description": val.get("description"),
                }
        if catalog_data and catalog_data.get("columns"):
            for col, val in catalog_data["columns"].items():
                col = col.lower()
                if col not in columns:
                    columns[col] = {}
                columns[col]["dataType"] = val.get("type")

        return {
            "nodeType": node_data.get("resource_type", "unknown") if node_data else "unknown",
            "rawCode": node_data.get("raw_code") or node_data.get("raw_sql"),
            "compiledCode": node_data.get("compiled_code") or node_data.get("compiled_sql"),
            "schema": node_data.get("schema"),
            "description": node_data.get("description"),
            "contractEnforced": node_data.get("config", {}).get("contract", {}).get("enforced"),
            "refs": node_data.get("refs", []),
            "columns": columns,
        }
    
    def build_full_lineage(self) -> dict:
        """Build complete lineage report with nodes and edges in a human-readable structure."""
        # Extract lineage data from the extractor
        lineage_data = self.extractor.extract_project_lineage()
        parents_map = lineage_data["lineage"]["parents"]
        children_map = lineage_data["lineage"]["children"]

        # Build nodes dictionary (keyed by node_id for easy lookup)
        nodes: Dict[str, dict] = {}
        edges: List[dict] = []

        def ensure_node(node_id: str) -> dict:
            """Ensure a node exists in the nodes dict, creating if necessary."""
            if node_id not in nodes:
                meta = self.build_manifest_node_data(node_id)
                
                # Build columns dictionary (keyed by column name)
                columns_dict = {}
                for col_name, col_meta in meta["columns"].items():
                    columns_dict[col_name] = {
                        "columnName": col_name,
                        "hasLineage": False,  # Will be updated when we process lineage
                        **{k: v for k, v in col_meta.items() if v is not None}
                    }
                
                nodes[node_id] = {
                    "id": node_id,
                    "name": node_id.split(".")[-1],
                    "fullName": node_id,
                    "nodeType": meta["nodeType"],
                    "modelType": self.detect_model_type(node_id),
                    "schema": meta["schema"],
                    "description": meta["description"],
                    "contractEnforced": meta["contractEnforced"],
                    "rawCode": meta["rawCode"],
                    "compiledCode": meta["compiledCode"],
                    "refs": meta["refs"],
                    "columns": columns_dict,
                }
            return nodes[node_id]

        def add_edge(src_id: str, src_col: str, tgt_id: str, tgt_col: str):
            """Add an edge between two columns."""
            edges.append({
                "id": f"{src_id}:{src_col}->{tgt_id}:{tgt_col}",
                "source": src_id,
                "target": tgt_id,
                "sourceColumn": src_col,
                "targetColumn": tgt_col,
            })

        # Traverse all edges from parents_map
        for tgt_id, mapping in parents_map.items():
            for tgt_col, sources in mapping.items():
                for src in sources:
                    src_id, src_col = src["dbt_node"], src["column"]
                    
                    # Ensure both nodes exist
                    src_node = ensure_node(src_id)
                    tgt_node = ensure_node(tgt_id)
                    
                    # Update column lineage flags
                    if src_col in src_node["columns"]:
                        src_node["columns"][src_col]["hasLineage"] = True
                    if tgt_col in tgt_node["columns"]:
                        tgt_node["columns"][tgt_col]["hasLineage"] = True
                    
                    add_edge(src_id, src_col, tgt_id, tgt_col)

        # Traverse all refs to add model-level relationships
        for node_id, node_data in self.manifest.get("nodes", {}).items():
            if "refs" in node_data:
                for ref in node_data["refs"]:
                    ref_name = ref.get("name")
                    ref_node_id = next(
                        (id for id in self.manifest["nodes"] if id.endswith(f".{ref_name}")), 
                        None
                    )
                    if ref_node_id:
                        ensure_node(ref_node_id)
                        ensure_node(node_id)
                        edges.append({
                            "id": f"{ref_node_id}::->{node_id}::",
                            "source": ref_node_id,
                            "target": node_id,
                            "sourceColumn": "",
                            "targetColumn": "",
                        })

        # Build all nodes (even if disconnected)
        all_ids = set(self.manifest.get("nodes", {}).keys()).union(
            self.manifest.get("sources", {}).keys()
        )
        for node_id in all_ids:
            ensure_node(node_id)

        # Build the final structure
        return {
            "metadata": {
                "colibri_version": self.colibri_version,
                "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "adapter_type": self.manifest.get("metadata", {}).get("adapter_type"),
                "dbt_version": self.manifest.get("metadata", {}).get("dbt_version"),
                "dbt_schema_version": self.manifest.get("metadata", {}).get("dbt_schema_version"),
                "dbt_invocation_id": self.manifest.get("metadata", {}).get("invocation_id"),
                "dbt_project_name": self.manifest.get("metadata", {}).get("project_name"),
                "dbt_project_id": self.manifest.get("metadata", {}).get("project_id"),
            },
            "nodes": nodes,  # Dictionary keyed by node_id
            "lineage": {
                "edges": edges,
                "parents": parents_map,
                "children": children_map
            }
        }
    
    def generate_report(self, target_dir: str = "dist") -> dict:
        """
        Generate the complete dbt-colibri report with both JSON and HTML output.
        
        Args:
            target_dir: Directory to save both JSON and HTML files (default: "dist")
            
        Returns:
            dict: Complete report data
        """
        lineage = self.build_full_lineage()
        
        # Create target directory
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        
        # Save JSON data
        json_path = target_path / "colibri-manifest.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(lineage, f, indent=2)
        
        # Generate HTML with injected data
        html_template_path = Path(__file__).parent / "index.html"
        html_output_path = target_path / "index.html"
        
        # Inject data into HTML
        injected_html_path = inject_data_into_html(
            json_data_path=str(json_path),
            template_html_path=str(html_template_path),
            output_html_path=str(html_output_path)
        )
        
        return lineage



def inject_data_into_html(
    json_data_path: str,
    template_html_path: str = "dist/index.html",
    output_html_path: Optional[str] = None,
) -> str:
    """
    Inject JSON data into the compiled HTML file.
    
    Args:
        json_data_path: Path to the JSON data file
        template_html_path: Path to the compiled HTML template
        output_html_path: Path for the output HTML file (optional)
    
    Returns:
        Path to the generated HTML file
    """
    
    # Read the JSON data
    with open(json_data_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Read the template HTML
    with open(template_html_path, "r", encoding="utf-8") as f:
        template_html = f.read()
    
    # Convert data to JSON string and encode
    json_string = json.dumps(data, separators=(',', ':'))
    encoded_data = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
    
    # Create the script tag with the encoded data
    script_tag = f'<script>window.colibriData = JSON.parse(atob("{encoded_data}"));</script>'
    
    # Insert the script tag before the closing </head> tag
    if "</head>" in template_html:
        compiled_html = template_html.replace("</head>", f"{script_tag}\n</head>")
    else:
        # Fallback: insert at the beginning of the body
        compiled_html = template_html.replace("<body>", f"<body>\n{script_tag}")
    
    # Determine output path
    if output_html_path is None:
        output_dir = Path(template_html_path).parent
        output_html_path = output_dir / "index_with_data.html"
    
    # Write the compiled HTML
    with open(output_html_path, "w", encoding="utf-8") as f:
        f.write(compiled_html)
    
    return str(output_html_path)
