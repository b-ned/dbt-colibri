import os
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator

# Fixed version
version = "1.10"

print(f"Processing dbt version {version}...")

manifest_path = f"tests/test_data/{version}/manifest.json"
catalog_path = f"tests/test_data/{version}/catalog.json"

# Output must be 'dist/' for GitHub Pages
output_dir = "dist"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Extract lineage
extractor = DbtColumnLineageExtractor(
    manifest_path=manifest_path,
    catalog_path=catalog_path
)

# Generate HTML report
report_generator = DbtColibriReportGenerator(extractor)
report_generator.generate_report(output_dir=output_dir)

print(f"✔ Report generated in {output_dir}/index.html")
