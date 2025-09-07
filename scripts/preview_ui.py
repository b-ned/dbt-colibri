import os
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator
import webbrowser

# List of dbt versions you want to process
versions = ["1.8", "1.9", "1.10", "bigquery", "redshift", "duckdb"]

for version in versions:
    print(f"Processing version {version}...")

    manifest_path = f"tests/test_data/{version}/manifest.json"
    catalog_path = f"tests/test_data/{version}/catalog.json"
    output_dir = os.path.join("output", version)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Dialect is now automatically detected from manifest metadata
    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path
    )

    report_generator = DbtColibriReportGenerator(extractor)
    report_generator.generate_report(output_dir=output_dir)

    print(f"✔ Done with {version}, results in {output_dir}")
     # assume the generator creates index.html
    report_path = os.path.abspath(os.path.join(output_dir, "index.html"))
    if os.path.exists(report_path):
        webbrowser.get("firefox").open_new_tab(f"file://{report_path}")

    print(f"✔ Done with {version}, opened in Firefox.")

print("All versions processed successfully.")
