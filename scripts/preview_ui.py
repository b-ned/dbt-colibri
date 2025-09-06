import os
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor
from dbt_colibri.report.generator import DbtColibriReportGenerator
import webbrowser

# List of dbt versions you want to process
versions = ["1.8", "1.9", "1.10", "bigquery", "redshift", "duckdb"]

# Dialect mapping for different versions/data sources
# Add new dialects here as you add more test data directories
dialect_mapping = {
    "1.8": "snowflake",      # dbt 1.8 typically uses snowflake
    "1.9": "snowflake",      # dbt 1.9 typically uses snowflake
    "1.10": "snowflake",     # dbt 1.10 typically uses snowflake
    "bigquery": "bigquery",  # BigQuery data uses bigquery dialect
    "postgres": "postgres",  # PostgreSQL data uses postgres dialect
    "mysql": "mysql",        # MySQL data uses mysql dialect
    "redshift": "redshift",  # Redshift data uses redshift dialect
    "sqlite": "sqlite",      # SQLite data uses sqlite dialect
    "duckdb": "duckdb",      # DuckDB data uses duckdb dialect
}

for version in versions:
    print(f"Processing version {version}...")

    manifest_path = f"tests/test_data/{version}/manifest.json"
    catalog_path = f"tests/test_data/{version}/catalog.json"
    output_dir = os.path.join("output", version)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get the appropriate dialect for this version, default to snowflake
    dialect = dialect_mapping.get(version, "snowflake")
    print(f"Using dialect: {dialect}")

    extractor = DbtColumnLineageExtractor(
        manifest_path=manifest_path,
        catalog_path=catalog_path,
        dialect=dialect
    )

    report_generator = DbtColibriReportGenerator(extractor)
    report_generator.generate_report(output_dir=output_dir)

    print(f"✔ Done with {version} (dialect: {dialect}), results in {output_dir}")
     # assume the generator creates index.html
    report_path = os.path.abspath(os.path.join(output_dir, "index.html"))
    if os.path.exists(report_path):
        webbrowser.get("firefox").open_new_tab(f"file://{report_path}")

    print(f"✔ Done with {version}, opened in Firefox.")

print("All versions processed successfully.")
