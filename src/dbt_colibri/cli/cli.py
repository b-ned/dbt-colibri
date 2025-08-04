# src/dbt_colibri/cli/cli.py

import click
import os
from ..lineage_extractor.extractor import DbtColumnLineageExtractor
from ..report.generator import DbtColibriReportGenerator

@click.group()
def cli():
    """dbt-colibri CLI tool"""
    pass

@cli.command("generate")
@click.option(
    "--target-dir",
    type=str,
    default="dist",
    help="Directory to save both JSON and HTML files (default: dist)"
)
@click.option(
    "--manifest",
    type=str,
    default="target/manifest.json",
    help="Path to dbt manifest.json file (default: target/manifest.json)"
)
@click.option(
    "--catalog", 
    type=str,
    default="target/catalog.json",
    help="Path to dbt catalog.json file (default: target/catalog.json)"
)
def generate_report(target_dir, manifest, catalog):
    """Generate a dbt-colibri lineage report with both JSON and HTML output."""
    try:
        if not os.path.exists(manifest):
            click.echo(f"❌ Manifest file not found at {manifest}")
            return 1
        if not os.path.exists(catalog):
            click.echo(f"❌ Catalog file not found at {catalog}")
            return 1

        click.echo("🔍 Loading dbt manifest and catalog...")
        extractor = DbtColumnLineageExtractor(manifest, catalog)

        click.echo("📊 Extracting lineage data...")
        report_generator = DbtColibriReportGenerator(extractor)

        click.echo("🚀 Generating report...")
        report_generator.generate_report(target_dir=target_dir)

        click.echo("✅ Report completed!")
        click.echo(f"   📁 JSON: {target_dir}/colibri-manifest.json")
        click.echo(f"   🌐 HTML: {target_dir}/index.html")
        return 0
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        return 1


if __name__ == "__main__":
    cli()
