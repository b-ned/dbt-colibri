# src/dbt_colibri/cli/cli.py

import click
import os
import sys
from ..lineage_extractor.extractor import DbtColumnLineageExtractor
from ..report.generator import DbtColibriReportGenerator
from importlib.metadata import version, PackageNotFoundError


def _is_telemetry_disabled(cli_flag: bool) -> bool:
    """Check if telemetry is disabled via CLI flag or environment variables.

    Telemetry is disabled if any of the following are true:
      - ``--disable-telemetry`` CLI flag is passed
      - ``DO_NOT_TRACK=1`` environment variable is set (community standard)
      - ``DISABLE_COLIBRI_TELEMETRY=1`` environment variable is set
    """
    if cli_flag:
        return True
    if os.environ.get("DO_NOT_TRACK", "").strip() == "1":
        return True
    if os.environ.get("DISABLE_COLIBRI_TELEMETRY", "").strip() == "1":
        return True
    return False

COLIBRI_LOGO = r"""
 ______     ______     __         __     ______     ______     __    
/\  ___\   /\  __ \   /\ \       /\ \   /\  == \   /\  == \   /\ \   
\ \ \____  \ \ \/\ \  \ \ \____  \ \ \  \ \  __<   \ \  __<   \ \ \  
 \ \_____\  \ \_____\  \ \_____\  \ \_\  \ \_____\  \ \_\ \_\  \ \_\ 
  \/_____/   \/_____/   \/_____/   \/_/   \/_____/   \/_/ /_/   \/_/ 
"""

try:
    __version__ = version("dbt-colibri")
except PackageNotFoundError:
    __version__ = "unknown"

@click.group()
@click.version_option(__version__, prog_name="dbt-colibri")
def cli():
    """dbt-colibri CLI tool"""
    click.echo(f"{COLIBRI_LOGO}\n")
    click.echo("Welcome to dbt-colibri 🐦")

    from ..utils.version_check import get_update_message

    update_msg = get_update_message(__version__)
    if update_msg:
        click.echo(click.style(update_msg, fg="yellow"))

@cli.command("generate")
@click.option(
    "--output-dir",
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
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable debug-level logging"
)
@click.option(
    "--light",
    is_flag=True,
    default=False,
    help="Enable light mode (excludes compiled_code from output for smaller file size)"
)
@click.option(
    "--disable-telemetry",
    is_flag=True,
    default=False,
    help="Disable anonymous usage telemetry in the generated HTML report. "
         "Can also be set via DO_NOT_TRACK=1 or DISABLE_COLIBRI_TELEMETRY=1 environment variables."
)

def generate_report(output_dir, manifest, catalog, debug, light, disable_telemetry):
    """Generate a dbt-colibri lineage report with both JSON and HTML output."""
    import logging
    from ..utils import log

    try:


        # Set up logging based on flag
        log_level = logging.DEBUG if debug else logging.INFO
        logger = log.setup_logging(level=log_level)

        # Resolve telemetry opt-out from CLI flag + env vars
        telemetry_disabled = _is_telemetry_disabled(disable_telemetry)

        if not telemetry_disabled:
            click.echo(
                "Colibri collects anonymous usage statistics (adapter type, node count) "
                "to improve the product.\n"
                "To disable, use --disable-telemetry, or set DO_NOT_TRACK=1 "
                "or DISABLE_COLIBRI_TELEMETRY=1.\n"
            )

        if not os.path.exists(manifest):
            logger.error(f"❌ Manifest file not found at {manifest}")
            sys.exit(1)
        if not os.path.exists(catalog):
            logger.error(f"❌ Catalog file not found at {catalog}")
            sys.exit(1)

        logger.info("Loading dbt manifest and catalog...")
        extractor = DbtColumnLineageExtractor(manifest, catalog)

        # --- Log version info (matches what will end up in metadata) ---
        manifest_meta = extractor.manifest.get("metadata", {})
        adapter = manifest_meta.get("adapter_type", "unknown")
        dbt_version = manifest_meta.get("dbt_version", "unknown")
        project = manifest_meta.get("project_name", "unknown")

        logger.info(
            "Running with configuration:\n"
            f"         dbt-colbri version : {extractor.colibri_version}\n"
            f"         dbt version        : {dbt_version}\n"
            f"         SQL dialect        : {adapter}\n"
            f"         dbt project        : {project}"
        )

        logger.info("Extracting lineage data...")
        report_generator = DbtColibriReportGenerator(
            extractor, light_mode=light, disable_telemetry=telemetry_disabled
        )

        logger.info("Generating report...")
        report_generator.generate_report(output_dir=output_dir)
        click.echo("\n")
        click.echo("✅ Report completed!")
        click.echo(f"  📁 JSON: {output_dir}/colibri-manifest.json")
        click.echo(f"  🌐 HTML: {output_dir}/index.html")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
