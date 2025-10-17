# src/dbt_colibri/cli/cli.py

import click
import os
import sys
from ..lineage_extractor.extractor import DbtColumnLineageExtractor
from ..report.generator import DbtColibriReportGenerator
from importlib.metadata import version, PackageNotFoundError

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
    click.echo(f"{COLIBRI_LOGO}\n")
    click.echo("Welcome to dbt-colibri 🐦")
    """dbt-colibri CLI tool"""
    pass

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

def generate_report(output_dir, manifest, catalog, debug, light):
    """Generate a dbt-colibri lineage report with both JSON and HTML output."""
    import logging
    from ..utils import log

    try:
       

        # Set up logging based on flag
        log_level = logging.DEBUG if debug else logging.INFO
        logger = log.setup_logging(level=log_level)

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
        report_generator = DbtColibriReportGenerator(extractor, light_mode=light)

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


@cli.group("mcp")
def mcp_group():
    """MCP (Model Context Protocol) server commands"""
    pass


@mcp_group.command("config")
@click.option(
    "--manifest",
    type=str,
    required=True,
    help="Path or URL to colibri-manifest.json (local path, http://, https://, s3://, gs://)"
)
@click.option(
    "--cache-dir",
    type=str,
    default=None,
    help="Directory to cache remote manifests (default: ~/.cache/dbt-colibri)"
)
def mcp_config(manifest, cache_dir):
    """Configure the MCP server with a manifest location"""
    from ..mcp.config import MCPConfig
    
    try:
        # Determine if path is remote
        is_remote = MCPConfig.is_remote_path(manifest)
        
        # If local, check if file exists
        if not is_remote and not os.path.exists(manifest):
            click.echo(f"❌ Error: Manifest file not found at {manifest}")
            sys.exit(1)
        
        # Create and save config
        config = MCPConfig(
            manifest_path=manifest,
            is_remote=is_remote,
            cache_dir=cache_dir
        )
        config.save()
        
        manifest_type = "remote" if is_remote else "local"
        click.echo("✅ MCP configured successfully!")
        click.echo(f"   Manifest type: {manifest_type}")
        click.echo(f"   Manifest path: {manifest}")
        if cache_dir:
            click.echo(f"   Cache directory: {cache_dir}")
        click.echo("\nNext steps:")
        click.echo("  1. Run 'colibri mcp serve' to start the server")
        click.echo("  2. Or run 'colibri mcp install' to generate config for Claude/Cursor")
        
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@mcp_group.command("serve")
def mcp_serve():
    """Start the MCP server (for testing; use 'fastmcp run' in production)"""
    try:
        from ..mcp.main import mcp
        mcp.run()
    except ImportError as e:
        if "fastmcp" in str(e):
            click.echo("❌ Error: fastmcp is required to run the MCP server")
            click.echo("   Install with: pip install fastmcp")
        else:
            click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@mcp_group.command("install")
@click.option(
    "--app",
    type=click.Choice(['claude-code', 'cursor'], case_sensitive=False),
    required=True,
    help="Application to install MCP config for"
)
@click.option(
    "--name",
    type=str,
    default="dbt-colibri",
    help="Name for the MCP server (default: dbt-colibri)"
)
@click.option(
    "--python",
    type=str,
    default=None,
    help="Python version to use (e.g., 3.12)"
)
def mcp_install(app, name, python):
    """Install MCP server configuration for Claude Desktop or Cursor"""
    import subprocess
    from ..mcp.config import MCPConfig
    
    try:
        # Check if MCP is configured
        config = MCPConfig.load()
        if not config:
            click.echo("❌ Error: MCP not configured yet")
            click.echo("   Run 'colibri mcp config --manifest <path>' first")
            sys.exit(1)
        
        # Check if fastmcp is available
        import shutil
        if not shutil.which("fastmcp"):
            click.echo("❌ Error: fastmcp is not installed or not in PATH")
            click.echo("   Install with: pip install fastmcp")
            sys.exit(1)
        
        # Build fastmcp install command
        cmd = [
            "fastmcp",
            "install",
            app.lower(),
            "dbt_colibri.mcp.main:mcp",
            "--name",
            name
        ]
        
        # Add python version if specified
        if python:
            cmd.extend(["--python", python])
        
        # Verify module can be imported
        try:
            from ..mcp.main import mcp as mcp_app
            click.echo(f"✅ MCP module found: {mcp_app.name}")
        except Exception as e:
            click.echo(f"❌ Error: Cannot import MCP module: {e}")
            sys.exit(1)
        
        # Run fastmcp install
        click.echo(f"\nInstalling MCP server for {app.title()}...")
        click.echo(f"Running: {' '.join(cmd)}\n")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            click.echo(f"✅ MCP server '{name}' installed for {app.title()}")
            if result.stdout:
                click.echo(result.stdout)
            click.echo(f"\nRestart {app.title()} to use the MCP server.")
        else:
            click.echo(f"❌ Installation failed (exit code {result.returncode})")
            if result.stderr:
                click.echo(f"\nError output:\n{result.stderr}")
            if result.stdout:
                click.echo(f"\nStandard output:\n{result.stdout}")
            click.echo("\nTry running the command manually to see full error:")
            click.echo(f"  {' '.join(cmd)}")
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@mcp_group.command("status")
def mcp_status():
    """Show current MCP configuration status"""
    from ..mcp.config import MCPConfig
    
    config = MCPConfig.load()
    if not config:
        click.echo("❌ MCP not configured")
        click.echo("   Run 'colibri mcp config --manifest <path>' to configure")
        sys.exit(1)
    
    click.echo("✅ MCP Configuration:")
    click.echo(f"   Manifest path: {config.manifest_path}")
    click.echo(f"   Type: {'remote' if config.is_remote else 'local'}")
    if config.cache_dir:
        click.echo(f"   Cache directory: {config.cache_dir}")
    
    # Try to load the manifest to verify it works
    try:
        manifest_path = config.get_manifest_path()
        if os.path.exists(manifest_path):
            click.echo("   Status: ✅ Manifest accessible")
        else:
            click.echo("   Status: ❌ Manifest not found")
    except Exception as e:
        click.echo(f"   Status: ❌ Error: {str(e)}")


@mcp_group.command("clear")
def mcp_clear():
    """Clear MCP configuration and cache"""
    from ..mcp.config import MCPConfig
    from ..mcp.remote import clear_cache
    
    try:
        config = MCPConfig.load()
        
        # Clear cache if config exists
        if config and config.cache_dir:
            clear_cache(config.cache_dir)
            click.echo("✅ Cache cleared")
        else:
            clear_cache()
            click.echo("✅ Cache cleared")
        
        # Delete config
        MCPConfig.delete()
        click.echo("✅ Configuration cleared")
        
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
