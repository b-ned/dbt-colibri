# dbt-colibri MCP Server Setup

The dbt-colibri MCP (Model Context Protocol) server provides AI assistants like Claude and Cursor with direct access to your dbt project's lineage data, enabling them to answer questions about model dependencies, column lineage, and more.

## Installation

### Basic Installation (Local Manifests)

```bash
pip install dbt-colibri[mcp]
```

### Installation with Remote Support

For S3 support:
```bash
pip install dbt-colibri[mcp-s3]
```

For Google Cloud Storage support:
```bash
pip install dbt-colibri[mcp-gcp]
```

For all remote options:
```bash
pip install dbt-colibri[mcp-all]
```

## Quick Start

### 1. Generate a Colibri Manifest

First, generate your lineage manifest:

```bash
colibri generate --manifest target/manifest.json --catalog target/catalog.json --output-dir dist
```

This creates a `colibri-manifest.json` file in the `dist/` directory.

### 2. Configure the MCP Server

#### Using a Local Manifest

```bash
colibri mcp config --manifest dist/colibri-manifest.json
```

#### Using a Remote Manifest

**HTTP/HTTPS:**
```bash
colibri mcp config --manifest https://example.com/lineage/colibri-manifest.json
```

**S3:**
```bash
colibri mcp config --manifest s3://my-bucket/dbt-lineage/colibri-manifest.json
```

**Google Cloud Storage:**
```bash
colibri mcp config --manifest gs://my-bucket/dbt-lineage/colibri-manifest.json
```

### 3. Install for Claude Desktop or Cursor

**For Claude Desktop:**
```bash
colibri mcp install --app claude
```

**For Cursor:**
```bash
colibri mcp install --app cursor
```

**With specific Python version:**
```bash
colibri mcp install --app cursor --python 3.12
```

### 4. Restart Your Application

Restart Claude Desktop or Cursor to load the new MCP server configuration.

## Usage

Once configured, you can ask your AI assistant questions like:

- "What models are downstream of `dim_customer`?"
- "Show me all columns that depend on `users.email`"
- "What are the upstream sources for the `revenue` column in `fct_orders`?"
- "Search for models related to customer data"
- "What's the compiled SQL for `dim_product`?"
- "Give me a summary of the dbt project"

## Available MCP Tools

The MCP server provides the following tools:

1. **find_downstream_models** - Find models impacted by changes
2. **find_column_downstream** - Trace column-level downstream dependencies
3. **find_column_upstream** - Trace where a column comes from
4. **get_model_info** - Get detailed model metadata
5. **get_compiled_sql** - Get the compiled SQL for a model
6. **search_models** - Search models by name or description
7. **find_models_with_column** - Find all models containing a column
8. **get_project_summary** - Get project statistics

## Additional Commands

### Check MCP Status

```bash
colibri mcp status
```

### Clear Configuration and Cache

```bash
colibri mcp clear
```

### Run Server Manually (for testing)

```bash
colibri mcp serve
```

## Remote Manifest Caching

When using remote manifests, the MCP server automatically caches them locally in `~/.cache/dbt-colibri/`. You can specify a custom cache directory:

```bash
colibri mcp config --manifest s3://bucket/manifest.json --cache-dir /custom/cache/path
```

## Configuration Files

- **MCP Config:** `~/.config/dbt-colibri/mcp-config.json`
- **Cache Directory:** `~/.cache/dbt-colibri/`
- **Claude Config:** `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
- **Cursor Config:** `~/.cursor/mcp.json`

## Troubleshooting

### "MCP not configured" Error

Run `colibri mcp config --manifest <path>` first to set up the MCP server.

### "fastmcp is required" Error

Install the MCP dependencies:
```bash
pip install dbt-colibri[mcp]
```

### S3 or GCS Errors

Ensure you have the necessary cloud storage libraries installed:
```bash
pip install dbt-colibri[mcp-s3]  # for S3
pip install dbt-colibri[mcp-gcp]  # for GCS
```

Also ensure your cloud credentials are properly configured (AWS CLI, gcloud CLI, etc.).

### Server Not Showing Up in Claude/Cursor

1. Verify the configuration was added: `colibri mcp status`
2. Check the config file exists (see Configuration Files above)
3. Restart the application completely
4. Check for any error messages in the application logs

## Example Workflow

Here's a complete example workflow:

```bash
# 1. Generate lineage manifest
cd my-dbt-project
colibri generate --manifest target/manifest.json --catalog target/catalog.json --output-dir dist

# 2. Upload to S3 (optional)
aws s3 cp dist/colibri-manifest.json s3://my-bucket/dbt-lineage/colibri-manifest.json

# 3. Configure MCP server
colibri mcp config --manifest s3://my-bucket/dbt-lineage/colibri-manifest.json

# 4. Install for Claude Desktop
colibri mcp install --app claude

# 5. Restart Claude Desktop and start asking questions!
```

## Advanced: CI/CD Integration

You can automate manifest generation and distribution in your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
- name: Generate dbt lineage
  run: |
    dbt docs generate
    colibri generate --output-dir dist

- name: Upload to S3
  run: aws s3 cp dist/colibri-manifest.json s3://my-bucket/lineage/latest/
```

Then configure your local MCP to point to the S3 location. The manifest will be automatically downloaded and cached when the MCP server starts.


