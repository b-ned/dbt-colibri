# Model Selection Syntax

This document explains the dbt-style model selection syntax supported by the DBT Column Lineage Extractor tool.

## Node Types
- Regular models: `model_name` or `model.package.model_name`
- Source nodes: `source.schema.table`

## Graph Operators
- `+model_name` - Include the model and all its ancestors (upstream models)
- `model_name+` - Include the model and all its descendants (downstream models)
- `+model_name+` - Include the model, all its ancestors, and all its descendants (entire lineage)

## Set Operators
- `model1 model2` - Select models that match either selector (union)
- `model1,model2` - Select models that match both selectors (intersection)

## Resource Selectors
- `tag:daily` - Select models with the tag "daily"
- `path:models/finance` - Select models in the specified path
- `package:marketing` - Select models from the specified package

## Examples

```bash
# Select all finance models and their dependencies
dbt_column_lineage_direct --model "+tag:finance"

# Select order models and their downstream dependencies
dbt_column_lineage_direct --model "orders+"

# Select models that are both daily and in the finance package
dbt_column_lineage_direct --model "tag:daily,package:finance"

# Select models in the core package or downstream from customers
dbt_column_lineage_direct --model "package:core customers+"

# Select a specific source
dbt_column_lineage_direct --model "source.raw.customers"

# Select a source and its downstream dependencies
dbt_column_lineage_direct --model "source.raw.customers+"

# Select a source and its entire lineage (both upstream and downstream)
dbt_column_lineage_direct --model "+source.raw.customers+"
```

You can also specify models from a JSON file using the `--model-list-json` parameter:
```bash
dbt_column_lineage_direct --manifest ./inputs/manifest.json --catalog ./inputs/catalog.json --model-list-json ./models.json
```
Where `models.json` is a JSON file containing a list of model names:
```json
[
  "model.jaffle_shop.customers",
  "model.jaffle_shop.orders",
  "source.raw.customers"
]
``` 