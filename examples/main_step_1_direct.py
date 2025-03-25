import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

utils.clear_screen()

manifest_path = "./inputs/manifest.json"
catalog_path = "./inputs/catalog.json"
dialect = "snowflake"

# Add the models you want to extract lineage for, or leave it empty to extract lineage for all models
li_selected_model = [
    # "model.dbt_project_1.model_1"
    # "model.dbt_project_1.model_2"
]

# Alternative: You can also load models from a JSON file
# import json
# with open('models.json', 'r') as f:
#     li_selected_model = json.load(f)

extractor = DbtColumnLineageExtractor(
    manifest_path=manifest_path,
    catalog_path=catalog_path,
    selected_models=li_selected_model,
    dialect=dialect,
)
lineage_map = extractor.build_lineage_map()
lineage_to_direct_parents = extractor.get_columns_lineage_from_sqlglot_lineage_map(lineage_map)
lineage_to_direct_children = (
    extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(
        lineage_to_direct_parents
    )
)

# %%
## OUTPUTS

# utils.pretty_print_dict(lineage_to_direct_parents)
utils.write_dict_to_file(
    lineage_to_direct_parents, "./outputs/lineage_to_direct_parents.json"
)

# utils.pretty_print_dict(lineage_to_direct_children)
utils.write_dict_to_file(
    lineage_to_direct_children, "./outputs/lineage_to_direct_children.json"
)
