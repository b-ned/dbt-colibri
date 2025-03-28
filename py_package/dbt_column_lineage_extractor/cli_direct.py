import argparse
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor


def main():
    parser = argparse.ArgumentParser(description="DBT Column Lineage Extractor CLI")
    parser.add_argument(
        "--manifest",
        default="./inputs/manifest.json",
        help="Path to the manifest.json file, default to ./inputs/manifest.json",
    )
    parser.add_argument(
        "--catalog",
        default="./inputs/catalog.json",
        help="Path to the catalog.json file, default to ./inputs/catalog.json",
    )
    parser.add_argument(
        "--dialect",
        default="snowflake",
        help="SQL dialect to use, default is snowflake, more dialects at https://github.com/tobymao/sqlglot/tree/v25.24.5/sqlglot/dialects",
    )
    parser.add_argument(
        "--model",
        nargs="*",
        default=[],
        help="""List of models to extract lineage for using dbt-style selectors:
            - Simple model names: model_name
            - Include ancestors: +model_name (include upstream/parent models)
            - Include descendants: model_name+ (include downstream/child models)
            - Union (either): "model1 model2" (models matching either selector)
            - Intersection (both): "model1,model2" (models matching both selectors)
            - Tag filtering: tag:my_tag (models with specific tag)
            - Path filtering: path:models/finance (models in specific path)
            - Package filtering: package:my_package (models in specific package)
            Default behavior extracts lineage for all models.""",
    )
    parser.add_argument(
        "--model-list-json",
        help="Path to a JSON file containing a list of models to extract lineage for. If specified, this takes precedence over --model",
    )
    parser.add_argument(
        "--output-dir",
        default="./outputs",
        help="Directory to write output json files, default to ./outputs",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing even if some models fail",
    )

    args = parser.parse_args()

    # Set up logging
    logger = utils.setup_logging()

    try:
        selected_models = args.model
        if args.model_list_json:
            try:
                selected_models = utils.read_json(args.model_list_json)
                if not isinstance(selected_models, list):
                    raise ValueError("The JSON file must contain a list of model names")
            except Exception as e:
                logger.error(f"Error reading model list from JSON file: {e}")
                return 1

        extractor = DbtColumnLineageExtractor(
            manifest_path=args.manifest,
            catalog_path=args.catalog,
            selected_models=selected_models,
            dialect=args.dialect,
        )

        logger.info(f"Processing {len(extractor.selected_models)} models after selector expansion")

        try:
            lineage_map = extractor.build_lineage_map()

            if not lineage_map:
                logger.warning("Warning: No valid lineage was generated. Check for errors above.")
                if not args.continue_on_error:
                    return 1

            lineage_to_direct_parents = extractor.get_columns_lineage_from_sqlglot_lineage_map(
                lineage_map
            )
            lineage_to_direct_children = (
                extractor.get_lineage_to_direct_children_from_lineage_to_direct_parents(
                    lineage_to_direct_parents
                )
            )

            utils.write_dict_to_file(
                lineage_to_direct_parents, f"{args.output_dir}/lineage_to_direct_parents.json"
            )

            utils.write_dict_to_file(
                lineage_to_direct_children, f"{args.output_dir}/lineage_to_direct_children.json"
            )

            logger.info("Lineage extraction complete. Output files written to output directory.")
            return 0

        except Exception as e:
            logger.error(f"Error during lineage extraction: {str(e)}")
            if not args.continue_on_error:
                raise
            return 1

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
