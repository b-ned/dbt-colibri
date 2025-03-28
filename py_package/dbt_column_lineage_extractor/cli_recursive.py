import argparse
import os
import json
import webbrowser
from pathlib import Path
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor
from dbt_column_lineage_extractor.visualization import create_html_viewer, convert_to_mermaid


def main():
    parser = argparse.ArgumentParser(description="Recursive DBT Column Lineage Extractor CLI")
    parser.add_argument(
        "--model",
        required=True,
        help='Model to find lineage for, can be short name (e.g. "customers") or full node path (e.g. "model.jaffle_shop.customers")',
    )
    parser.add_argument(
        "--column", required=True, help="Column name to find lineage for, e.g. order_id"
    )
    parser.add_argument(
        "--lineage-parents-file",
        default="./outputs/lineage_to_direct_parents.json",
        help="Path to the lineage_to_direct_parents.json file, default to ./outputs/lineage_to_direct_parents.json",
    )
    parser.add_argument(
        "--lineage-children-file",
        default="./outputs/lineage_to_direct_children.json",
        help="Path to the lineage_to_direct_children.json file, default to ./outputs/lineage_to_direct_children.json",
    )
    parser.add_argument(
        "--output-dir",
        default="./outputs",
        help="Output directory for lineage files, default to ./outputs",
    )
    parser.add_argument(
        "--no-ui",
        action="store_true",
        help="Do not automatically open the visualization in browser",
    )
    parser.add_argument(
        "--output-format",
        choices=["json", "mermaid", "both"],
        default="both",
        help="Output format for lineage data. Choose between json, mermaid, or both. Default is both.",
    )
    parser.add_argument(
        "--show-details",
        action="store_true",
        help="Show detailed squashed/structured ancestors/descendants in terminal",
    )

    args = parser.parse_args()

    # Set up logging
    logger = utils.setup_logging()

    try:
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Read lineage data from files
        try:
            lineage_to_direct_parents = utils.read_dict_from_file(args.lineage_parents_file)
            lineage_to_direct_children = utils.read_dict_from_file(args.lineage_children_file)
        except FileNotFoundError as e:
            logger.error(f"Error: Could not find required lineage file: {e}")
            logger.info(
                "\nTo generate the required lineage files, please run one of the following commands first:"
            )
            logger.info(
                "\n1. To scan the whole project (takes longer, but you don't need to run it again for different models if there is no model change):"
            )
            logger.info(
                "   dbt_column_lineage_direct --manifest path/to/manifest.json --catalog path/to/catalog.json"
            )
            logger.info("\n2. If only interested in this model (faster):")
            logger.info(
                f"   dbt_column_lineage_direct --manifest path/to/manifest.json --catalog path/to/catalog.json --model +{args.model}+"
            )
            logger.info("\nAfter running one of these commands, try this command again.")
            return 1
        except json.JSONDecodeError as e:
            logger.error(f"Error: Invalid JSON in lineage file: {e}")
            return 1

        # Use model name as provided - must be full node path
        model_node = args.model

        # Check if model exists in lineage files
        if (
            model_node not in lineage_to_direct_parents
            and model_node not in lineage_to_direct_children
        ):
            # First try to find exact matches by name (model.package.name -> name)
            parent_exact_matches = utils.find_exact_name_matches(
                lineage_to_direct_parents, model_node
            )
            child_exact_matches = utils.find_exact_name_matches(
                lineage_to_direct_children, model_node
            )
            exact_matches = list(set(parent_exact_matches + child_exact_matches))

            # If we have exact matches, only consider those
            if exact_matches:
                if len(exact_matches) > 1:
                    logger.error(
                        "\nError: Multiple models with the same name found. Please use one of the following full node paths:"
                    )
                    for match in sorted(exact_matches):
                        logger.info(f"  - {match}")
                    return 1
                else:
                    # Single exact match found - use it automatically
                    model_node = exact_matches[0]
                    logger.info(f"Using model: {model_node}")
            else:
                # Fall back to substring matching to find potential matches
                parent_matches = utils.find_potential_matches(lineage_to_direct_parents, model_node)
                child_matches = utils.find_potential_matches(lineage_to_direct_children, model_node)

                # Combine unique matches
                all_matches = list(set(parent_matches + child_matches))

                if all_matches:
                    # Single match found - use it automatically
                    if len(all_matches) == 1:
                        model_node = all_matches[0]
                        logger.info(f"Using model: {model_node}")
                    else:
                        # For multiple substring matches, just display information without error
                        logger.info(
                            "\nMultiple potential matches found. Using best match, but you can use one of the following full node paths for precision:"
                        )
                        for match in sorted(all_matches):
                            logger.info(f"  - {match}")
                        # Use the most likely match (shortest one)
                        model_node = min(all_matches, key=len)
                        logger.info(f"Automatically selecting: {model_node}")
                else:
                    logger.warning(
                        f"No matches found for '{model_node}'. Results may be empty or incomplete."
                    )
                    logger.warning("Skipping output generation.")
                    return 1

        logger.info("========================================")
        # Find all ancestors for a specific model and column
        logger.info(f"Finding all ancestors of {model_node}.{args.column}:")
        ancestors_squashed = DbtColumnLineageExtractor.find_all_related(
            lineage_to_direct_parents, model_node, args.column
        )
        ancestors_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_parents, model_node, args.column
        )

        if args.show_details:
            logger.info("---squashed ancestors---")
            utils.pretty_print_dict(ancestors_squashed)
            logger.info("---structured ancestors---")
            utils.pretty_print_dict(ancestors_structured)

        # Find all descendants for a specific model and column
        logger.info("========================================")
        logger.info(f"Finding all descendants of {model_node}.{args.column}:")
        descendants_squashed = DbtColumnLineageExtractor.find_all_related(
            lineage_to_direct_children, model_node, args.column
        )
        descendants_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_children, model_node, args.column
        )

        if args.show_details:
            logger.info("---squashed descendants---")
            utils.pretty_print_dict(descendants_squashed)
            logger.info("---structured descendants---")
            utils.pretty_print_dict(descendants_structured)
            
        # Check if no lineage information was found
        if not ancestors_structured and not descendants_structured:
            logger.warning(f"No lineage found for column '{args.column}' in model '{model_node}'. Skipping output generation.")
            return 0
            
        # Save outputs based on format
        if args.output_format in ["json", "both"]:
            # Create safe filenames by replacing dots with underscores
            safe_model_name = model_node.replace(".", "_")

            # Save ancestors to files
            ancestors_file = os.path.join(
                args.output_dir, f"{safe_model_name}_{args.column}_ancestors.json"
            )
            utils.write_dict_to_file(ancestors_structured, ancestors_file)

            # Save descendants to files
            descendants_file = os.path.join(
                args.output_dir, f"{safe_model_name}_{args.column}_descendants.json"
            )
            utils.write_dict_to_file(descendants_structured, descendants_file)

            logger.info("========================================")
            logger.info(f"Lineage outputs saved to {ancestors_file} and {descendants_file}")

        if args.output_format in ["mermaid", "both"]:
            # Convert to Mermaid format
            mermaid_output = convert_to_mermaid(
                model_node, args.column, ancestors_structured, descendants_structured
            )

            # Save Mermaid output
            mermaid_file = os.path.join(args.output_dir, f"{model_node}_{args.column}_lineage.mmd")
            with open(mermaid_file, "w") as f:
                f.write(mermaid_output)

            # Always create HTML viewer for Mermaid output
            viewer_file = create_html_viewer(
                mermaid_output, args.output_dir, model_node, args.column
            )

            logger.info(f"Mermaid output saved to {mermaid_file}")
            logger.info(f"HTML viewer created at: {viewer_file}")

            # Open the viewer by default unless --no-ui is specified
            if not args.no_ui:
                logger.info("Opening Mermaid diagram in local viewer...")
                webbrowser.open(f"file://{os.path.abspath(viewer_file)}")

        return 0
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
