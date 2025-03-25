import argparse
import os
import json
import dbt_column_lineage_extractor.utils as utils
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def find_model_in_lineage(lineage_data, model_name):
    """Find full node paths in lineage data that match the model name."""
    matching_nodes = []
    for node in lineage_data.keys():
        # Check if the node ends with the model name
        if node.split('.')[-1] == model_name:
            matching_nodes.append(node)
    return matching_nodes

def main():
    parser = argparse.ArgumentParser(description="Recursive DBT Column Lineage Extractor CLI")
    parser.add_argument('--model', required=True, help='Model to find lineage for, can be short name (e.g. "customers") or full node path (e.g. "model.jaffle_shop.customers")')
    parser.add_argument('--column', required=True, help='Column name to find lineage for, e.g. order_id')
    parser.add_argument('--lineage-parents-file', default='./outputs/lineage_to_direct_parents.json',
                        help='Path to the lineage_to_direct_parents.json file, default to ./outputs/lineage_to_direct_parents.json')
    parser.add_argument('--lineage-children-file', default='./outputs/lineage_to_direct_children.json',
                        help='Path to the lineage_to_direct_children.json file, default to ./outputs/lineage_to_direct_children.json')
    parser.add_argument('--manifest', default='./target/manifest.json', 
                        help='Path to the dbt manifest.json file, default to ./target/manifest.json')
    parser.add_argument('--catalog', default='./target/catalog.json',
                        help='Path to the dbt catalog.json file, default to ./target/catalog.json')
    parser.add_argument('--output-dir', default='./outputs', help='Output directory for lineage files, default to ./outputs')
    parser.add_argument('--show-ui', action='store_true', help='Show web UI for lineage visualization')

    args = parser.parse_args()

    try:
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Read lineage data from files
        try:
            lineage_to_direct_parents = utils.read_dict_from_file(args.lineage_parents_file)
            lineage_to_direct_children = utils.read_dict_from_file(args.lineage_children_file)
        except FileNotFoundError as e:
            print(f"Error: Could not find required lineage file: {e}")
            return 1
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in lineage file: {e}")
            return 1

        # Resolve model name to full node path if needed
        model_node = args.model
        # Check if this is not already a full node path
        if not model_node.startswith(('model.', 'source.')):
            # First try to find the model in lineage files
            matching_nodes_from_lineage = find_model_in_lineage(lineage_to_direct_parents, model_node)
            
            if matching_nodes_from_lineage:
                if len(matching_nodes_from_lineage) > 1:
                    print(f"Warning: Multiple models match '{model_node}' in lineage files. Using the first match: {matching_nodes_from_lineage[0]}")
                model_node = matching_nodes_from_lineage[0]
                print(f"Resolved model name '{args.model}' to full node path from lineage file: {model_node}")
            else:
                # Try alternate lineage file if first one didn't have matches
                matching_nodes_from_lineage = find_model_in_lineage(lineage_to_direct_children, model_node)
                if matching_nodes_from_lineage:
                    if len(matching_nodes_from_lineage) > 1:
                        print(f"Warning: Multiple models match '{model_node}' in lineage files. Using the first match: {matching_nodes_from_lineage[0]}")
                    model_node = matching_nodes_from_lineage[0]
                    print(f"Resolved model name '{args.model}' to full node path from lineage file: {model_node}")
                else:
                    # If not found in lineage files, try to use manifest if available
                    try:
                        # First check if manifest and catalog files exist
                        if not os.path.exists(args.manifest):
                            print(f"Warning: Manifest file not found at '{args.manifest}'. Cannot resolve model name.")
                            print("Proceeding with the original model name. If this is incorrect, please provide a full node path (e.g., model.package.model_name)")
                        elif not os.path.exists(args.catalog):
                            print(f"Warning: Catalog file not found at '{args.catalog}'. Cannot resolve model name.")
                            print("Proceeding with the original model name. If this is incorrect, please provide a full node path (e.g., model.package.model_name)")
                        else:
                            extractor = DbtColumnLineageExtractor(
                                manifest_path=args.manifest,
                                catalog_path=args.catalog
                            )
                            # Try to resolve the model name
                            matching_nodes = extractor._resolve_node_by_name(model_node)
                            if matching_nodes:
                                if len(matching_nodes) > 1:
                                    print(f"Warning: Multiple models match '{model_node}'. Using the first match: {matching_nodes[0]}")
                                model_node = matching_nodes[0]
                                print(f"Resolved model name '{args.model}' to full node path: {model_node}")
                            else:
                                print(f"Warning: Could not find any model with name '{model_node}' in the manifest file.")
                                print("Proceeding with the original model name. If this is incorrect, please provide a full node path (e.g., model.package.model_name)")
                    except Exception as e:
                        print(f"Warning: Error when trying to resolve model name: {e}")
                        print("Proceeding with the original model name. If this is incorrect, please provide a full node path (e.g., model.package.model_name)")

        # Check if model exists in lineage files
        if model_node not in lineage_to_direct_parents and model_node not in lineage_to_direct_children:
            print(f"Warning: Model '{model_node}' not found in lineage files. Results may be empty or incomplete.")

        print("========================================")
        # Find all ancestors for a specific model and column
        print(f"Finding all ancestors of {model_node}.{args.column}:")
        ancestors_squashed = DbtColumnLineageExtractor.find_all_related(lineage_to_direct_parents, model_node, args.column)
        ancestors_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_parents, model_node, args.column
        )

        print("---squashed ancestors---")
        utils.pretty_print_dict(ancestors_squashed)
        print("---structured ancestors---")
        utils.pretty_print_dict(ancestors_structured)

        # Save ancestors to files
        ancestors_file = os.path.join(args.output_dir, f"{model_node}_{args.column}_ancestors.json")
        utils.write_dict_to_file(ancestors_structured, ancestors_file)

        print("========================================")
        # Find all descendants for a specific model and column
        print(f"Finding all descendants of {model_node}.{args.column}:")
        descendants_squashed = DbtColumnLineageExtractor.find_all_related(
            lineage_to_direct_children, model_node, args.column
        )
        descendants_structured = DbtColumnLineageExtractor.find_all_related_with_structure(
            lineage_to_direct_children, model_node, args.column
        )

        print("---squashed descendants---")
        utils.pretty_print_dict(descendants_squashed)
        print("---structured descendants---")
        utils.pretty_print_dict(descendants_structured)

        # Save descendants to files
        descendants_file = os.path.join(args.output_dir, f"{model_node}_{args.column}_descendants.json")
        utils.write_dict_to_file(descendants_structured, descendants_file)

        print("========================================")
        print(
            "You can use the structured ancestors and descendants to programmatically use the lineage, "
            "such as for impact analysis, data tagging, etc."
        )
        print(
            "Or, you can copy the json outputs to tools like https://github.com/AykutSarac/jsoncrack.com, "
            "https://jsoncrack.com/editor to visualize the lineage"
        )
        print(f"Lineage outputs saved to {ancestors_file} and {descendants_file}")
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())
