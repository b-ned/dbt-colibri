import json
import os
import logging
import sys


def setup_logging():
    """
    Set up logging configuration.
    Returns:
        Logger object
    """
    logger = logging.getLogger("dbt_column_lineage")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def read_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def pretty_print_dict(dict_to_print):
    """
    Pretty print a dictionary as JSON.
    Logs the formatted JSON and also prints it directly for test compatibility.

    Args:
        dict_to_print: Dictionary to print

    Returns:
        Formatted JSON string
    """
    formatted_json = json.dumps(dict_to_print, indent=4)

    # Log using the logger
    logger = setup_logging()
    logger.info(formatted_json)

    # Also print directly for test compatibility
    print(formatted_json)

    return formatted_json


def write_dict_to_file(dict_to_write, file_path):
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as file:
        json.dump(dict_to_write, file, indent=4)


def read_dict_from_file(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def find_potential_matches(lineage_data, model_name):
    """Find potential model matches based on partial name match."""
    model_name = model_name.lower()
    return [model for model in lineage_data.keys() if model_name in model.lower()]


def find_exact_name_matches(lineage_data, model_name):
    """Find models that match exactly by node name (after the resource type and package)."""
    model_name = model_name.lower()
    # Extract just the name part from full node paths like 'model.analytics.aicg__fact_user'
    return [model for model in lineage_data.keys() if model.lower().split(".")[-1] == model_name]
