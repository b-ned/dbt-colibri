import json
import os

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

def read_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def pretty_print_dict(dict_to_print):
    print(json.dumps(dict_to_print, indent=4))


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
