import os
import glob
import json


def get_dialect_from_test_data_dir(test_data_dir: str) -> str:
    """
    Determine the SQL dialect from the test data directory name.
    
    Args:
        test_data_dir: Path to test data directory (e.g., 'tests/test_data/bigquery')
        
    Returns:
        SQL dialect string (e.g., 'bigquery', 'snowflake')
    """
    if test_data_dir is None:
        return "snowflake"  # default fallback
    
    # Extract directory name from path
    dir_name = os.path.basename(test_data_dir)
    
    # Dialect mapping based on directory names
    dialect_mapping = {
        "1.8": "snowflake",      # dbt 1.8 typically uses snowflake
        "1.9": "snowflake",      # dbt 1.9 typically uses snowflake  
        "1.10": "snowflake",     # dbt 1.10 typically uses snowflake
        "bigquery": "bigquery",  # BigQuery data uses bigquery dialect
        "postgres": "postgres",  # PostgreSQL data uses postgres dialect
        "mysql": "mysql",        # MySQL data uses mysql dialect
        "redshift": "redshift",  # Redshift data uses redshift dialect
        "sqlite": "sqlite",      # SQLite data uses sqlite dialect
        "duckdb": "duckdb",      # DuckDB data uses duckdb dialect
    }
    
    return dialect_mapping.get(dir_name, "snowflake")



def _test_data_base_dir() -> str:
    return os.path.join("tests", "test_data")


def _discover_all_dataset_dirs() -> list:
    base_dir = _test_data_base_dir()
    candidates = [
        d for d in glob.glob(os.path.join(base_dir, "*"))
        if os.path.isdir(d)
    ]
    return sorted(candidates)


def _discover_valid_dataset_dirs() -> list:
    valid_dirs = []
    for d in _discover_all_dataset_dirs():
        manifest = os.path.join(d, "manifest.json")
        catalog = os.path.join(d, "catalog.json")
        if not (os.path.exists(manifest) and os.path.exists(catalog)):
            continue
        # Try to validate minimally that these are real dbt artifacts
        try:
            with open(manifest, "r", encoding="utf-8") as mf:
                manifest_json = json.load(mf)
            with open(catalog, "r", encoding="utf-8") as cf:
                catalog_json = json.load(cf)
        except Exception:
            continue
        if not isinstance(manifest_json, dict) or not isinstance(catalog_json, dict):
            continue
        if not manifest_json.get("nodes") and not manifest_json.get("sources"):
            continue
        valid_dirs.append(d)
    return valid_dirs


def pytest_generate_tests(metafunc):
    # Any test that declares a parameter named "dbt_test_data_dir"
    # will be parametrized to run once per discovered version directory
    if "dbt_test_data_dir" in metafunc.fixturenames:
        all_dirs = _discover_all_dataset_dirs()
        if not all_dirs:
            all_dirs = [None]
        ids = [os.path.basename(d) if d is not None else "no-data" for d in all_dirs]
        metafunc.parametrize("dbt_test_data_dir", all_dirs, ids=ids)

    if "dbt_valid_test_data_dir" in metafunc.fixturenames:
        valid_dirs = _discover_valid_dataset_dirs()
        if not valid_dirs:
            valid_dirs = [None]
        ids = [os.path.basename(d) if d is not None else "no-valid-data" for d in valid_dirs]
        metafunc.parametrize("dbt_valid_test_data_dir", valid_dirs, ids=ids)

