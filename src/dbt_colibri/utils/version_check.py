# src/dbt_colibri/utils/version_check.py

import json
import os
import time
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PYPI_URL = "https://pypi.org/pypi/dbt-colibri/json"
CACHE_DIR = Path.home() / ".dbt-colibri"
CACHE_FILE = CACHE_DIR / "version_check.json"
CHECK_INTERVAL_SECONDS = 24 * 60 * 60  # 24 hours
REQUEST_TIMEOUT_SECONDS = 2


def _is_version_check_disabled() -> bool:
    return os.environ.get("DBT_COLIBRI_NO_VERSION_CHECK", "").strip() == "1"


def _read_cache() -> Optional[dict]:
    try:
        if CACHE_FILE.exists():
            data = json.loads(CACHE_FILE.read_text())
            if time.time() - data.get("timestamp", 0) < CHECK_INTERVAL_SECONDS:
                return data
    except Exception:
        pass
    return None


def _write_cache(latest_version: str) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(
            json.dumps({"timestamp": time.time(), "latest_version": latest_version})
        )
    except Exception:
        pass


def _fetch_latest_version() -> Optional[str]:
    try:
        from urllib.request import urlopen, Request

        req = Request(PYPI_URL, headers={"Accept": "application/json"})
        with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
            data = json.loads(resp.read())
            return data["info"]["version"]
    except Exception:
        return None


def _parse_version(v: str) -> tuple:
    """Parse a PEP 440-ish version string into a comparable tuple."""
    try:
        return tuple(int(x) for x in v.split("."))
    except (ValueError, AttributeError):
        return (0,)


def get_update_message(current_version: str) -> Optional[str]:
    """Check if a newer version is available and return an update message, or None."""
    if _is_version_check_disabled():
        return None

    if current_version == "unknown":
        return None

    # Try cache first
    cache = _read_cache()
    if cache:
        latest_version = cache["latest_version"]
    else:
        latest_version = _fetch_latest_version()
        if latest_version:
            _write_cache(latest_version)

    if not latest_version:
        return None

    if _parse_version(latest_version) > _parse_version(current_version):
        return (
            f"\n╭───────────────────────────────────────────────────╮\n"
            f"│  Update available: {current_version} → {latest_version:<28s}│\n"
            f"│  Run `pip install -U dbt-colibri` to update       │\n"
            f"╰───────────────────────────────────────────────────╯"
        )

    return None
