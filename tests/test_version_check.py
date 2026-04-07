import json
import time
from unittest.mock import patch


from dbt_colibri.utils.version_check import (
    get_update_message,
    _read_cache,
    _write_cache,
    _parse_version,
    _is_version_check_disabled,
)


class TestParseVersion:
    def test_simple(self):
        assert _parse_version("0.3.0") == (0, 3, 0)

    def test_comparison(self):
        assert _parse_version("0.4.0") > _parse_version("0.3.0")
        assert _parse_version("1.0.0") > _parse_version("0.99.99")

    def test_invalid(self):
        assert _parse_version("unknown") == (0,)


class TestIsVersionCheckDisabled:
    def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("DBT_COLIBRI_NO_VERSION_CHECK", "1")
        assert _is_version_check_disabled() is True

    def test_not_disabled(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        assert _is_version_check_disabled() is False


class TestCache:
    def test_write_and_read(self, tmp_path):
        cache_file = tmp_path / "version_check.json"
        with patch("dbt_colibri.utils.version_check.CACHE_FILE", cache_file), \
             patch("dbt_colibri.utils.version_check.CACHE_DIR", tmp_path):
            _write_cache("0.5.0")
            cache = _read_cache()
            assert cache is not None
            assert cache["latest_version"] == "0.5.0"

    def test_expired_cache(self, tmp_path):
        cache_file = tmp_path / "version_check.json"
        cache_file.write_text(json.dumps({
            "timestamp": time.time() - 100_000,
            "latest_version": "0.5.0",
        }))
        with patch("dbt_colibri.utils.version_check.CACHE_FILE", cache_file):
            assert _read_cache() is None


class TestGetUpdateMessage:
    def test_returns_none_when_disabled(self, monkeypatch):
        monkeypatch.setenv("DBT_COLIBRI_NO_VERSION_CHECK", "1")
        assert get_update_message("0.3.0") is None

    def test_returns_none_for_unknown_version(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        assert get_update_message("unknown") is None

    def test_returns_message_when_outdated(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        with patch("dbt_colibri.utils.version_check._read_cache", return_value=None), \
             patch("dbt_colibri.utils.version_check._fetch_latest_version", return_value="0.5.0"), \
             patch("dbt_colibri.utils.version_check._write_cache"):
            msg = get_update_message("0.3.0")
            assert msg is not None
            assert "0.3.0" in msg
            assert "0.5.0" in msg
            assert "pip install -U dbt-colibri" in msg

    def test_returns_none_when_up_to_date(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        with patch("dbt_colibri.utils.version_check._read_cache", return_value=None), \
             patch("dbt_colibri.utils.version_check._fetch_latest_version", return_value="0.3.0"), \
             patch("dbt_colibri.utils.version_check._write_cache"):
            assert get_update_message("0.3.0") is None

    def test_returns_none_when_pypi_unreachable(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        with patch("dbt_colibri.utils.version_check._read_cache", return_value=None), \
             patch("dbt_colibri.utils.version_check._fetch_latest_version", return_value=None):
            assert get_update_message("0.3.0") is None

    def test_uses_cache(self, monkeypatch):
        monkeypatch.delenv("DBT_COLIBRI_NO_VERSION_CHECK", raising=False)
        cache = {"timestamp": time.time(), "latest_version": "0.6.0"}
        with patch("dbt_colibri.utils.version_check._read_cache", return_value=cache), \
             patch("dbt_colibri.utils.version_check._fetch_latest_version") as mock_fetch:
            msg = get_update_message("0.3.0")
            assert "0.6.0" in msg
            mock_fetch.assert_not_called()
