import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "next_beta_version", Path(__file__).parents[1] / "scripts" / "next_beta_version.py"
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


@pytest.mark.parametrize(
    "tags,expected",
    [
        (["v0.3.6", "v0.3.6b1", "v0.3.7b1"], "0.3.7b2"),
        (["v0.3.7b1", "v0.3.7"], "0.3.8b1"),        # stable sorts after its betas
        (["v0.3.9b1", "v0.3.10b2"], "0.3.10b3"),    # numeric, not lexical, order
        (["v0.4.0b1", "v0.3.9"], "0.4.0b2"),        # manual beta of a new minor wins
        (["v1.0.0-rc1", "latest", "v0.1.0"], "0.1.1b1"),  # other tags ignored
        ([], "0.0.1b1"),
    ],
)
def test_next_beta(tags, expected):
    assert _mod.next_beta(tags) == expected
