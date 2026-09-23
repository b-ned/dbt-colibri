"""Print the next beta version, derived from the repo's ``vX.Y.Z[bN]`` tags.

- Latest tag is a beta (``v0.3.7b1``) -> next beta of that version (``0.3.7b2``).
- Latest tag is stable (``v0.3.7``)   -> first beta of the next patch (``0.3.8b1``).
"""

import re
import subprocess

TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:b(\d+))?$")


def next_beta(tags):
    versions = []
    for tag in tags:
        m = TAG.match(tag.strip())
        if m:
            major, minor, patch, beta = m.groups()
            # A stable release sorts after all of its betas.
            versions.append((int(major), int(minor), int(patch), int(beta) if beta else float("inf")))
    if not versions:
        return "0.0.1b1"
    major, minor, patch, beta = max(versions)
    if beta == float("inf"):
        return f"{major}.{minor}.{patch + 1}b1"
    return f"{major}.{minor}.{patch}b{beta + 1}"


if __name__ == "__main__":
    tags = subprocess.run(["git", "tag", "--list", "v*"], capture_output=True, text=True, check=True).stdout
    print(next_beta(tags.splitlines()))
