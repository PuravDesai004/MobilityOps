"""Optional official Ruff library fallback when Windows blocks the native executable."""

import base64
import hashlib
import io
import json
import tarfile
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.16.10"
PACKAGE = "@astral-sh/ruff-wasm-nodejs"


def main():
    response = requests.get(f"https://registry.npmjs.org/{PACKAGE}/{VERSION}", timeout=(10, 30))
    response.raise_for_status()
    metadata = response.json()
    if metadata.get("dependencies"):
        raise ValueError("Unexpected dependencies: review the Ruff tooling package")
    distribution = metadata["dist"]
    tarball = requests.get(distribution["tarball"], timeout=(10, 60))
    tarball.raise_for_status()
    actual = "sha512-" + base64.b64encode(hashlib.sha512(tarball.content).digest()).decode()
    if actual != distribution["integrity"]:
        raise ValueError("Ruff official package integrity check failed")
    target = ROOT / ".tools/ruff-wasm"
    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(tarball.content), mode="r:gz") as archive:
        # Python's data filter rejects traversal, absolute paths and unsafe links.
        archive.extractall(target, filter="data")
    receipt = {
        "package": PACKAGE,
        "version": VERSION,
        "tarball": distribution["tarball"],
        "integrity": actual,
        "purpose": "Official Ruff library via existing Node runtime",
    }
    (ROOT / "requirements/ruff-wasm.lock.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
