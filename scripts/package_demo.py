"""Copy only a verified small snapshot bundle to a portable repository demo directory."""

import argparse
import json
import shutil
import uuid
from pathlib import Path

from mobilityops.manifest import write_manifest_atomically
from mobilityops.snapshot import load_snapshot

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="demo/snapshots")
    parser.add_argument("--destination", default="demo/published")
    args = parser.parse_args()
    source, destination = ROOT / args.source, ROOT / args.destination
    _, _, _, manifest = load_snapshot(source)
    generation = destination / "generations" / uuid.uuid4().hex
    generation.mkdir(parents=True)
    public_manifest = json.loads(json.dumps(manifest))
    for name, receipt in manifest["files"].items():
        shutil.copy2(source / receipt["path"], generation / name)
        public_manifest["files"][name]["path"] = (
            (generation / name).relative_to(destination).as_posix()
        )
    # Validate the candidate bundle before making its manifest visible.
    from mobilityops.snapshot import _load_validated

    _load_validated(destination, public_manifest)
    write_manifest_atomically(destination / "snapshot_manifest.json", public_manifest)
    # Keep old local generations recoverable, but only the reviewed current bundle Git-eligible.
    (destination / ".gitignore").write_text(
        f"generations/*\n!generations/{generation.name}/\n", encoding="utf-8"
    )
    print(f"Portable verified demo: {destination}")


if __name__ == "__main__":
    main()
