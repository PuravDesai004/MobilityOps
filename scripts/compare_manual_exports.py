"""Offline comparison of user-exported Snowflake CSV to verified local artifact evidence."""

import argparse
import csv
import json
from decimal import Decimal
from pathlib import Path

from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically

FIELDS = (
    "source_rows",
    "accepted_rows",
    "quarantined_rows",
    "fact_rows",
    "mart_trip_count",
    "recorded_charge_usd",
)


def compare_export(export: Path, reports: list[Path]):
    local = [json.loads(path.read_text()) for path in reports]
    if not local or any(report["status"] != "PASSED" for report in local):
        raise ValueError("Only passed local reconciliation reports may be comparison references")
    reference = {(report["dataset_id"], report["artifact_id"]): report for report in local}
    with export.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if set(reader.fieldnames or []) != {"dataset_id", "artifact_id", *FIELDS}:
            raise ValueError("Manual export CSV headers disagree with file 13 comparison query")
        rows = list(reader)
    keys = [(row["dataset_id"], row["artifact_id"]) for row in rows]
    if len(set(keys)) != len(keys) or set(keys) != set(reference):
        raise ValueError("Manual export artifact identities/coverage differ from local reports")
    differences = [
        {
            "dataset_id": row["dataset_id"],
            "artifact_id": row["artifact_id"],
            "differences": {
                field: str(Decimal(row[field]) - Decimal(str(reference[key][field])))
                for field in FIELDS
            },
        }
        for row, key in zip(rows, keys, strict=True)
    ]
    passed = all(
        Decimal(value) == 0 for row in differences for value in row["differences"].values()
    )
    return {
        "status": "PASSED" if passed else "FAILED",
        "verified_at": utc_now(),
        "kind": "offline_comparison_of_user_supplied_exports",
        "snowflake_connection_performed": False,
        "snowflake_execution_provenance": "USER_SUPPLIED_NOT_INDEPENDENTLY_AUTHENTICATED",
        "export_sha256": sha256_file(export),
        "artifacts": differences,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--local-report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare_export(args.export, args.local_report)
    write_manifest_atomically(args.output, result)
    print(json.dumps(result, indent=2))
    if result["status"] != "PASSED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
