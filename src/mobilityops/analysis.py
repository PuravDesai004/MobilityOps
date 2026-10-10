"""Read-only SQL findings and portable validation; no source preparation or cloud client."""

from __future__ import annotations

import calendar
import csv
import json
import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mobilityops.identity import sha256_file
from mobilityops.manifest import utc_now, write_manifest_atomically
from mobilityops.storage import connect_local

ROOT = Path(__file__).resolve().parents[2]
QUERY_NAMES = (
    "pickup_zone_hour",
    "monthly_changes",
    "pickup_month_coverage",
    "quality_month",
    "warning_codes",
    "quarantine_codes",
    "quality_zones",
)
ANALYSIS_VERSION = 1
MINIMUM_ZONE_VOLUME = 10000
DEFINITIONS = {
    "scope": "All active artifacts in one isolated database; fixture/sample/real never mix.",
    "activity": "Observed accepted Yellow Taxi source records; not total transport demand.",
    "charge": "Signed recorded passenger charge USD; not profit; cash tips incomplete.",
    "monthly_totals": "Source-file month; all accepted pickups, including out-of-partition.",
    "trips_per_calendar_day": "In-partition accepted pickups / days in source-file month.",
    "trips_per_observed_day": "In-partition accepted pickups / distinct observed pickup dates.",
    "coverage": "An observed date is not proof of complete service or provider reporting.",
    "mom": "Only adjacent source months compare; missing/zero prior denominator gives null.",
    "warning_rate": "Distinct accepted records with >=1 warning / accepted records x 100.",
    "quarantine_rate": "Distinct quarantined records / source records x 100.",
    "warning_codes": "Distinct records per code; counts overlap across different codes.",
    "zone_flags": "Rate exceeds source-month rate, with >=10,000 in its own denominator.",
    "zone_members": "Unresolved key 0 and official Unknown keys remain separate members.",
    "duration": "Sum of naive New York wall-clock seconds / accepted count / 60.",
    "peak": "Largest zone/hour count across scope; earliest hour wins ties.",
}


def _portable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def execute_queries(connection) -> dict[str, list[dict[str, Any]]]:
    """Execute the same reviewed SQL on the real database and tiny offline test databases."""
    results = {}
    for name in QUERY_NAMES:
        cursor = connection.execute((ROOT / f"sql/analysis/{name}.sql").read_text(encoding="utf-8"))
        columns = [column[0] for column in cursor.description]
        results[name] = [
            {key: _portable(value) for key, value in zip(columns, row, strict=True)}
            for row in cursor.fetchall()
        ]
    return results


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"Analysis validation failed: {message}")


def _rate(numerator, denominator):
    return 100.0 * numerator / denominator if denominator else None


def _close(actual, expected) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isfinite(float(actual)) and math.isclose(
        float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-8
    )


def validate_analysis(bundle: dict[str, Any], summary: dict[str, Any]) -> None:
    """Validate exported formulas, grains, scope and totals without opening a database."""
    _require(bundle["analysis_version"] == ANALYSIS_VERSION, "version")
    _require(bundle["status"] == "PASSED", "status")
    _require(bundle["scope"] == summary["scope"], "scope")
    _require(bundle["origin_engine"] == summary["origin_engine"], "origin")
    _require(bundle["source_months"] == summary["source_months"], "month coverage")
    _require(bundle["definitions"] == DEFINITIONS, "metric definitions")
    _require(bundle["minimum_zone_volume"] == MINIMUM_ZONE_VOLUME, "zone threshold")

    def keys(rows):
        return {(r["dataset_id"], r["artifact_id"]) for r in rows}

    _require(keys(bundle["artifacts"]) == keys(summary["artifacts"]), "artifact identities")
    _require(len(bundle["artifacts"]) == len(summary["artifacts"]), "duplicate artifacts")
    for artifact in bundle["artifacts"]:
        expected = next(a for a in summary["artifacts"] if keys([a]) == keys([artifact]))
        for field in ("source_partition", "source_rows", "accepted_rows", "quarantined_rows"):
            _require(artifact[field] == expected[field], f"artifact {field}")
        _require(
            Decimal(artifact["recorded_charge"]) == Decimal(expected["recorded_charge"]),
            "artifact charge",
        )
    tables = bundle["tables"]
    _require(set(tables) == set(QUERY_NAMES), "result tables")
    # Reject nonfinite values anywhere, including fields not displayed as headline metrics.
    for rows in tables.values():
        for row in rows:
            for value in row.values():
                if isinstance(value, float):
                    _require(math.isfinite(value), "nonfinite result")
    monthly, quality = tables["monthly_changes"], tables["quality_month"]
    _require([r["source_month"] for r in monthly] == summary["source_months"], "month grain")
    _require([r["source_month"] for r in quality] == summary["source_months"], "quality grain")
    if "accepted_any_warning_trips" in summary:
        _require(
            sum(r["any_warning_trips"] for r in quality) == summary["accepted_any_warning_trips"],
            "any-warning reconciliation",
        )
    zones = tables["pickup_zone_hour"]
    _require(len({r["pickup_zone_key"] for r in zones}) == len(zones), "zone grain")
    _require(sum(r["accepted_trips"] for r in zones) == summary["accepted_rows"], "zone count")
    _require(
        sum((Decimal(r["recorded_charge_usd"]) for r in zones), Decimal(0))
        == Decimal(summary["recorded_charge_usd"]),
        "zone charge",
    )
    for rank, row in enumerate(zones, 1):
        _require(
            row["volume_rank"] == rank and 0 <= row["peak_pickup_hour"] <= 23, "zone rank/hour"
        )
        _require(0 <= row["peak_hour_trips"] <= row["accepted_trips"], "peak denominator")
        _require(
            _close(
                row["accepted_share_pct"], _rate(row["accepted_trips"], summary["accepted_rows"])
            ),
            "zone share",
        )
        _require(
            _close(
                row["peak_hour_zone_share_pct"],
                _rate(row["peak_hour_trips"], row["accepted_trips"]),
            ),
            "peak share",
        )
    for index, row in enumerate(monthly):
        month = row["source_month"]
        artifacts = [a for a in summary["artifacts"] if a["source_partition"] == month]
        count = sum(a["accepted_rows"] for a in artifacts)
        charge = sum((Decimal(a["recorded_charge"]) for a in artifacts), Decimal(0))
        _require(
            row["accepted_trips"] == count and Decimal(row["recorded_charge_usd"]) == charge,
            "monthly count/charge",
        )
        days = calendar.monthrange(*map(int, month.split("-")))[1]
        _require(row["calendar_days"] == days, "calendar denominator")
        _require(0 <= row["observed_in_partition_days"] <= days, "observed-day denominator")
        _require(
            row["in_partition_trips"] + row["excluded_out_of_partition_trips"] == count,
            "excluded count",
        )
        _require(
            Decimal(row["in_partition_charge_usd"]) + Decimal(row["excluded_charge_usd"]) == charge,
            "excluded charge",
        )
        _require(_close(row["avg_charge_usd"], charge / count if count else None), "avg charge")
        _require(
            _close(row["trips_per_calendar_day"], row["in_partition_trips"] / days),
            "calendar-day rate",
        )
        observed = row["observed_in_partition_days"]
        _require(
            _close(
                row["trips_per_observed_day"],
                row["in_partition_trips"] / observed if observed else None,
            ),
            "observed rate",
        )
        previous = monthly[index - 1] if index else None
        _require(
            row["previous_source_month"] == (previous["source_month"] if previous else None),
            "previous source month",
        )
        adjacent = previous and (
            int(month[:4]) * 12
            + int(month[5:])
            - int(previous["source_month"][:4]) * 12
            - int(previous["source_month"][5:])
            == 1
        )
        for value_field, delta_field, pct_field in (
            ("accepted_trips", "trips_change", "trips_change_pct"),
            ("recorded_charge_usd", "charge_change_usd", "charge_change_pct"),
            ("avg_charge_usd", "avg_charge_change_usd", "avg_charge_change_pct"),
            ("trips_per_calendar_day", "trips_per_day_change", "trips_per_day_change_pct"),
        ):
            valid = adjacent and row[value_field] is not None and previous[value_field] is not None
            delta = (
                Decimal(str(row[value_field])) - Decimal(str(previous[value_field]))
                if valid
                else None
            )
            pct = (
                float(delta / Decimal(str(previous[value_field])) * 100)
                if valid and Decimal(str(previous[value_field]))
                else None
            )
            _require(_close(row[delta_field], delta) and _close(row[pct_field], pct), "MoM formula")
        q = quality[index]
        source = sum(a["source_rows"] for a in artifacts)
        rejected = sum(a["quarantined_rows"] for a in artifacts)
        _require(
            q["source_trips"] == source
            and q["accepted_trips"] == count
            and q["quarantined_trips"] == rejected,
            "quality counts",
        )
        _require(0 <= q["any_warning_trips"] <= count, "distinct warned count")
        _require(
            _close(q["warning_rate_pct"], _rate(q["any_warning_trips"], count)), "warning rate"
        )
        _require(_close(q["quarantine_rate_pct"], _rate(rejected, source)), "quarantine rate")
        coverage = [r for r in tables["pickup_month_coverage"] if r["source_month"] == month]
        _require(sum(r["accepted_trips"] for r in coverage) == count, "pickup-month count")
        _require(
            sum((Decimal(r["recorded_charge_usd"]) for r in coverage), Decimal(0)) == charge,
            "pickup-month charge",
        )
        in_month = [r for r in coverage if r["pickup_month"] == month]
        _require(
            sum(r["accepted_trips"] for r in in_month) == row["in_partition_trips"],
            "pickup-month exclusion",
        )
        grouped = [r for r in tables["quality_zones"] if r["source_month"] == month]
        _require(
            sum(r["source_trips"] for r in grouped) == source
            and sum(r["accepted_trips"] for r in grouped) == count
            and sum(r["any_warning_trips"] for r in grouped) == q["any_warning_trips"]
            and sum(r["quarantined_trips"] for r in grouped) == rejected,
            "quality-zone sums",
        )
        for group in grouped:
            _require(
                group["source_trips"] == group["accepted_trips"] + group["quarantined_trips"],
                "quality-zone dispositions",
            )
            _require(
                _close(
                    group["warning_rate_pct"],
                    _rate(group["any_warning_trips"], group["accepted_trips"]),
                )
                and _close(
                    group["quarantine_rate_pct"],
                    _rate(group["quarantined_trips"], group["source_trips"]),
                ),
                "quality-zone rate",
            )
            _require(group["minimum_volume"] == MINIMUM_ZONE_VOLUME, "group threshold")
            for label, denominator in (
                ("warning", "accepted_trips"),
                ("quarantine", "source_trips"),
            ):
                baseline = q[f"{label}_rate_pct"]
                rate = group[f"{label}_rate_pct"]
                _require(_close(group[f"month_{label}_rate_pct"], baseline), "month baseline")
                elevated = bool(
                    group[denominator] >= MINIMUM_ZONE_VOLUME
                    and rate is not None
                    and baseline is not None
                    and rate > baseline
                )
                _require(group[f"elevated_{label}_rate"] is elevated, "volume flag")
    for name, grain in (
        ("pickup_month_coverage", ("source_month", "pickup_month")),
        ("quality_zones", ("source_month", "pickup_zone_key")),
        ("warning_codes", ("source_month", "code")),
        ("quarantine_codes", ("source_month", "code")),
    ):
        rows = tables[name]
        _require(len({tuple(r[k] for k in grain) for r in rows}) == len(rows), f"{name} grain")
        _require(all(r["source_month"] in summary["source_months"] for r in rows), "unknown month")
    for name, count_field, summary_field in (
        ("warning_codes", "warning_trips", "accepted_warning_counts"),
        ("quarantine_codes", "quarantined_trips", "reason_counts"),
    ):
        totals = {}
        for row in tables[name]:
            q = next(q for q in quality if q["source_month"] == row["source_month"])
            limit = q["any_warning_trips"] if name == "warning_codes" else q["quarantined_trips"]
            _require(0 < row[count_field] <= limit, "code count")
            if name == "warning_codes":
                _require(
                    row["accepted_trips"] == q["accepted_trips"]
                    and _close(
                        row["warning_rate_pct"], _rate(row[count_field], q["accepted_trips"])
                    ),
                    "code denominator",
                )
            totals[row["code"]] = totals.get(row["code"], 0) + row[count_field]
        _require(totals == summary[summary_field], "code totals")


def run_analysis(
    database: Path, output_root: Path, *, scope: str, verification_paths: list[Path]
) -> dict[str, Any]:
    if not database.is_file():
        raise ValueError(f"Existing verified local database required: {database}")
    reports = [json.loads(p.read_text(encoding="utf-8")) for p in verification_paths]
    reports = [r.get("verification", r) for r in reports]
    if not reports or any(
        r["status"] != "PASSED"
        or r["engine"] != "duckdb"
        or any(Decimal(str(v)) != 0 for v in r["differences"].values())
        for r in reports
    ):
        raise ValueError("Analysis requires passed local per-artifact verification")
    with connect_local(database, read_only=True) as connection:
        connection.execute("BEGIN TRANSACTION")
        cursor = connection.execute(
            "SELECT * FROM control.source_artifacts ORDER BY source_partition"
        )
        fields = [column[0] for column in cursor.description]
        artifacts = [
            {k: _portable(v) for k, v in zip(fields, r, strict=True)} for r in cursor.fetchall()
        ]
        _require(bool(artifacts) and {a["scope"] for a in artifacts} == {scope}, "database scope")
        _require(
            {(r["dataset_id"], r["artifact_id"]) for r in reports}
            == {(a["dataset_id"], a["artifact_id"]) for a in artifacts},
            "verified artifact set",
        )
        for table in ("curated.accepted_trips", "curated.quarantined_trips", "analytics.fct_trip"):
            unknown = connection.execute(f"""SELECT count(*) FROM {table} t WHERE NOT EXISTS (
                SELECT 1 FROM control.source_artifacts a WHERE a.dataset_id=t.dataset_id
                AND a.artifact_id=t.artifact_id
                AND a.source_partition=t.source_partition)""").fetchone()[
                0
            ]
            _require(unknown == 0, "unregistered records")
        for artifact in artifacts:
            report = next(
                r
                for r in reports
                if r["artifact_id"] == artifact["artifact_id"]
                and r["dataset_id"] == artifact["dataset_id"]
            )
            for field in ("source_rows", "accepted_rows", "quarantined_rows", "source_partition"):
                _require(artifact[field] == report[field], "verification receipt drift")
            _require(
                Decimal(artifact["recorded_charge"]) == Decimal(report["recorded_charge_usd"]),
                "verified charge drift",
            )
            for table, count_field in (
                ("curated.accepted_trips", "accepted_rows"),
                ("curated.quarantined_trips", "quarantined_rows"),
                ("analytics.fct_trip", "accepted_rows"),
            ):
                n, distinct, charge = connection.execute(
                    f"""SELECT count(*),
                    count(DISTINCT trip_record_id), coalesce(sum(recorded_charge),0)
                    FROM {table} WHERE dataset_id=? AND artifact_id=?""",
                    [artifact["dataset_id"], artifact["artifact_id"]],
                ).fetchone()
                _require(n == distinct == artifact[count_field], "live database count/keys")
                if count_field == "accepted_rows":
                    _require(charge == Decimal(artifact["recorded_charge"]), "live database charge")
        tables = execute_queries(connection)
        warning_count = connection.execute(
            "SELECT coalesce(sum(warning_trip_count),0) FROM analytics.mart_zone_hour"
        ).fetchone()[0]
        connection.execute("COMMIT")
    summary = {
        "scope": scope,
        "origin_engine": f"duckdb_{scope}",
        "artifacts": artifacts,
        "source_months": sorted({a["source_partition"] for a in artifacts}),
        "source_rows": sum(a["source_rows"] for a in artifacts),
        "accepted_rows": sum(a["accepted_rows"] for a in artifacts),
        "recorded_charge_usd": str(
            sum((Decimal(a["recorded_charge"]) for a in artifacts), Decimal(0))
        ),
        "accepted_warning_counts": {},
        "reason_counts": {},
        "accepted_any_warning_trips": int(warning_count),
    }
    for name, count_field, summary_field in (
        ("warning_codes", "warning_trips", "accepted_warning_counts"),
        ("quarantine_codes", "quarantined_trips", "reason_counts"),
    ):
        for r in tables[name]:
            summary[summary_field][r["code"]] = (
                summary[summary_field].get(r["code"], 0) + r[count_field]
            )
    bundle = {
        "analysis_version": ANALYSIS_VERSION,
        "status": "PASSED",
        "generated_at": utc_now(),
        "scope": scope,
        "origin_engine": f"duckdb_{scope}",
        "source_months": summary["source_months"],
        "artifacts": artifacts,
        "minimum_zone_volume": MINIMUM_ZONE_VOLUME,
        "definitions": DEFINITIONS,
        "tables": tables,
        "queries": {
            name: {
                "path": f"sql/analysis/{name}.sql",
                "sha256": sha256_file(ROOT / f"sql/analysis/{name}.sql"),
            }
            for name in QUERY_NAMES
        },
        "verification_inputs": [
            {"path": p.as_posix(), "sha256": sha256_file(p)} for p in verification_paths
        ],
        "reconciliation": {
            "status": "PASSED",
            "accepted_rows": summary["accepted_rows"],
            "source_rows": summary["source_rows"],
            "recorded_charge_usd": summary["recorded_charge_usd"],
            "count_difference": 0,
            "charge_difference_usd": "0.00",
        },
    }
    validate_analysis(bundle, summary)
    output_root.mkdir(parents=True, exist_ok=True)
    bundle["result_files"] = {}
    for name, rows in tables.items():
        path = output_root / f"{name}.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            if rows:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        bundle["result_files"][name] = {
            "path": path.as_posix(),
            "sha256": sha256_file(path),
            "rows": len(rows),
            "bytes": path.stat().st_size,
        }
    write_manifest_atomically(output_root / "business_insights.json", bundle)
    return bundle
