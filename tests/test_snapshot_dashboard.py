import json
import shutil
import socket
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from streamlit.testing.v1 import AppTest

from mobilityops.identity import sha256_file
from mobilityops.snapshot import filtered_metrics, load_snapshot


def deny_network(*args, **kwargs):
    raise AssertionError("Dashboard attempted network access")


def test_snapshot_loader_and_filters_need_no_network(monkeypatch):
    monkeypatch.setattr(socket, "create_connection", deny_network)
    monkeypatch.setattr(requests.Session, "request", deny_network)
    zone, _, summary, _ = load_snapshot(Path("demo/fixture"))
    metrics = filtered_metrics(zone)
    assert summary["origin_engine"] == "duckdb_fixture"
    assert metrics["trips"] == 8
    assert metrics["charge_usd"] == Decimal("134.00")
    filtered = zone[zone.pickup_zone_key == 0]
    assert filtered_metrics(filtered)["trips"] == 1
    assert filtered_metrics(zone.iloc[:0])["average_duration_minutes"] is None


def test_weighted_metrics_do_not_average_group_averages():
    data = pd.DataFrame(
        {
            "trip_count": [1, 9],
            "duration_count": [1, 9],
            "distance_count": [1, 9],
            "recorded_charge_sum": [Decimal("1.00"), Decimal("9.00")],
            "distance_miles_sum": [Decimal("10"), Decimal("9")],
            "duration_seconds_sum": [600, 540],
            "warning_trip_count": [0, 1],
        }
    )
    metrics = filtered_metrics(data)
    assert metrics["average_duration_minutes"] == pytest.approx(1.9)
    assert metrics["average_distance_miles"] == pytest.approx(1.9)
    assert metrics["average_duration_minutes"] != (10 + 1) / 2


def test_missing_and_corrupt_snapshots_fail_clearly(tmp_path):
    with pytest.raises(ValueError, match="missing"):
        load_snapshot(tmp_path)
    shutil.copytree("demo/fixture", tmp_path / "snapshot")
    manifest = json.loads((tmp_path / "snapshot/snapshot_manifest.json").read_text())
    file = tmp_path / "snapshot" / manifest["files"]["zone_hour.parquet"]["path"]
    file.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        load_snapshot(tmp_path / "snapshot")


def test_actual_streamlit_render_and_filter(monkeypatch):
    monkeypatch.setenv("MOBILITYOPS_SNAPSHOT_DIR", str(Path("demo/fixture").resolve()))
    app = AppTest.from_file(str(Path("app/streamlit_app.py").resolve()), default_timeout=30).run()
    assert not app.exception
    assert "duckdb_fixture" in app.caption[0].value
    assert app.metric[0].value == "8"
    app.multiselect[0].set_value(["Unresolved"]).run()
    assert not app.exception
    assert app.metric[0].value == "1"


def test_dashboard_corruption_shows_error(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILITYOPS_SNAPSHOT_DIR", str(tmp_path))
    app = AppTest.from_file(str(Path("app/streamlit_app.py").resolve()), default_timeout=30).run()
    assert not app.exception
    assert "Cannot load validated" in app.error[0].value


def test_dashboard_date_filter_retains_actual_out_of_partition_coverage(monkeypatch):
    zone, _, _, _ = load_snapshot(Path("demo/fixture"))
    dates = sorted(zone.pickup_date.unique())
    monkeypatch.setenv("MOBILITYOPS_SNAPSHOT_DIR", str(Path("demo/fixture").resolve()))
    app = AppTest.from_file(str(Path("app/streamlit_app.py").resolve()), default_timeout=30).run()
    assert not app.exception and app.metric[0].value == "8"
    app.date_input[0].set_value((min(d for d in dates if d.year == 2026), dates[-1])).run()
    assert not app.exception and app.metric[0].value == "7"


def test_snapshot_daily_fractional_count_is_invalid(tmp_path):
    root = tmp_path / "snapshot"
    shutil.copytree("demo/fixture", root)
    manifest_path = root / "snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    receipt = manifest["files"]["daily_reconciliation.csv"]
    path = root / receipt["path"]
    daily = pd.read_csv(path)
    daily["source_count"] = daily.source_count.astype(float)
    daily.loc[0, "source_count"] += 0.1
    daily.to_csv(path, index=False)
    receipt.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="count domain"):
        load_snapshot(root)


@pytest.mark.parametrize("change", ["count_type", "charge", "grain", "partition"])
def test_snapshot_semantics_fail_even_with_updated_checksum(tmp_path, change):
    root = tmp_path / "snapshot"
    shutil.copytree("demo/fixture", root)
    manifest_path = root / "snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    receipt = manifest["files"]["zone_hour.parquet"]
    path = root / receipt["path"]
    table = pq.read_table(path)
    if change == "count_type":
        index = table.schema.get_field_index("trip_count")
        table = table.set_column(index, "trip_count", table["trip_count"].cast(pa.string()))
    else:
        rows = table.to_pylist()
        if change == "charge":
            rows[0]["recorded_charge_sum"] += Decimal("0.01")
        elif change == "grain":
            rows.append(rows[0].copy())
        else:
            rows[0]["source_partition"] = "2099-01"
        table = pa.Table.from_pylist(rows, table.schema)
    pq.write_table(table, path)
    receipt.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Snapshot"):
        load_snapshot(root)


@pytest.mark.parametrize(
    "change", ["count", "fact_count", "charge", "engine", "difference", "identity", "duplicate"]
)
def test_snapshot_rejects_stale_verification_receipts(tmp_path, change):
    root = tmp_path / "snapshot"
    shutil.copytree("demo/fixture", root)
    manifest_path = root / "snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    receipt = manifest["files"]["run_summary.json"]
    path = root / receipt["path"]
    summary = json.loads(path.read_text())
    report = summary["reconciliation"][0]
    if change == "count":
        report["accepted_rows"] -= 1
    elif change == "fact_count":
        report["fact_rows"] -= 1
    elif change == "charge":
        report["recorded_charge_usd"] = "0.00"
    elif change == "engine":
        report["engine"] = "snowflake"
    elif change == "difference":
        report["differences"]["source_minus_dispositions"] = 1
    elif change == "identity":
        report["invalid_identity_rows"] = 1
    else:
        summary["reconciliation"].append(report.copy())
    path.write_text(json.dumps(summary), encoding="utf-8")
    receipt.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="verification"):
        load_snapshot(root)


def test_dashboard_handles_snapshot_with_only_quarantined_records(monkeypatch, tmp_path):
    root = tmp_path / "snapshot"
    shutil.copytree("demo/fixture", root)
    _, daily, summary, manifest = load_snapshot(root)
    # This regression exercises a base snapshot, without any optional analysis export.
    manifest["files"].pop("business_insights.json", None)
    manifest.pop("analysis_generated_at", None)
    summary.pop("business_insights", None)
    zone_path = root / manifest["files"]["zone_hour.parquet"]["path"]
    pq.write_table(pq.read_table(zone_path).slice(0, 0), zone_path)
    daily["accepted_count"] = 0
    daily["quarantined_count"] = daily.source_count
    daily["recorded_charge_sum"] = "0.00"
    daily.to_csv(root / manifest["files"]["daily_reconciliation.csv"]["path"], index=False)
    summary.update(
        accepted_rows=0,
        quarantined_rows=summary["source_rows"],
        recorded_charge_usd="0.00",
        actual_date_coverage=[None, None],
        accepted_warning_counts={},
        reason_counts={"MISSING_PICKUP": summary["source_rows"]},
    )
    for artifact in summary["artifacts"]:
        artifact.update(
            accepted_rows=0, quarantined_rows=artifact["source_rows"], recorded_charge="0.00"
        )
    for report in summary["reconciliation"]:
        report.update(
            accepted_rows=0,
            quarantined_rows=report["source_rows"],
            fact_rows=0,
            mart_trip_count=0,
            recorded_charge_usd="0.00",
            independent_eligible_source_charge_usd="0.00",
        )
    summary_path = root / manifest["files"]["run_summary.json"]["path"]
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    for receipt in manifest["files"].values():
        path = root / receipt["path"]
        receipt.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    (root / "snapshot_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert load_snapshot(root)[0].empty
    monkeypatch.setenv("MOBILITYOPS_SNAPSHOT_DIR", str(root))
    app = AppTest.from_file(str(Path("app/streamlit_app.py").resolve()), default_timeout=30).run()
    assert not app.exception
    assert app.metric[0].value == "0"
    assert any("No accepted trips" in item.value for item in app.info)
    assert len(app.tabs) == 3
    assert any(item.label == "Quarantined records" for item in app.metric)
