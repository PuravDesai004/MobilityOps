import copy
import json
import shutil
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from mobilityops.analysis import (
    ANALYSIS_VERSION,
    DEFINITIONS,
    MINIMUM_ZONE_VOLUME,
    execute_queries,
    run_analysis,
    validate_analysis,
)
from mobilityops.identity import sha256_file
from mobilityops.snapshot import attach_analysis_snapshot, load_snapshot


@pytest.fixture
def analysis_database(tmp_path):
    """Synthetic small analytical tables; never read the private real database or download."""
    database = tmp_path / "analysis.duckdb"
    with duckdb.connect(str(database)) as c:
        for schema in ("analytics", "curated", "control"):
            c.execute(f"CREATE SCHEMA {schema}")
        c.execute("""CREATE TABLE curated.accepted_trips (
            trip_record_id VARCHAR, dataset_id VARCHAR, artifact_id VARCHAR,
            source_partition VARCHAR, pickup_date DATE, pickup_hour BIGINT,
            pickup_zone_key BIGINT, recorded_charge DECIMAL(18,2),
            duration_seconds DOUBLE, warning_codes VARCHAR[], reason_codes VARCHAR[])""")
        c.execute(
            "CREATE TABLE curated.quarantined_trips AS SELECT * FROM curated.accepted_trips LIMIT 0"
        )
        c.execute("""CREATE TABLE control.source_artifacts (
            dataset_id VARCHAR, artifact_id VARCHAR, source_partition VARCHAR, scope VARCHAR,
            source_rows BIGINT, accepted_rows BIGINT, quarantined_rows BIGINT,
            recorded_charge DECIMAL(38,2))""")
        c.execute("""CREATE TABLE analytics.dim_zone AS SELECT * FROM (VALUES
            (0,'Unresolved','Unknown'), (1,'Central','Manhattan'), (264,'Unknown','Unknown'))
            t(zone_key,zone_name,borough)""")
        # January: zero accepted denominator. February: one record with two distinct codes.
        c.execute("""INSERT INTO curated.quarantined_trips VALUES
            ('q','j','ja','2026-01',NULL,NULL,0,NULL,NULL,[],['MISSING_PICKUP'])""")
        c.execute("""INSERT INTO curated.accepted_trips VALUES
            ('f','f','fa','2026-02','2026-02-01',10,1,10,600,['A','B','A'],[])""")
        for i in range(9):
            c.execute(
                "INSERT INTO curated.accepted_trips VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    f"m{i}",
                    "m",
                    "ma",
                    "2026-03",
                    "2026-02-28" if i == 0 else "2026-03-01",
                    10 if i < 5 else 11,
                    264 if i == 0 else 1,
                    Decimal("1.00"),
                    60,
                    ["A"] if i == 0 else [],
                    [],
                ],
            )
        for values in [
            ("j", "ja", "2026-01", "fixture", 1, 0, 1, 0),
            ("f", "fa", "2026-02", "fixture", 1, 1, 0, 10),
            ("m", "ma", "2026-03", "fixture", 9, 9, 0, 9),
        ]:
            c.execute("INSERT INTO control.source_artifacts VALUES (?,?,?,?,?,?,?,?)", values)
        c.execute("CREATE TABLE analytics.fct_trip AS SELECT * FROM curated.accepted_trips")
        c.execute("""CREATE TABLE analytics.mart_zone_hour AS SELECT
            dataset_id, artifact_id, source_partition, pickup_date, pickup_hour, pickup_zone_key,
            count(*)::BIGINT AS trip_count, sum(recorded_charge) AS recorded_charge_sum,
            sum(duration_seconds) AS duration_seconds_sum, count(*) AS duration_count,
            count(*) FILTER (WHERE len(warning_codes)>0) AS warning_trip_count
            FROM analytics.fct_trip GROUP BY ALL""")
    return database


def fixture_reports(database, tmp_path):
    with duckdb.connect(str(database), read_only=True) as c:
        cursor = c.execute("SELECT * FROM control.source_artifacts ORDER BY source_partition")
        fields = [field[0] for field in cursor.description]
        rows = [dict(zip(fields, r, strict=True)) for r in cursor.fetchall()]
    paths = []
    for row in rows:
        report = {
            **row,
            "recorded_charge_usd": str(row["recorded_charge"]),
            "status": "PASSED",
            "engine": "duckdb",
            "differences": {"count": 0, "charge": "0.00"},
        }
        report.pop("recorded_charge")
        path = tmp_path / f"{row['source_partition']}.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        paths.append(path)
    return paths


def test_sql_multiple_warnings_and_zone_buckets(analysis_database):
    with duckdb.connect(str(analysis_database), read_only=True) as c:
        tables = execute_queries(c)
    feb = tables["quality_month"][1]
    assert feb["any_warning_trips"] == 1 and feb["warning_rate_pct"] == 100
    codes = [r for r in tables["warning_codes"] if r["source_month"] == "2026-02"]
    assert {r["code"]: r["warning_trips"] for r in codes} == {"A": 1, "B": 1}
    assert sum(r["warning_trips"] for r in codes) == 2
    groups = tables["quality_zones"]
    assert {r["pickup_zone_key"] for r in groups} == {0, 1, 264}
    assert all(not r["elevated_warning_rate"] for r in groups)  # Tiny denominators stay visible.
    assert all(r["minimum_volume"] == 10000 for r in groups)


def test_sql_mom_zero_denominators_and_coverage(analysis_database):
    with duckdb.connect(str(analysis_database), read_only=True) as c:
        months = execute_queries(c)["monthly_changes"]
    jan, feb, mar = months
    assert jan["trips_change_pct"] is None and jan["avg_charge_usd"] is None
    assert feb["trips_change"] == 1 and feb["trips_change_pct"] is None
    assert feb["charge_change_pct"] is None and feb["avg_charge_change_usd"] is None
    assert [r["calendar_days"] for r in months] == [31, 28, 31]
    assert mar["accepted_trips"] == 9 and mar["in_partition_trips"] == 8
    assert mar["excluded_out_of_partition_trips"] == 1 and mar["excluded_charge_usd"] == "1.00"
    assert mar["trips_per_calendar_day"] == pytest.approx(8 / 31)
    assert mar["trips_change_pct"] == pytest.approx(800)
    assert mar["trips_per_day_change_pct"] == pytest.approx((8 / 31 / (1 / 28) - 1) * 100)


def test_sql_gap_is_not_a_month_over_month_comparison(analysis_database):
    with duckdb.connect(str(analysis_database)) as c:
        c.execute("DELETE FROM control.source_artifacts WHERE source_partition='2026-02'")
        months = execute_queries(c)["monthly_changes"]
    assert months[-1]["previous_source_month"] == "2026-01"
    assert months[-1]["trips_change"] is None and months[-1]["trips_change_pct"] is None


def test_sql_weighted_average_and_deterministic_hour_ties(analysis_database):
    with duckdb.connect(str(analysis_database), read_only=True) as c:
        zones = execute_queries(c)["pickup_zone_hour"]
    central = next(r for r in zones if r["pickup_zone_key"] == 1)
    assert central["accepted_trips"] == 9
    assert central["avg_duration_minutes"] == pytest.approx((600 + 8 * 60) / 9 / 60)
    assert central["peak_pickup_hour"] == 10
    assert sum(r["accepted_share_pct"] for r in zones) == pytest.approx(100)


def test_analysis_read_only_exports_and_reconciliation(analysis_database, tmp_path):
    before = sha256_file(analysis_database)
    bundle = run_analysis(
        analysis_database,
        tmp_path / "results",
        scope="fixture",
        verification_paths=fixture_reports(analysis_database, tmp_path),
    )
    assert sha256_file(analysis_database) == before
    assert bundle["reconciliation"]["accepted_rows"] == 10
    assert bundle["reconciliation"]["recorded_charge_usd"] == "19.00"
    assert bundle["tables"]["quality_month"][0]["quarantine_rate_pct"] == 100
    for receipt in bundle["result_files"].values():
        assert sha256_file(Path(receipt["path"])) == receipt["sha256"]
    with pytest.raises(ValueError, match="scope"):
        run_analysis(
            analysis_database,
            tmp_path / "bad",
            scope="real",
            verification_paths=fixture_reports(analysis_database, tmp_path),
        )


def test_sql_volume_threshold_allows_large_elevated_groups(analysis_database):
    with duckdb.connect(str(analysis_database)) as c:
        c.execute("""INSERT INTO curated.accepted_trips SELECT 'large-' || i::VARCHAR,
            'm','ma','2026-03','2026-03-02'::DATE,10,264,1,60,['A'],[] FROM range(10000) t(i)""")
        groups = execute_queries(c)["quality_zones"]
    group = next(
        r for r in groups if r["source_month"] == "2026-03" and r["pickup_zone_key"] == 264
    )
    assert group["accepted_trips"] == 10001 and group["elevated_warning_rate"] is True
    assert group["warning_rate_pct"] > group["month_warning_rate_pct"]


def test_analysis_rejects_unregistered_and_changed_counts(analysis_database, tmp_path):
    reports = fixture_reports(analysis_database, tmp_path)
    with duckdb.connect(str(analysis_database)) as c:
        c.execute(
            "UPDATE analytics.fct_trip SET artifact_id='unregistered' WHERE trip_record_id='f'"
        )
    with pytest.raises(ValueError, match="unregistered"):
        run_analysis(
            analysis_database, tmp_path / "bad", scope="fixture", verification_paths=reports
        )


def test_enriched_snapshot_offline_validation_and_failed_attach(tmp_path, monkeypatch):
    import socket

    def denied(*args, **kwargs):
        raise AssertionError("Snapshot analysis attempted network")

    monkeypatch.setattr(socket, "create_connection", denied)
    root = tmp_path / "snapshot"
    shutil.copytree("demo/published", root)
    _, _, summary, manifest = load_snapshot(root)
    # Published real analysis is committed as a small offline bundle, never a private DB dependency.
    bundle = copy.deepcopy(summary["business_insights"])
    assert bundle["analysis_version"] == ANALYSIS_VERSION and bundle["definitions"] == DEFINITIONS
    assert bundle["minimum_zone_volume"] == MINIMUM_ZONE_VOLUME
    path = tmp_path / "analysis.json"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    attach_analysis_snapshot(root, path)
    _, _, enriched, _ = load_snapshot(root)
    assert enriched["business_insights"]["reconciliation"]["count_difference"] == 0
    before = (root / "snapshot_manifest.json").read_bytes()
    bundle["scope"] = "sample"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    with pytest.raises(ValueError, match="scope"):
        attach_analysis_snapshot(root, path)
    assert (root / "snapshot_manifest.json").read_bytes() == before
    assert "business_insights.json" in manifest["files"]


@pytest.mark.parametrize("change", ["warning", "mom", "artifact", "charge", "volume", "nonfinite"])
def test_analysis_semantics_even_with_updated_file_checksum(change):
    _, _, summary, _ = load_snapshot(Path("demo/published"))
    bundle = copy.deepcopy(summary["business_insights"])
    if change == "warning":
        bundle["tables"]["quality_month"][0]["any_warning_trips"] += 1
    elif change == "mom":
        bundle["tables"]["monthly_changes"][1]["trips_change_pct"] = 0.0
    elif change == "artifact":
        bundle["artifacts"][0]["artifact_id"] = "different"
    elif change == "charge":
        bundle["tables"]["pickup_zone_hour"][0]["recorded_charge_usd"] = "0.00"
    elif change == "volume":
        bundle["tables"]["quality_zones"][0]["minimum_volume"] = 1
    else:
        bundle["tables"]["monthly_changes"][0]["avg_charge_usd"] = float("nan")
    with pytest.raises(ValueError, match="Analysis validation"):
        validate_analysis(bundle, summary)


def test_snapshot_rejects_bad_insight_with_updated_checksum(tmp_path):
    root = tmp_path / "snapshot"
    shutil.copytree("demo/published", root)
    manifest_path = root / "snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = manifest["files"]["business_insights.json"]
    path = root / receipt["path"]
    bundle = json.loads(path.read_text(encoding="utf-8"))
    bundle["tables"]["pickup_zone_hour"][0]["avg_duration_minutes"] += 1
    path.write_text(json.dumps(bundle), encoding="utf-8")
    receipt.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="zone/hour/duration"):
        load_snapshot(root)


def test_actual_business_tab_is_snapshot_only_and_scope_fixed(monkeypatch):
    import socket

    import requests
    from streamlit.testing.v1 import AppTest

    def denied(*args, **kwargs):
        raise AssertionError("Dashboard attempted database or network access")

    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(requests.Session, "request", denied)
    monkeypatch.setattr(duckdb, "connect", denied)
    monkeypatch.setenv("MOBILITYOPS_SNAPSHOT_DIR", str(Path("demo/published").resolve()))
    app = AppTest.from_file(str(Path("app/streamlit_app.py").resolve()), default_timeout=30).run()
    assert not app.exception
    assert [t.label for t in app.tabs] == ["Operations", "Reliability", "Business Insights"]
    assert app.metric[7].value == "479,826" and app.metric[9].value == "3,186,377"
    app.multiselect[0].set_value(["Upper East Side South"]).run()
    assert not app.exception and app.metric[0].value == "479,826"
    assert app.metric[7].value == "479,826" and app.metric[9].value == "3,186,377"
    assert any("Operations filters do not change" in caption.value for caption in app.caption)
