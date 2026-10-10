"""Recruiter dashboard. Every data access is through validated local snapshots."""

from __future__ import annotations

import os
from pathlib import Path

import plotly.express as px
import streamlit as st

from mobilityops.snapshot import filtered_metrics, load_snapshot

ROOT = Path(__file__).resolve().parents[1]


def main():
    st.set_page_config(page_title="MobilityOps", page_icon="🚕", layout="wide")
    st.title("MobilityOps · Taxi operations and reliability")
    path = Path(os.environ.get("MOBILITYOPS_SNAPSHOT_DIR", str(ROOT / "demo/published")))
    try:
        zone, daily, summary, _ = load_snapshot(path)
    except Exception as error:
        st.error(f"Cannot load validated MobilityOps snapshots: {error}")
        st.code(
            ".\\scripts\\run_local.ps1 -Scope fixture -Offline\n"
            '$env:MOBILITYOPS_SNAPSHOT_DIR = "$PWD\\demo\\fixture"'
        )
        st.stop()
    st.caption(
        f"Origin: {summary['origin_engine']} · Scope: {summary['scope']} · "
        f"Source months: {', '.join(summary['source_months'])} · "
        f"Actual pickup dates: {summary['actual_date_coverage'][0]} → "
        f"{summary['actual_date_coverage'][1]} · Export: {summary['exported_at']}"
    )
    if summary["sampling_note"]:
        st.info(summary["sampling_note"])
    operations, reliability = st.tabs(["Operations", "Reliability"])
    with operations:
        dates = sorted(zone.pickup_date.unique())
        selected_dates = st.date_input(
            "Pickup date range", (dates[0], dates[-1]), min_value=dates[0], max_value=dates[-1]
        )
        zone_options = sorted(zone.zone_name.unique())
        selected_zones = st.multiselect("Pickup zones (empty means all)", zone_options)
        if not selected_dates:
            st.info("Choose at least one pickup date.")
            st.stop()
        start, end = selected_dates[0], selected_dates[-1]
        selected = zone[zone.pickup_date.between(start, end)]
        if selected_zones:
            selected = selected[selected.zone_name.isin(selected_zones)]
        metrics = filtered_metrics(selected)
        columns = st.columns(4)
        columns[0].metric("Accepted trips", f"{metrics['trips']:,}")
        columns[1].metric("Recorded passenger charge (USD)", f"${metrics['charge_usd']:,.2f}")
        columns[2].metric(
            "Average distance (miles)",
            "—"
            if metrics["average_distance_miles"] is None
            else f"{metrics['average_distance_miles']:.2f}",
        )
        columns[3].metric(
            "Average duration (minutes)",
            "—"
            if metrics["average_duration_minutes"] is None
            else f"{metrics['average_duration_minutes']:.2f}",
        )
        st.caption(
            f"{metrics['warning_trips']:,} accepted trips have warnings. "
            "Averages use summed distance/duration divided by their record counts. "
            "Signed recorded charges include adjustments; cash tips are incomplete."
        )
        if selected.empty:
            st.info("No accepted trips match these filters.")
        else:
            demand = selected.groupby(["zone_name", "pickup_hour"], as_index=False).trip_count.sum()
            top_zones = demand.groupby("zone_name").trip_count.sum().nlargest(12).index
            demand = demand[demand.zone_name.isin(top_zones)]
            st.caption(
                "Hourly demand for the 12 busiest pickup zones in the current filters. "
                "The metrics above include every selected zone."
            )
            st.plotly_chart(
                px.bar(
                    demand,
                    x="pickup_hour",
                    y="trip_count",
                    color="zone_name",
                    labels={
                        "pickup_hour": "Pickup hour (New York wall clock)",
                        "trip_count": "Accepted trips",
                        "zone_name": "Zone",
                    },
                ),
                width="stretch",
            )
            st.dataframe(selected.drop(columns=["artifact_id"]), hide_index=True)
    with reliability:
        columns = st.columns(3)
        for column, field, label in zip(
            columns,
            ["source_rows", "accepted_rows", "quarantined_rows"],
            ["Source records", "Accepted records", "Quarantined records"],
            strict=True,
        ):
            column.metric(label, f"{summary[field]:,}")
        st.success("All saved artifact reconciliation gates passed at export time.")
        st.dataframe(
            [
                {
                    "Source month": r["source_partition"],
                    "Source": r["source_rows"],
                    "Accepted": r["accepted_rows"],
                    "Quarantined": r["quarantined_rows"],
                    "Fact": r["fact_rows"],
                    "Charge USD": r["recorded_charge_usd"],
                    "Gates": r["status"],
                }
                for r in summary["reconciliation"]
            ],
            hide_index=True,
        )
        with st.expander("Artifact provenance and exact reconciliation differences"):
            st.json(summary["reconciliation"])
        st.subheader("Historical replay and recovery evidence")
        st.dataframe(
            [
                {
                    "Committed": r["committed_at"],
                    "Dataset": r["dataset_id"],
                    "Accepted inserted": r["accepted_inserted"],
                    "Quarantine inserted": r["quarantined_inserted"],
                    "Replay records": r["replay_records"],
                    "Duration seconds": r["elapsed_seconds"],
                }
                for r in summary["ingestion_audit"]
            ],
            hide_index=True,
        )
        if summary.get("isolated_fixture_recovery_and_backfill"):
            with st.expander("Verified isolated fixture rollback/retry and older-month backfill"):
                st.json(summary["isolated_fixture_recovery_and_backfill"])
        st.caption(
            "Committed audits record actual inserted keys and replayed keys. "
            "Failed attempts are saved locally in the evidence reports. "
            "These are historical local results, not live warehouse status."
        )
        left, right = st.columns(2)
        left.write("Quarantine reasons")
        left.dataframe(
            [{"reason": k, "records": v} for k, v in summary["reason_counts"].items()],
            hide_index=True,
        )
        right.write("Accepted warnings")
        right.dataframe(
            [{"warning": k, "records": v} for k, v in summary["accepted_warning_counts"].items()],
            hide_index=True,
        )
        st.caption(
            "A quarantined record can have several reasons, so reason totals may exceed "
            "quarantined record totals. Missing pickup dates have their own bucket."
        )
        st.dataframe(daily, hide_index=True)
    st.caption(
        "Portable snapshots · DuckDB local implementation · Snowflake remains manual · "
        "Distance: miles · Duration: minutes in averages, seconds in exports · Charge: USD"
    )


if __name__ == "__main__":
    main()
