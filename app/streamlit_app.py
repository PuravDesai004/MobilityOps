"""Recruiter dashboard. Every data access is through validated local snapshots."""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from mobilityops.snapshot import filtered_metrics, load_snapshot

ROOT = Path(__file__).resolve().parents[1]


def main():
    st.set_page_config(page_title="MobilityOps", page_icon="🚕", layout="wide")
    st.markdown(
        '<style>[data-testid="stMainBlockContainer"] {padding-top: 2rem;}</style>',
        unsafe_allow_html=True,
    )
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
    operations, reliability, insights = st.tabs(["Operations", "Reliability", "Business Insights"])
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
                "Observed Yellow Taxi pickup activity for the 12 busiest zones in these filters. "
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
                height=400,
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
        st.subheader("Quality evidence · distinct records versus code counts")
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
            "A record can have multiple warnings or reasons, so code counts can overlap. "
            "Distinct any-warning rates are in Business Insights. "
            "Missing pickup dates have their own reconciliation bucket."
        )
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
        st.dataframe(daily, hide_index=True)
    with insights:
        analysis = summary.get("business_insights")
        if not analysis:
            st.info(
                "This snapshot has no SQL-analysis export. "
                "Operations and Reliability remain available."
            )
        else:
            tables = analysis["tables"]
            st.caption(
                f"Fixed SQL findings · All source months: {', '.join(analysis['source_months'])} · "
                f"Generated: {analysis['generated_at']} · "
                "Operations filters do not change this view."
            )
            top = tables["pickup_zone_hour"][0] if tables["pickup_zone_hour"] else None
            months = tables["monthly_changes"]
            quality = tables["quality_month"]
            columns = st.columns(3)
            if top:
                columns[0].metric("Busiest zone · accepted records", f"{top['accepted_trips']:,}")
                columns[0].caption(
                    f"{top['zone_name']} · "
                    f"{top['accepted_share_pct']:.2f}% "
                    f"of scope · Peak hour {top['peak_pickup_hour']:02d}:00–"
                    f"{top['peak_pickup_hour'] + 1:02d}:00 New York"
                )
            latest = months[-1]
            change = latest["trips_per_day_change_pct"]
            columns[1].metric(
                f"{latest['source_month']} in-month trips/day",
                f"{latest['trips_per_calendar_day']:,.0f}",
                f"{change:+.2f}% vs prior month" if change is not None else None,
            )
            warned = sum(r["any_warning_trips"] for r in quality)
            rate = 100 * warned / summary["accepted_rows"] if summary["accepted_rows"] else None
            columns[2].metric("Accepted records with warnings", f"{warned:,}")
            columns[2].caption(f"{rate:.2f}% of accepted" if rate is not None else "No denominator")
            st.subheader("A · Where observed pickups concentrate")
            zone_data = pd.DataFrame(tables["pickup_zone_hour"][:10])
            if not zone_data.empty:
                st.plotly_chart(
                    px.bar(
                        zone_data,
                        x="zone_name",
                        y="accepted_trips",
                        labels={"zone_name": "Pickup zone", "accepted_trips": "Accepted records"},
                    ),
                    width="stretch",
                    height=320,
                )
                st.dataframe(
                    zone_data[
                        [
                            "zone_name",
                            "accepted_trips",
                            "accepted_share_pct",
                            "peak_pickup_hour",
                            "avg_duration_minutes",
                        ]
                    ],
                    hide_index=True,
                )
            st.caption(
                "Possible positioning/staffing planning input, not a proven staffing outcome. "
                "Yellow Taxi activity does not measure all transportation demand. "
                "All accepted dates included."
            )
            st.subheader("B · Compare months with explicit denominators")
            monthly_display = pd.DataFrame(months)[
                [
                    "source_month",
                    "accepted_trips",
                    "recorded_charge_usd",
                    "avg_charge_usd",
                    "calendar_days",
                    "observed_in_partition_days",
                    "excluded_out_of_partition_trips",
                    "trips_per_calendar_day",
                    "trips_change_pct",
                    "trips_per_day_change_pct",
                ]
            ]
            st.dataframe(monthly_display, hide_index=True)
            excluded = sum(r["excluded_out_of_partition_trips"] for r in months)
            excluded_charge = sum((Decimal(r["excluded_charge_usd"]) for r in months), Decimal(0))
            st.caption(
                "Source-file totals retain every accepted record. "
                "Per-calendar-day comparison excludes "
                f"{excluded:,} out-of-partition records (${excluded_charge:,.2f} signed charge). "
                "Observed dates do not establish complete reporting. "
                "Different weekdays/seasons remain confounders."
            )
            st.subheader("C · Retain useful trips and show quality gaps")
            st.dataframe(pd.DataFrame(quality), hide_index=True)
            with st.expander("Warning codes and high-volume zone groups"):
                st.dataframe(pd.DataFrame(tables["warning_codes"]), hide_index=True)
                elevated = [r for r in tables["quality_zones"] if r["elevated_warning_rate"]]
                st.dataframe(pd.DataFrame(elevated), hide_index=True)
            st.caption(
                "Any-warning counts each accepted record once. Code counts can overlap. "
                "Elevated zone flags need ≥10,000 accepted/source records "
                "in the relevant denominator and exceed their source-month rate. "
                "Unresolved and official Unknown zones remain visible. "
                "Warnings describe missing/unusual fields, not proven incorrect trips."
            )
            with st.expander("SQL, artifact identities and analysis checks"):
                st.json(
                    {
                        k: analysis[k]
                        for k in ("queries", "artifacts", "definitions", "reconciliation")
                    }
                )
    st.caption(
        "Portable snapshots · DuckDB local implementation · Snowflake remains manual · "
        "Distance: miles · Duration: minutes in averages, seconds in exports · Charge: USD"
    )


if __name__ == "__main__":
    main()
