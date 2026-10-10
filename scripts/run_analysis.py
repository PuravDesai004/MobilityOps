"""Run local read-only SQL and optionally attach its validated export to a snapshot."""

import argparse
from pathlib import Path

from mobilityops.analysis import run_analysis
from mobilityops.snapshot import attach_analysis_snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default="data/db/real.duckdb")
    parser.add_argument("--output", default="reports/analysis/real")
    parser.add_argument("--scope", choices=["real", "fixture", "sample"], default="real")
    parser.add_argument("--verification", nargs="+")
    parser.add_argument("--snapshot", help="Existing validated bundle to enrich atomically")
    args = parser.parse_args()
    if not args.verification and args.scope != "real":
        parser.error("Fixture/sample analysis requires explicit --verification paths")
    verification = (
        [Path(p) for p in args.verification]
        if args.verification
        else sorted(Path("reports/evidence").glob("real_????-??.json"))
    )
    bundle = run_analysis(
        Path(args.database), Path(args.output), scope=args.scope, verification_paths=verification
    )
    if args.snapshot:
        attach_analysis_snapshot(Path(args.snapshot), Path(args.output) / "business_insights.json")
    print(
        f"Analysis PASSED: {bundle['scope']}; {bundle['reconciliation']['accepted_rows']:,} "
        f"accepted; charge USD {bundle['reconciliation']['recorded_charge_usd']}"
    )
    print(f"Results: {args.output}; snapshot attachment: {args.snapshot or 'not requested'}")


if __name__ == "__main__":
    main()
