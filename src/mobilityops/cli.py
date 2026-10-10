"""Small PowerShell-friendly CLI for fixture generation and source profiling."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.fixture import generate_fixture
from mobilityops.identity import trip_record_id
from mobilityops.manifest import record_failed_profile_attempt, record_profiled_source
from mobilityops.profile import profile_parquet
from mobilityops.real_source import prepare_real_source
from mobilityops.snapshot import load_snapshot
from mobilityops.workflow import STAGES, run_workflow


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobilityops")
    parser.add_argument("--config", default="config/project.yaml")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("generate-fixture", help="write the local 12-row Parquet fixture")

    profile = commands.add_parser("profile", help="profile the configured fixture Parquet")
    profile.add_argument("--path", help="optional Parquet path override")
    profile.add_argument("--run-id", help="optional run ID for this local attempt")
    profile.add_argument("--attempt-id", help="optional attempt ID for this local attempt")
    profile.add_argument("--zone-lookup", help="optional zone CSV path override")

    real_source = commands.add_parser(
        "prepare-real-source",
        help="download, profile, contract-check, and sample the configured TLC month",
    )
    real_source.add_argument("--run-id", help="optional local source-preparation run ID")
    real_source.add_argument("--attempt-id", help="optional local source-preparation attempt ID")
    workflow = commands.add_parser(
        "run-local", help="run the complete credential-free local workflow"
    )
    workflow.add_argument("--scope", choices=["fixture", "sample", "real"], default="fixture")
    workflow.add_argument("--month")
    workflow.add_argument("--offline", action="store_true")
    workflow.add_argument("--stage", choices=["all", *STAGES], default="all")
    validation = commands.add_parser("validate-snapshot")
    validation.add_argument("--path", default="demo/snapshots")
    return parser


def _run_generate_fixture(config_path: str) -> int:
    config = load_project_config(config_path)
    artifacts = generate_fixture(
        config.resolve_path("fixture_parquet"),
        config.resolve_path("zone_lookup_csv"),
    )
    print(json.dumps(artifacts.as_dict(), indent=2, sort_keys=True))
    return 0


def _run_profile(args: argparse.Namespace) -> int:
    config = load_project_config(args.config)
    contract = load_yellow_taxi_contract(config)
    artifact_path = Path(args.path) if args.path else config.resolve_path("fixture_parquet")
    zone_path = (
        Path(args.zone_lookup) if args.zone_lookup else config.resolve_path("zone_lookup_csv")
    )
    run_id = args.run_id or str(uuid.uuid4())
    attempt_id = args.attempt_id or str(uuid.uuid4())

    try:
        profile = profile_parquet(artifact_path)
        manifest = record_profiled_source(
            config=config,
            contract=contract,
            profile=profile,
            artifact_path=artifact_path,
            zone_lookup_path=zone_path,
            run_id=run_id,
            attempt_id=attempt_id,
        )
    except Exception as error:
        record_failed_profile_attempt(
            config=config,
            run_id=run_id,
            attempt_id=attempt_id,
            error_category=type(error).__name__,
        )
        print(
            f"Profile failed ({type(error).__name__}). See attempt history for this attempt.",
            file=sys.stderr,
        )
        return 1

    identity_summary = {
        "source_row_id_count": profile.row_count,
        "distinct_source_row_id_count": len(
            {
                trip_record_id(profile.source_artifact_id, row)
                for row in range(1, profile.row_count + 1)
            }
        ),
        "row_1_trip_record_id": trip_record_id(profile.source_artifact_id, 1)
        if profile.row_count >= 1
        else None,
        "row_2_trip_record_id": trip_record_id(profile.source_artifact_id, 2)
        if profile.row_count >= 2
        else None,
    }
    result = {
        "profile": profile.as_dict(),
        "identity": identity_summary,
        "manifest_path": str(config.resolve_path("manifest_json")),
        "attempt_history_path": str(config.resolve_path("attempt_history_jsonl")),
        "manifest_status": manifest["status"],
        "classification_run": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _run_prepare_real_source(args: argparse.Namespace) -> int:
    config = load_project_config(args.config)
    contract = load_yellow_taxi_contract(config)
    result = prepare_real_source(
        config=config,
        contract=contract,
        run_id=args.run_id,
        attempt_id=args.attempt_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parser().parse_args(argv)
    try:
        if args.command == "generate-fixture":
            return _run_generate_fixture(args.config)
        if args.command == "profile":
            return _run_profile(args)
        if args.command == "prepare-real-source":
            return _run_prepare_real_source(args)
        if args.command == "run-local":
            result = run_workflow(
                load_project_config(args.config),
                scope=args.scope,
                month=args.month,
                offline=args.offline,
                stage=args.stage,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.command == "validate-snapshot":
            _, _, summary, _ = load_snapshot(Path(args.path))
            print(
                json.dumps(
                    {
                        "status": "VALIDATED",
                        "origin_engine": summary["origin_engine"],
                        "source_rows": summary["source_rows"],
                    },
                    indent=2,
                )
            )
            return 0
    except Exception as error:
        print(f"MobilityOps command failed ({type(error).__name__}): {error}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
