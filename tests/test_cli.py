"""CLI defaults and bounded profiling summaries."""

import json
from types import SimpleNamespace

from mobilityops import cli


def test_profile_summary_does_not_materialize_every_row_identity(monkeypatch, capsys):
    profile = SimpleNamespace(
        row_count=10_000_000,
        source_artifact_id="a" * 64,
        as_dict=lambda: {"row_count": 10_000_000},
    )
    monkeypatch.setattr(cli, "profile_parquet", lambda path: profile)
    monkeypatch.setattr(cli, "record_profiled_source", lambda **kwargs: {"status": "PROFILED"})
    calls = []

    def identity(artifact_id, row):
        calls.append(row)
        assert len(calls) <= 2, "CLI must not allocate an identity for every physical row"
        return f"{artifact_id}:{row}"

    monkeypatch.setattr(cli, "trip_record_id", identity)
    assert cli.main(["profile"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["identity"]["source_row_id_count"] == 10_000_000
    assert summary["identity"]["distinct_source_row_id_count"] == 10_000_000
    assert calls == [1, 2]


def test_snapshot_validation_defaults_to_the_shipped_demo():
    assert cli._parser().parse_args(["validate-snapshot"]).path == "demo/published"
