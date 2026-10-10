import hashlib
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from mobilityops.config import load_project_config, load_yellow_taxi_contract
from mobilityops.contract import FileContractError, validate_required_columns
from mobilityops.download import (
    DownloadError,
    HttpStatusError,
    InvalidArtifactError,
    SourceRevisionRequiresReview,
    download_artifact,
    validate_parquet_file,
)
from mobilityops.fixture import generate_fixture
from mobilityops.profile import profile_parquet


class FakeResponse:
    def __init__(self, status_code, chunks=(), headers=None):
        self.status_code = status_code
        self._chunks = list(chunks)
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, chunk_size):
        assert chunk_size > 0
        for chunk in self._chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _temporary_contract(tmp_path: Path):
    config_dir = tmp_path / "config"
    contract_dir = config_dir / "contracts"
    contract_dir.mkdir(parents=True)
    (config_dir / "project.yaml").write_text(
        Path("config/project.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (contract_dir / "yellow_taxi_v1.yaml").write_text(
        Path("config/contracts/yellow_taxi_v1.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return load_yellow_taxi_contract(load_project_config(config_dir / "project.yaml"))


def _fixture_bytes(tmp_path: Path) -> bytes:
    path = tmp_path / "fixture.parquet"
    generate_fixture(path, tmp_path / "zones.csv")
    return path.read_bytes()


def _response(payload: bytes, *, etag='"artifact-v1"'):
    return FakeResponse(
        200,
        [payload],
        {"Content-Length": str(len(payload)), "ETag": etag},
    )


def test_successful_download_streams_validates_hashes_and_publishes_atomically(tmp_path):
    payload = _fixture_bytes(tmp_path)
    target = tmp_path / "downloaded.parquet"
    response = _response(payload)
    session = FakeSession([response])

    result = download_artifact(
        url="https://example.test/yellow.parquet",
        destination=target,
        validator=validate_parquet_file,
        session=session,
    )

    assert result.outcome == "DOWNLOADED"
    assert result.source_artifact_id == hashlib.sha256(payload).hexdigest()
    assert target.read_bytes() == payload
    assert response.closed
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob(".*.part"))


def test_interrupted_stream_retries_and_removes_partial_file(tmp_path):
    payload = _fixture_bytes(tmp_path)
    target = tmp_path / "retried.parquet"
    interrupted = FakeResponse(
        200,
        [payload[:100], requests.exceptions.ConnectionError("connection dropped")],
        {"Content-Length": str(len(payload))},
    )
    session = FakeSession([interrupted, _response(payload)])

    result = download_artifact(
        url="https://example.test/yellow.parquet",
        destination=target,
        validator=validate_parquet_file,
        retry_attempts=2,
        retry_backoff_seconds=0,
        session=session,
        sleep_fn=lambda _seconds: None,
    )

    assert len(session.calls) == 2
    assert result.request_attempts == 2
    assert result.outcome == "DOWNLOADED"
    assert target.read_bytes() == payload
    assert not list(tmp_path.glob(".*.part"))


def test_failed_replacement_preserves_existing_valid_artifact(tmp_path):
    payload = _fixture_bytes(tmp_path)
    target = tmp_path / "active.parquet"
    target.write_bytes(payload)
    original_mtime = target.stat().st_mtime_ns
    broken_response = FakeResponse(
        200,
        [payload[:64], requests.exceptions.ConnectionError("interrupted")],
        {"Content-Length": str(len(payload))},
    )

    with pytest.raises(DownloadError, match="failed after 1 attempts"):
        download_artifact(
            url="https://example.test/yellow.parquet",
            destination=target,
            validator=validate_parquet_file,
            retry_attempts=1,
            session=FakeSession([broken_response]),
            sleep_fn=lambda _seconds: None,
        )

    assert target.read_bytes() == payload
    assert target.stat().st_mtime_ns == original_mtime
    assert not list(tmp_path.glob(".*.part"))


def test_invalid_content_is_not_published_or_retried(tmp_path):
    target = tmp_path / "bad.parquet"
    session = FakeSession([_response(b"<html>not a parquet file</html>")])

    with pytest.raises(InvalidArtifactError, match="failed artifact validation"):
        download_artifact(
            url="https://example.test/yellow.parquet",
            destination=target,
            validator=validate_parquet_file,
            retry_attempts=3,
            session=session,
            sleep_fn=lambda _seconds: None,
        )

    assert len(session.calls) == 1
    assert not target.exists()
    assert not list(tmp_path.glob(".*.part"))


def test_transient_http_status_retries_but_missing_file_does_not(tmp_path):
    payload = _fixture_bytes(tmp_path)
    sleeps = []
    session = FakeSession([FakeResponse(503), _response(payload)])
    result = download_artifact(
        url="https://example.test/yellow.parquet",
        destination=tmp_path / "retry.parquet",
        validator=validate_parquet_file,
        retry_attempts=3,
        retry_backoff_seconds=0.25,
        session=session,
        sleep_fn=sleeps.append,
    )
    assert result.outcome == "DOWNLOADED"
    assert len(session.calls) == 2
    assert sleeps == [0.25]

    not_found = FakeSession([FakeResponse(404)])
    with pytest.raises(HttpStatusError, match="HTTP 404.*not retrying"):
        download_artifact(
            url="https://example.test/missing.parquet",
            destination=tmp_path / "missing.parquet",
            validator=validate_parquet_file,
            retry_attempts=3,
            session=not_found,
            sleep_fn=lambda _seconds: None,
        )
    assert len(not_found.calls) == 1


def test_verified_unchanged_artifact_uses_conditional_request(tmp_path):
    payload = _fixture_bytes(tmp_path)
    target = tmp_path / "active.parquet"
    target.write_bytes(payload)
    original_mtime = target.stat().st_mtime_ns
    artifact_id = hashlib.sha256(payload).hexdigest()
    session = FakeSession([FakeResponse(304)])

    result = download_artifact(
        url="https://example.test/yellow.parquet",
        destination=target,
        validator=validate_parquet_file,
        prior_receipt={"source_artifact_id": artifact_id, "etag": '"artifact-v1"'},
        session=session,
    )

    assert result.outcome == "REUSED_VERIFIED_UNCHANGED"
    assert result.source_artifact_id == artifact_id
    assert session.calls[0][1]["headers"]["If-None-Match"] == '"artifact-v1"'
    assert target.stat().st_mtime_ns == original_mtime


def test_changed_valid_month_is_blocked_and_prior_artifact_preserved(tmp_path):
    contract = _temporary_contract(tmp_path)
    prior_payload = _fixture_bytes(tmp_path)
    prior_path = tmp_path / "active.parquet"
    prior_path.write_bytes(prior_payload)
    prior_mtime = prior_path.stat().st_mtime_ns

    changed_table = pq.read_table(prior_path).append_column(
        "source_revision_marker", pa.array(range(12))
    )
    output = io.BytesIO()
    pq.write_table(changed_table, output)
    with pytest.raises(SourceRevisionRequiresReview, match="SOURCE_REVISION_REQUIRES_REVIEW"):
        download_artifact(
            url="https://example.test/yellow.parquet",
            destination=prior_path,
            validator=lambda path: validate_parquet_file(path, contract),
            immutable_existing=True,
            session=FakeSession([_response(output.getvalue())]),
        )

    assert prior_path.read_bytes() == prior_payload
    assert prior_path.stat().st_mtime_ns == prior_mtime


def test_manifest_checksum_mismatch_blocks_monthly_source_reuse(tmp_path):
    payload = _fixture_bytes(tmp_path)
    target = tmp_path / "active.parquet"
    target.write_bytes(payload)
    session = FakeSession([])

    with pytest.raises(SourceRevisionRequiresReview, match="differs from its last successful"):
        download_artifact(
            url="https://example.test/yellow.parquet",
            destination=target,
            validator=validate_parquet_file,
            immutable_existing=True,
            prior_receipt={"source_artifact_id": "0" * 64, "etag": '"old"'},
            session=session,
        )

    assert not session.calls
    assert target.read_bytes() == payload


def test_required_column_failure_blocks_download_publication(tmp_path):
    contract = _temporary_contract(tmp_path)
    fixture = tmp_path / "full.parquet"
    generate_fixture(fixture, tmp_path / "zones.csv")
    table = pq.read_table(fixture).drop(["total_amount"])
    output = io.BytesIO()
    pq.write_table(table, output)
    target = tmp_path / "missing-required.parquet"

    with pytest.raises(InvalidArtifactError, match="missing required columns: total_amount"):
        download_artifact(
            url="https://example.test/yellow.parquet",
            destination=target,
            validator=lambda path: validate_parquet_file(path, contract),
            session=FakeSession([_response(output.getvalue())]),
        )

    assert not target.exists()


def test_optional_and_new_fields_are_preserved_by_contract_check_and_profile(tmp_path):
    contract = _temporary_contract(tmp_path)
    fixture = tmp_path / "full.parquet"
    generate_fixture(fixture, tmp_path / "zones.csv")
    table = pq.read_table(fixture).append_column("future_tlc_field", pa.array(range(12)))
    path = tmp_path / "with-extra.parquet"
    pq.write_table(table, path)

    validate_parquet_file(path, contract)
    check = validate_required_columns(table.schema.names, contract)
    profile = profile_parquet(path, batch_rows=3)

    assert "passenger_count" in check.present_optional_columns
    assert check.additional_columns == ("future_tlc_field",)
    assert profile.fields[-1].name == "future_tlc_field"
    assert profile.row_count == 12
    assert profile.null_counts["future_tlc_field"] == 0


def test_contract_error_names_all_missing_required_fields():
    class Contract:
        required_columns = ["pickup", "dropoff"]
        optional_columns = []

    with pytest.raises(FileContractError, match="pickup, dropoff"):
        validate_required_columns(["fare"], Contract())
