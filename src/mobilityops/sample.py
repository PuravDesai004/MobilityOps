"""Deterministic, bounded Arrow sampling that keeps parent-row lineage."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mobilityops.config import YellowTaxiContract
from mobilityops.contract import validate_required_columns
from mobilityops.identity import sha256_file
from mobilityops.profile import ParquetProfile, profile_parquet

SOURCE_ROW_NUMBER_COLUMN = "_mobilityops_source_row_number"


@dataclass(frozen=True)
class SampleResult:
    path: str
    outcome: str
    sample_artifact_id: str
    parent_artifact_id: str
    row_count: int
    parent_row_count: int
    selected_source_row_numbers: tuple[int, ...]
    source_row_number_column: str
    sampling_method: str
    profile: ParquetProfile

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["profile"] = self.profile.as_dict()
        result.pop("selected_source_row_numbers")
        result["sampled_rows"] = self.row_count
        result["first_source_row_number"] = (
            self.selected_source_row_numbers[0] if self.row_count else None
        )
        result["last_source_row_number"] = (
            self.selected_source_row_numbers[-1] if self.row_count else None
        )
        selection_bytes = json.dumps(
            self.selected_source_row_numbers, separators=(",", ":")
        ).encode("ascii")
        result["selection_sha256"] = hashlib.sha256(selection_bytes).hexdigest()
        return result


def evenly_spaced_source_rows(parent_row_count: int, limit_rows: int) -> tuple[int, ...]:
    """Choose deterministic, one-based positions spanning the ordered source file."""

    if parent_row_count < 0:
        raise ValueError("parent_row_count cannot be negative")
    if limit_rows < 1:
        raise ValueError("limit_rows must be positive")
    sample_count = min(parent_row_count, limit_rows)
    if sample_count == 0:
        return ()
    if sample_count == 1:
        return (1,)
    return tuple(
        1 + (sample_index * (parent_row_count - 1)) // (sample_count - 1)
        for sample_index in range(sample_count)
    )


def create_deterministic_sample(
    *,
    source_path: str | Path,
    sample_path: str | Path,
    contract: YellowTaxiContract,
    parent_artifact_id: str,
    limit_rows: int = 10_000,
    batch_rows: int = 10_000,
) -> SampleResult:
    """Write at most ``limit_rows`` evenly spaced source rows without pandas.

    The only added field stores each selected row's one-based physical position in
    the unchanged parent Parquet artifact. The sample checksum and identities are
    computed from the separate sample file after it is written.
    """

    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    source = Path(source_path)
    target = Path(sample_path)
    if not source.is_file():
        raise FileNotFoundError(f"Parent Parquet file not found: {source}")
    actual_parent_id = sha256_file(source)
    if actual_parent_id != parent_artifact_id:
        raise ValueError("parent artifact checksum does not match the supplied identity")

    parquet_file = pq.ParquetFile(source)
    source_schema = parquet_file.schema_arrow
    validate_required_columns(source_schema.names, contract)
    if SOURCE_ROW_NUMBER_COLUMN in source_schema.names:
        raise ValueError(f"source already contains reserved field {SOURCE_ROW_NUMBER_COLUMN}")

    total_rows = parquet_file.metadata.num_rows
    selected_rows = evenly_spaced_source_rows(total_rows, limit_rows)
    output_schema = source_schema.append(
        pa.field(SOURCE_ROW_NUMBER_COLUMN, pa.int64(), nullable=False)
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
    selected_batches: list[pa.RecordBatch] = []
    selected_index = 0
    seen_source_rows = 0
    try:
        for batch in parquet_file.iter_batches(batch_size=batch_rows, use_threads=False):
            batch_start = seen_source_rows
            batch_end = batch_start + batch.num_rows
            offsets: list[int] = []
            lineage: list[int] = []
            while (
                selected_index < len(selected_rows) and selected_rows[selected_index] <= batch_end
            ):
                source_row_number = selected_rows[selected_index]
                if source_row_number > batch_start:
                    offsets.append(source_row_number - batch_start - 1)
                    lineage.append(source_row_number)
                selected_index += 1
            if offsets:
                selected_batch = batch.take(pa.array(offsets, type=pa.int64()))
                selected_batch = selected_batch.append_column(
                    pa.field(SOURCE_ROW_NUMBER_COLUMN, pa.int64(), nullable=False),
                    pa.array(lineage, type=pa.int64()),
                )
                selected_batches.append(selected_batch)
            seen_source_rows = batch_end

        if seen_source_rows != total_rows or selected_index != len(selected_rows):
            raise ValueError(
                "Parquet scan row count did not match metadata while constructing sample"
            )

        with pq.ParquetWriter(
            temp_path,
            output_schema,
            version="2.6",
            compression="NONE",
            data_page_version="1.0",
            use_dictionary=False,
            write_statistics=True,
        ) as writer:
            if selected_batches:
                sample_table = pa.Table.from_batches(selected_batches, schema=output_schema)
                writer.write_table(sample_table, row_group_size=max(1, limit_rows))

        with temp_path.open("rb+") as stream:
            os.fsync(stream.fileno())
        sample_profile = profile_parquet(temp_path, batch_rows=batch_rows)
        expected_sample_schema = output_schema
        actual_sample_schema = pq.read_schema(temp_path)
        if not actual_sample_schema.equals(expected_sample_schema, check_metadata=False):
            raise ValueError("sample Parquet schema changed while writing")

        outcome = "CREATED"
        if target.is_file():
            try:
                prior_profile = profile_parquet(target, batch_rows=batch_rows)
            except Exception:
                prior_profile = None
            if (
                prior_profile is not None
                and prior_profile.source_artifact_id == sample_profile.source_artifact_id
            ):
                outcome = "REUSED_VERIFIED_UNCHANGED"
                temp_path.unlink(missing_ok=True)
            else:
                os.replace(temp_path, target)
        else:
            os.replace(temp_path, target)

        published_profile = profile_parquet(target, batch_rows=batch_rows)
        if published_profile.source_artifact_id != sample_profile.source_artifact_id:
            raise ValueError("published sample checksum differs from validated temporary artifact")
        return SampleResult(
            path=str(target),
            outcome=outcome,
            sample_artifact_id=published_profile.source_artifact_id,
            parent_artifact_id=parent_artifact_id,
            row_count=published_profile.row_count,
            parent_row_count=total_rows,
            selected_source_row_numbers=selected_rows,
            source_row_number_column=SOURCE_ROW_NUMBER_COLUMN,
            sampling_method="evenly_spaced_source_rows_v1",
            profile=published_profile,
        )
    finally:
        temp_path.unlink(missing_ok=True)
