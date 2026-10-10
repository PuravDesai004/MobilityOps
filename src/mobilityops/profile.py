"""Parquet metadata profiling without loading trip data into pandas."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow.parquet as pq

from mobilityops.identity import sha256_file


@dataclass(frozen=True)
class FieldProfile:
    name: str
    arrow_type: str
    nullable: bool
    parquet_physical_type: str
    parquet_logical_type: str


@dataclass(frozen=True)
class ParquetProfile:
    file_name: str
    file_size_bytes: int
    row_count: int
    row_group_count: int
    fields: tuple[FieldProfile, ...]
    schema_sha256: str
    source_artifact_id: str
    null_counts: dict[str, int]

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-ready profile."""

        result = asdict(self)
        result["fields"] = [asdict(field) for field in self.fields]
        return result


def profile_parquet(path: str | Path, *, batch_rows: int = 10_000) -> ParquetProfile:
    """Profile Parquet metadata and count nulls in bounded Arrow record batches."""

    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")

    parquet_path = Path(path)
    if not parquet_path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    metadata = pq.read_metadata(parquet_path)
    arrow_schema = metadata.schema.to_arrow_schema()
    parquet_fields = [metadata.schema.column(index) for index in range(len(arrow_schema))]
    fields = tuple(
        FieldProfile(
            name=field.name,
            arrow_type=str(field.type),
            nullable=field.nullable,
            parquet_physical_type=str(parquet_fields[index].physical_type),
            parquet_logical_type=str(parquet_fields[index].logical_type),
        )
        for index, field in enumerate(arrow_schema)
    )
    schema_payload = [
        {"name": field.name, "type": field.arrow_type, "nullable": field.nullable}
        for field in fields
    ]
    schema_bytes = json.dumps(
        schema_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    null_counts = {field.name: 0 for field in arrow_schema}
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches(batch_size=batch_rows):
        for index, field in enumerate(arrow_schema):
            null_counts[field.name] += batch.column(index).null_count

    return ParquetProfile(
        file_name=parquet_path.name,
        file_size_bytes=parquet_path.stat().st_size,
        row_count=metadata.num_rows,
        row_group_count=metadata.num_row_groups,
        fields=fields,
        schema_sha256=hashlib.sha256(schema_bytes).hexdigest(),
        source_artifact_id=sha256_file(parquet_path),
        null_counts=null_counts,
    )
