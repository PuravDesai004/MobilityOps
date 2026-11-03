"""Parquet metadata profiling without loading trip data into pandas."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
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


def _leaf_count(data_type: pa.DataType) -> int:
    """Parquet columns describe leaves, while Arrow fields describe top-level values."""
    if isinstance(data_type, pa.BaseExtensionType):
        return _leaf_count(data_type.storage_type)
    if pa.types.is_dictionary(data_type):
        return _leaf_count(data_type.value_type)
    if pa.types.is_struct(data_type):
        return sum(_leaf_count(field.type) for field in data_type)
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        return _leaf_count(data_type.value_type)
    if pa.types.is_map(data_type):
        return _leaf_count(data_type.key_type) + _leaf_count(data_type.item_type)
    return 1


def profile_parquet(path: str | Path, *, batch_rows: int = 10_000) -> ParquetProfile:
    """Profile Parquet metadata and count nulls in bounded Arrow record batches."""

    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")

    parquet_path = Path(path)
    if not parquet_path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    with pq.ParquetFile(parquet_path) as parquet_file:
        metadata = parquet_file.metadata
        arrow_schema = parquet_file.schema_arrow
        profiles = []
        leaf_index = 0
        for field in arrow_schema:
            leaf_end = leaf_index + _leaf_count(field.type)
            leaves = [metadata.schema.column(index) for index in range(leaf_index, leaf_end)]
            profiles.append(
                FieldProfile(
                    name=field.name,
                    arrow_type=str(field.type),
                    nullable=field.nullable,
                    parquet_physical_type=", ".join(str(leaf.physical_type) for leaf in leaves),
                    parquet_logical_type=", ".join(str(leaf.logical_type) for leaf in leaves),
                )
            )
            leaf_index = leaf_end

        null_counts = {field.name: 0 for field in arrow_schema}
        for batch in parquet_file.iter_batches(batch_size=batch_rows):
            for index, field in enumerate(arrow_schema):
                null_counts[field.name] += batch.column(index).null_count

    fields = tuple(profiles)
    schema_payload = [
        {"name": field.name, "type": field.arrow_type, "nullable": field.nullable}
        for field in fields
    ]
    schema_bytes = json.dumps(
        schema_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

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
