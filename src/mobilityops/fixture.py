"""Deterministic, source-shaped Yellow Taxi fixture data."""

from __future__ import annotations

import csv
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mobilityops.identity import sha256_file

FIXTURE_ROW_COUNT = 12
FIXTURE_ROW_GROUP_SIZE = 4

FIXTURE_SCHEMA = pa.schema(
    [
        pa.field("VendorID", pa.int64(), nullable=True),
        pa.field("tpep_pickup_datetime", pa.timestamp("us"), nullable=True),
        pa.field("tpep_dropoff_datetime", pa.timestamp("us"), nullable=True),
        pa.field("passenger_count", pa.int64(), nullable=True),
        pa.field("trip_distance", pa.float64(), nullable=True),
        pa.field("PULocationID", pa.int64(), nullable=True),
        pa.field("DOLocationID", pa.int64(), nullable=True),
        pa.field("payment_type", pa.int64(), nullable=True),
        pa.field("fare_amount", pa.float64(), nullable=True),
        pa.field("tip_amount", pa.float64(), nullable=True),
        pa.field("total_amount", pa.float64(), nullable=True),
        pa.field("cbd_congestion_fee", pa.float64(), nullable=True),
    ]
)

ZONE_ROWS = (
    (132, "Queens", "JFK Airport", "Airports"),
    (138, "Queens", "LaGuardia Airport", "Airports"),
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (162, "Manhattan", "Midtown East", "Yellow Zone"),
)
ZONE_HEADER = ("LocationID", "Borough", "Zone", "service_zone")


@dataclass(frozen=True)
class FixtureArtifacts:
    parquet_path: Path
    parquet_sha256: str
    parquet_bytes: int
    zone_lookup_path: Path
    zone_lookup_sha256: str

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["parquet_path"] = str(self.parquet_path)
        result["zone_lookup_path"] = str(self.zone_lookup_path)
        return result


def _valid_row(
    *,
    pickup: datetime,
    dropoff: datetime,
    passenger_count: int,
    distance: float,
    pickup_zone: int,
    dropoff_zone: int,
    fare: float,
    tip: float,
    total: float | None,
    vendor: int = 1,
) -> dict[str, object]:
    return {
        "VendorID": vendor,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": passenger_count,
        "trip_distance": distance,
        "PULocationID": pickup_zone,
        "DOLocationID": dropoff_zone,
        "payment_type": 1,
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": total,
        "cbd_congestion_fee": 0.75,
    }


def fixture_rows() -> list[dict[str, object]]:
    """Return rows in stable physical order; rows 1 and 2 are identical payloads."""

    row_1 = _valid_row(
        pickup=datetime(2026, 1, 5, 8, 0),
        dropoff=datetime(2026, 1, 5, 8, 18),
        passenger_count=1,
        distance=2.40,
        pickup_zone=161,
        dropoff_zone=162,
        fare=14.00,
        tip=3.00,
        total=20.25,
    )
    rows = [
        row_1,
        row_1.copy(),
        _valid_row(
            pickup=datetime(2026, 1, 5, 9, 10),
            dropoff=datetime(2026, 1, 5, 9, 29),
            passenger_count=2,
            distance=3.15,
            pickup_zone=162,
            dropoff_zone=161,
            fare=16.50,
            tip=2.00,
            total=23.25,
            vendor=2,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 6, 17, 20),
            dropoff=datetime(2026, 1, 6, 17, 43),
            passenger_count=1,
            distance=4.10,
            pickup_zone=132,
            dropoff_zone=161,
            fare=21.00,
            tip=4.00,
            total=29.25,
            vendor=2,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 7, 7, 0),
            dropoff=datetime(2026, 1, 7, 7, 16),
            passenger_count=1,
            distance=1.80,
            pickup_zone=161,
            dropoff_zone=138,
            fare=12.00,
            tip=2.40,
            total=17.65,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 8, 8, 15),
            dropoff=datetime(2026, 1, 8, 8, 10),
            passenger_count=1,
            distance=0.80,
            pickup_zone=161,
            dropoff_zone=162,
            fare=8.00,
            tip=0.00,
            total=10.75,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 9, 10, 0),
            dropoff=datetime(2026, 1, 9, 10, 12),
            passenger_count=1,
            distance=-0.25,
            pickup_zone=162,
            dropoff_zone=161,
            fare=9.00,
            tip=1.00,
            total=13.75,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 10, 11, 0),
            dropoff=datetime(2026, 1, 10, 11, 14),
            passenger_count=1,
            distance=1.40,
            pickup_zone=161,
            dropoff_zone=162,
            fare=10.00,
            tip=2.00,
            total=None,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 11, 12, 0),
            dropoff=datetime(2026, 1, 11, 12, 19),
            passenger_count=1,
            distance=2.20,
            pickup_zone=999,
            dropoff_zone=161,
            fare=13.00,
            tip=2.00,
            total=18.75,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 12, 13, 0),
            dropoff=datetime(2026, 1, 12, 13, 13),
            passenger_count=1,
            distance=1.25,
            pickup_zone=161,
            dropoff_zone=162,
            fare=-3.00,
            tip=0.00,
            total=-2.25,
        ),
        _valid_row(
            pickup=datetime(2026, 1, 13, 14, 0),
            dropoff=datetime(2026, 1, 13, 14, 12),
            passenger_count=0,
            distance=1.30,
            pickup_zone=162,
            dropoff_zone=161,
            fare=9.50,
            tip=1.50,
            total=13.75,
        ),
        _valid_row(
            pickup=datetime(2025, 12, 31, 23, 50),
            dropoff=datetime(2025, 12, 31, 23, 59),
            passenger_count=1,
            distance=0.90,
            pickup_zone=161,
            dropoff_zone=162,
            fare=7.00,
            tip=1.00,
            total=10.75,
        ),
    ]
    rows[4]["tpep_pickup_datetime"] = None
    return rows


def _write_zone_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(ZONE_HEADER)
            writer.writerows(ZONE_ROWS)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def generate_fixture(
    parquet_path: str | Path,
    zone_lookup_path: str | Path,
) -> FixtureArtifacts:
    """Write stable fixture rows and a small zone lookup using the installed PyArrow."""

    output_path = Path(parquet_path)
    zone_path = Path(zone_lookup_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = fixture_rows()
    table = pa.Table.from_pylist(rows, schema=FIXTURE_SCHEMA)
    temp_path = output_path.with_name(f".{output_path.name}.tmp")
    try:
        pq.write_table(
            table,
            temp_path,
            compression="NONE",
            version="2.6",
            data_page_version="1.0",
            row_group_size=FIXTURE_ROW_GROUP_SIZE,
            use_dictionary=False,
            write_statistics=True,
        )
        with temp_path.open("rb+") as stream:
            os.fsync(stream.fileno())
        os.replace(temp_path, output_path)
    finally:
        temp_path.unlink(missing_ok=True)

    _write_zone_csv(zone_path)
    return FixtureArtifacts(
        parquet_path=output_path,
        parquet_sha256=sha256_file(output_path),
        parquet_bytes=output_path.stat().st_size,
        zone_lookup_path=zone_path,
        zone_lookup_sha256=sha256_file(zone_path),
    )
