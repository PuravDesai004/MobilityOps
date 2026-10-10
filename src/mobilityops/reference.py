"""Independent source eligibility/money reference; does not call the classifier."""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from pathlib import Path

import pyarrow.parquet as pq


def eligible_source_charge(path: Path, batch_rows: int = 10000) -> tuple[int, Decimal]:
    count = 0
    total = Decimal("0.00")
    names = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=batch_rows, columns=names, use_threads=False):
            for row in batch.to_pylist():
                try:
                    timestamps = []
                    for column in names[:2]:
                        value = row[column]
                        if value is None or isinstance(value, (bool, int, float)):
                            raise ValueError("timestamp")
                        parsed = (
                            value
                            if isinstance(value, datetime)
                            else datetime.fromisoformat(str(value))
                        )
                        if parsed.tzinfo is not None:
                            raise ValueError("timezone")
                        timestamps.append(parsed)
                    if timestamps[1] < timestamps[0]:
                        continue
                    if isinstance(row[names[2]], bool) or isinstance(row[names[3]], bool):
                        continue
                    distance = Decimal(str(row[names[2]]))
                    charge = Decimal(str(row[names[3]]))
                    if (
                        not distance.is_finite()
                        or not charge.is_finite()
                        or distance < 0
                        or distance.copy_abs() >= 10**15
                        or charge.copy_abs() >= 10**16
                    ):
                        continue
                    with localcontext() as context:
                        context.prec = 40
                        rounded_distance = distance.quantize(Decimal(".001"), ROUND_HALF_UP)
                        rounded_charge = charge.quantize(Decimal(".01"), ROUND_HALF_UP)
                    if rounded_distance.copy_abs() >= 10**15 or rounded_charge.copy_abs() >= 10**16:
                        continue
                    count += 1
                    total += rounded_charge
                except (ValueError, TypeError, InvalidOperation, OverflowError):
                    continue
    return count, total
