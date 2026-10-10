"""Pure shared classification policy; also packaged for the future user-run Snowflake UDF."""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Any

from mobilityops.identity import trip_record_id

LONG_DURATION_SECONDS = 86400


class ProvenanceError(ValueError):
    """Artifact identity or lineage is broken; do not quarantine it as source data."""


def decimal_value(value: Any, scale: int) -> Decimal | None:
    """DECIMAL(18,scale), string-form float conversion, half away from zero ties."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number.copy_abs() >= Decimal(10) ** (18 - scale):
            return None
        with localcontext() as context:
            context.prec = 40
            rounded = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        if rounded.copy_abs() >= Decimal(10) ** (18 - scale):
            return None
        return rounded
    except (InvalidOperation, ValueError, TypeError):
        return None


def timestamp_value(value: Any) -> datetime | None:
    if value is None or isinstance(value, (bool, int, float)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        # TLC timestamps are New York wall-clock values; timezone-bearing inputs need review.
        return parsed if parsed.tzinfo is None else None
    except (ValueError, TypeError, OverflowError):
        return None


def integer_value(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value():
            return None
        if not -(2**63) <= number < 2**63:
            return None
        return int(number)
    except (ValueError, TypeError, InvalidOperation):
        return None


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return {"__type__": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, float) and not math.isfinite(value):
        return {"__type__": "nonfinite_float", "value": repr(value)}
    if isinstance(value, bytes):
        return {"__type__": "bytes_hex", "value": value.hex()}
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def classify_row(
    row: dict[str, Any],
    *,
    artifact_id: str,
    source_row_number: int,
    dataset_id: str,
    source_partition: str,
    zone_ids: set[int],
    parent_artifact_id: str | None = None,
    long_duration_seconds: int = LONG_DURATION_SECONDS,
) -> dict[str, Any]:
    if not dataset_id or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", source_partition):
        raise ProvenanceError("Missing dataset identity or invalid source partition")
    try:
        record_id = trip_record_id(artifact_id, source_row_number)
        parent_row = row.get("_mobilityops_source_row_number")
        if parent_artifact_id is not None:
            trip_record_id(parent_artifact_id, parent_row)
        elif parent_row is not None:
            raise ValueError("Parent row exists without parent artifact")
    except (ValueError, TypeError) as error:
        raise ProvenanceError(str(error)) from error
    reasons: list[str] = []
    warnings: list[str] = []
    pickup = timestamp_value(row.get("tpep_pickup_datetime"))
    dropoff = timestamp_value(row.get("tpep_dropoff_datetime"))
    for name, parsed, label in [
        ("tpep_pickup_datetime", pickup, "PICKUP"),
        ("tpep_dropoff_datetime", dropoff, "DROPOFF"),
    ]:
        if row.get(name) is None:
            reasons.append(f"MISSING_{label}")
        elif parsed is None:
            reasons.append(f"INVALID_{label}_TIMESTAMP")
    duration = (dropoff - pickup).total_seconds() if pickup and dropoff else None
    if duration is not None and duration < 0:
        reasons.append("DROPOFF_BEFORE_PICKUP")
    distance = decimal_value(row.get("trip_distance"), 3)
    if row.get("trip_distance") is None:
        reasons.append("MISSING_DISTANCE")
    elif distance is None:
        reasons.append("INVALID_DISTANCE")
    elif Decimal(str(row["trip_distance"])) < 0:
        reasons.append("NEGATIVE_DISTANCE")
    charge = decimal_value(row.get("total_amount"), 2)
    if row.get("total_amount") is None:
        reasons.append("MISSING_TOTAL_AMOUNT")
    elif charge is None:
        reasons.append("INVALID_TOTAL_AMOUNT")
    elif Decimal(str(row["total_amount"])) < 0:
        warnings.append("NEGATIVE_RECORDED_CHARGE")
    pickup_id = integer_value(row.get("PULocationID"))
    dropoff_id = integer_value(row.get("DOLocationID"))
    if pickup_id not in zone_ids:
        warnings.append("UNRESOLVED_PICKUP_ZONE")
    if dropoff_id not in zone_ids:
        warnings.append("UNRESOLVED_DROPOFF_ZONE")
    passenger = integer_value(row.get("passenger_count"))
    if passenger is None:
        warnings.append("MISSING_PASSENGER_COUNT")
    elif passenger == 0:
        warnings.append("ZERO_PASSENGER_COUNT")
    payment = integer_value(row.get("payment_type"))
    if payment not in {1, 2, 3, 4, 5, 6}:
        warnings.append("UNKNOWN_PAYMENT_CODE")
    if pickup and pickup.strftime("%Y-%m") != source_partition:
        warnings.append("OUT_OF_PARTITION_PICKUP")
    if duration is not None and duration > long_duration_seconds:
        warnings.append("LONG_DURATION")
    return {
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "source_partition": source_partition,
        "source_row_number": source_row_number,
        "trip_record_id": record_id,
        "parent_artifact_id": parent_artifact_id,
        "parent_source_row_number": parent_row,
        "pickup_at": pickup,
        "dropoff_at": dropoff,
        "pickup_zone_source_id": pickup_id,
        "dropoff_zone_source_id": dropoff_id,
        "pickup_zone_key": pickup_id if pickup_id in zone_ids else 0,
        "dropoff_zone_key": dropoff_id if dropoff_id in zone_ids else 0,
        "passenger_count": passenger,
        "payment_type": payment,
        "distance_miles": distance,
        "recorded_charge": charge,
        "fare_amount": decimal_value(row.get("fare_amount"), 2),
        "tip_amount": decimal_value(row.get("tip_amount"), 2),
        "cbd_congestion_fee": decimal_value(row.get("cbd_congestion_fee"), 2),
        "duration_seconds": duration,
        "warning_codes": sorted(set(warnings)),
        "reason_codes": sorted(set(reasons)),
        "raw_payload_json": json.dumps(_json_value(row), allow_nan=False, separators=(",", ":")),
    }
