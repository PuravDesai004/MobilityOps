"""File-level schema contract checks; value classification belongs to a later milestone."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from mobilityops.config import YellowTaxiContract


class FileContractError(ValueError):
    """Raised when a source artifact is missing required payload columns."""


@dataclass(frozen=True)
class ContractCheck:
    present_columns: tuple[str, ...]
    present_optional_columns: tuple[str, ...]
    additional_columns: tuple[str, ...]


def validate_required_columns(
    observed_columns: Iterable[str], contract: YellowTaxiContract
) -> ContractCheck:
    """Require contract columns while leaving every observed source field intact."""

    columns = tuple(observed_columns)
    observed = set(columns)
    missing = [name for name in contract.required_columns if name not in observed]
    if missing:
        raise FileContractError(
            "Yellow Taxi file contract failed; missing required columns: " + ", ".join(missing)
        )

    required = set(contract.required_columns)
    optional = set(contract.optional_columns)
    return ContractCheck(
        present_columns=columns,
        present_optional_columns=tuple(name for name in columns if name in optional),
        additional_columns=tuple(name for name in columns if name not in required | optional),
    )
