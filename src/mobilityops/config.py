"""Validated, local-only project and contract configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator


class ProjectPaths(BaseModel):
    """Paths are stored relative to the project root."""

    model_config = ConfigDict(extra="forbid")

    contract_yaml: str
    fixture_parquet: str
    zone_lookup_csv: str
    expected_results_json: str
    manifest_json: str
    attempt_history_jsonl: str
    real_parquet: str
    real_zone_lookup_csv: str
    real_manifest_json: str
    real_attempt_history_jsonl: str
    real_profile_json: str
    source_schema_json: str
    sample_parquet: str
    sample_manifest_json: str
    sample_attempt_history_jsonl: str
    sample_profile_json: str


class RealSourceConfig(BaseModel):
    """Official TLC source addresses and the separate sample dataset identity."""

    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(pattern=r"^real-yellow-taxi-\d{4}-\d{2}$")
    sample_dataset_id: str = Field(pattern=r"^sample-yellow-taxi-\d{4}-\d{2}$")
    parquet_url: str = Field(pattern=r"^https://")
    zone_lookup_url: str = Field(pattern=r"^https://")
    sample_limit_rows: int = Field(default=10_000, ge=1, le=10_000)
    retry_attempts: int = Field(default=3, ge=1, le=8)
    retry_backoff_seconds: float = Field(default=0.5, ge=0, le=30)
    connect_timeout_seconds: float = Field(default=10, gt=0, le=120)
    read_timeout_seconds: float = Field(default=60, gt=0, le=600)


class ProjectConfig(BaseModel):
    """Settings supported by local fixture and real-source preparation."""

    model_config = ConfigDict(extra="forbid")

    project_name: str
    execution_mode: Literal["local"]
    demo_mode: Literal["snapshot"]
    dataset_id: Literal["fixture"]
    source_month: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    contract_version: Literal["v1"]
    chunk_rows: int = Field(ge=1, le=1_000_000)
    paths: ProjectPaths
    real_source: RealSourceConfig

    _project_root: Path = PrivateAttr()

    @model_validator(mode="after")
    def source_ids_match_partition(self) -> ProjectConfig:
        """Prevent a source identity from drifting away from its configured month."""

        if not self.real_source.dataset_id.endswith(self.source_month):
            raise ValueError("real_source.dataset_id must end with source_month")
        if not self.real_source.sample_dataset_id.endswith(self.source_month):
            raise ValueError("real_source.sample_dataset_id must end with source_month")
        return self

    def resolve_path(self, path_name: str) -> Path:
        """Resolve one configured, project-relative path."""

        if path_name not in ProjectPaths.model_fields:
            raise KeyError(f"Unknown configured path: {path_name}")
        relative_path = getattr(self.paths, path_name)
        return (self._project_root / relative_path).resolve()

    @property
    def project_root(self) -> Path:
        return self._project_root


class YellowTaxiContract(BaseModel):
    """The file-level column contract; row classification is a later milestone."""

    model_config = ConfigDict(extra="allow")

    contract_version: Literal["v1"]
    required_columns: list[str]
    optional_columns: list[str]


def load_project_config(config_path: str | Path) -> ProjectConfig:
    """Load YAML and anchor all configured paths at the repository root."""

    resolved_config_path = Path(config_path).resolve()
    with resolved_config_path.open("r", encoding="utf-8") as stream:
        raw_config = yaml.safe_load(stream)
    if not isinstance(raw_config, dict):
        raise ValueError("Project configuration must be a YAML mapping.")

    config = ProjectConfig.model_validate(raw_config)
    config._project_root = resolved_config_path.parent.parent
    return config


def load_yellow_taxi_contract(config: ProjectConfig) -> YellowTaxiContract:
    """Load the v1 contract referenced by project configuration."""

    contract_path = config.resolve_path("contract_yaml")
    with contract_path.open("r", encoding="utf-8") as stream:
        raw_contract = yaml.safe_load(stream)
    if not isinstance(raw_contract, dict):
        raise ValueError("Yellow Taxi contract must be a YAML mapping.")
    return YellowTaxiContract.model_validate(raw_contract)
