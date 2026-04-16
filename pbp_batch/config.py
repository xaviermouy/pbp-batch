"""
Pydantic models for pbp-batch YAML configuration validation.

Validates the YAML config file at load time so that misconfigured jobs
fail immediately with a clear error rather than crashing mid-run inside pbp.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, field_validator, model_validator


class PbpJobAgentConfig(BaseModel):
    """Validates the `pbp_job_agent` section of the YAML config."""

    model_config = {"extra": "allow"}  # tolerate unknown fields added in future pbp versions

    # Required paths
    recorder: str
    audio_base_dir: str
    json_base_dir: str
    nc_output_dir: str
    meta_output_dir: str
    log_dir: str
    global_attrs: str
    variable_attrs: str

    # Required processing parameters
    start: str
    end: str
    prefix: str
    output_prefix: str
    title: str = ""

    # Optional sensitivity / calibration (empty string or numeric value)
    sensitivity_uri: Optional[str] = None
    sensitivity_flat_value: Union[float, str, None] = None
    voltage_multiplier: Union[float, str, None] = None

    # Space-separated pair fields
    subset_to: str
    latlon: str
    cmlim: str
    ylim: str

    # Deprecated / ignored
    xml_dir: Optional[str] = None

    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("sensitivity_uri", "sensitivity_flat_value", "voltage_multiplier",
                     mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        return None if v == "" else v

    @field_validator("sensitivity_flat_value", "voltage_multiplier", mode="before")
    @classmethod
    def validate_numeric_or_none(cls, v):
        if v is None or v == "":
            return None
        try:
            float(v)
        except (ValueError, TypeError):
            raise ValueError(f"must be a number or empty, got '{v}'")
        return v

    @field_validator("start", "end")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%Y%m%d")
        except ValueError:
            raise ValueError(f"must be in YYYYMMDD format, got '{v}'")
        return v

    @field_validator("subset_to", "latlon", "cmlim", "ylim")
    @classmethod
    def validate_two_numbers(cls, v: str, info) -> str:
        parts = v.split()
        if len(parts) != 2:
            raise ValueError(
                f"must be exactly 2 space-separated numbers, got '{v}'"
            )
        try:
            [float(p) for p in parts]
        except ValueError:
            raise ValueError(
                f"must contain numeric values, got '{v}'"
            )
        return v

    @field_validator("subset_to", "cmlim", "ylim")
    @classmethod
    def validate_increasing(cls, v: str, info) -> str:
        lo, hi = (float(p) for p in v.split())
        if lo >= hi:
            raise ValueError(
                f"first value must be less than second, got '{v}'"
            )
        return v

    @field_validator("audio_base_dir", "global_attrs", "variable_attrs")
    @classmethod
    def validate_path_exists(cls, v: str, info) -> str:
        p = Path(v)
        if not p.exists():
            raise ValueError(f"path does not exist: '{v}'")
        return v

    @field_validator("nc_output_dir", "meta_output_dir", "log_dir")
    @classmethod
    def validate_output_parent_exists(cls, v: str, info) -> str:
        p = Path(v)
        if not p.exists() and not p.parent.exists():
            raise ValueError(
                f"parent directory does not exist for '{v}' — "
                f"create '{p.parent}' first"
            )
        return v

    # ------------------------------------------------------------------
    # Cross-field validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def validate_date_range(self) -> "PbpJobAgentConfig":
        try:
            start = datetime.strptime(self.start, "%Y%m%d")
            end = datetime.strptime(self.end, "%Y%m%d")
        except ValueError:
            return self  # date format errors already caught above
        if start > end:
            raise ValueError(
                f"start ({self.start}) must not be after end ({self.end})"
            )
        return self


class PbpYamlConfig(BaseModel):
    """Top-level YAML config. Validates pbp_job_agent; global attribute fields are optional."""

    model_config = {"extra": "allow"}  # global attribute fields (title, institution, etc.) are flexible

    pbp_job_agent: PbpJobAgentConfig
