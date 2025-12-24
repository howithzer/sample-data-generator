"""
Pydantic configuration models for Data Injector.

Provides strict validation with fail-fast behavior for missing required fields.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


class DriftConfig(BaseModel):
    """Configuration for schema drift simulation."""

    enabled: bool = False
    percentage: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of drift per record (0.0 to 1.0)",
    )
    add_columns: bool = Field(default=True, description="Randomly add new columns")
    delete_columns: bool = Field(default=False, description="Randomly remove columns")
    reorder_columns: bool = Field(default=False, description="Randomly reorder columns")
    type_changes: bool = Field(default=True, description="Randomly change field types")


class DuplicateConfig(BaseModel):
    """Configuration for duplicate record simulation (network latency)."""

    enabled: bool = False
    percentage: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of duplicating a record (0.0 to 1.0)",
    )
    max_duplicates: int = Field(
        default=2,
        ge=1,
        le=10,
        description="Maximum number of duplicates per record",
    )


class SchemaConfig(BaseModel):
    """Configuration for schema loading and drift."""

    base_schema: str = Field(description="Path to the base schema JSON file")
    drift: DriftConfig = Field(default_factory=DriftConfig)
    duplicates: DuplicateConfig = Field(default_factory=DuplicateConfig)


class SQSConfig(BaseModel):
    """Configuration for SQS sink."""

    target_sqs_url: str = Field(description="The SQS queue URL to send messages to")
    region: str = Field(default="us-east-1", description="AWS region")
    message_group_id: Optional[str] = Field(
        default=None,
        description="Message group ID for FIFO queues (required for .fifo queues)",
    )


class FileConfig(BaseModel):
    """Configuration for local file sink."""

    output_dir: str = Field(default="./output", description="Output directory for files")


class JobConfig(BaseModel):
    """Main configuration model for the data injector job."""

    run_mode: Literal["local", "cloud"] = Field(
        description="Execution environment: 'local' or 'cloud'"
    )
    log_format: Literal["user", "json"] = Field(
        description="Log output format: 'user' (human-readable) or 'json' (structured)"
    )
    log_level: str = Field(default="INFO", description="Logging level")
    sink_type: Literal["console", "file", "sqs"] = Field(
        description="Output destination: 'console', 'file', or 'sqs'"
    )
    concurrency: int = Field(
        default=10,
        gt=0,
        description="Number of concurrent batch operations",
    )
    batch_size: int = Field(
        default=100,
        gt=0,
        description="Number of records per batch",
    )
    total_records: int = Field(
        default=1000,
        gt=0,
        description="Total number of records to generate",
    )
    metrics_interval_seconds: int = Field(
        default=5,
        gt=0,
        description="Interval in seconds for logging metrics summary",
    )
    schema_config: SchemaConfig = Field(description="Schema configuration")
    sqs_config: Optional[SQSConfig] = Field(
        default=None,
        description="SQS configuration (required when sink_type is 'sqs')",
    )
    file_config: FileConfig = Field(
        default_factory=FileConfig,
        description="File sink configuration",
    )

    @model_validator(mode="after")
    def validate_sqs_config(self) -> "JobConfig":
        """Ensure SQS config is provided when sink_type is 'sqs'."""
        if self.sink_type == "sqs" and self.sqs_config is None:
            raise ValueError(
                "sqs_config is required when sink_type is 'sqs'. "
                "Please provide target_sqs_url."
            )
        return self


def load_config(config_path: str) -> JobConfig:
    """
    Load and validate configuration from a JSON file.

    Args:
        config_path: Path to the JSON configuration file

    Returns:
        Validated JobConfig instance

    Raises:
        FileNotFoundError: If config file doesn't exist
        pydantic.ValidationError: If config validation fails (fail-fast)
    """
    import json
    from pathlib import Path

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, "r") as f:
        config_data = json.load(f)

    return JobConfig.model_validate(config_data)
