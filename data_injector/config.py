"""
Pydantic Configuration Models for Data Injector.

================================================================================
DESIGN OVERVIEW
================================================================================

This module uses Pydantic v2 to define strongly-typed configuration models.
The key design principles are:

1. FAIL-FAST VALIDATION
   - Invalid configurations crash immediately with clear error messages
   - No silent failures or unexpected behavior at runtime
   - Better to fail at startup than fail in production mid-execution

2. TYPE SAFETY WITH BaseModel
   - Each class extends pydantic.BaseModel
   - Fields are defined with Python type hints (str, int, bool, etc.)
   - Pydantic automatically validates types and converts when possible
   - Example: age: int will accept "25" and convert it to 25

3. FIELD CONSTRAINTS WITH Field()
   - Field() adds extra validation rules beyond just type checking
   - Common constraints:
     * default=X     : Default value if not provided
     * gt=0          : Must be greater than 0
     * ge=0          : Must be greater than or equal to 0
     * le=1.0        : Must be less than or equal to 1.0
     * description=  : Self-documenting config fields

4. CROSS-FIELD VALIDATION WITH @model_validator
   - Used when one field's validity depends on another field's value
   - Example: If sink_type is "sqs", then sqs_config must be provided
   - The decorator hooks into Pydantic's validation pipeline
   - mode="after" means: run this check after all individual fields pass

================================================================================
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


# ==============================================================================
# DRIFT CONFIGURATION
# ==============================================================================

class DriftConfig(BaseModel):
    """
    Configuration for schema drift simulation.
    
    Schema drift tests how your data pipeline handles unexpected changes:
    - New columns appearing in data
    - Columns being removed
    - Column order changing
    - Data types changing (int -> string, etc.)
    
    This is critical for testing Iceberg/Delta Lake schema evolution.
    """

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

    # -------------------------------------------------------------------------
    # HARDENING: Cross-field validation
    # -------------------------------------------------------------------------
    @model_validator(mode="after")
    def validate_drift_settings(self) -> "DriftConfig":
        """
        Ensure percentage is meaningful when drift is enabled.
        
        Prevents misconfiguration where user enables drift but forgets
        to set a percentage, resulting in no drift actually occurring.
        """
        if self.enabled and self.percentage == 0.0:
            raise ValueError(
                "drift.percentage must be > 0 when drift.enabled is True. "
                "Otherwise, no drift will occur despite being enabled."
            )
        return self


# ==============================================================================
# DUPLICATE CONFIGURATION
# ==============================================================================

class DuplicateConfig(BaseModel):
    """
    Configuration for duplicate record simulation.
    
    Supports TWO types of duplicates for testing deduplication:
    
    1. NETWORK DUPLICATES (network_duplicate_*)
       - Same messageId, same content
       - Simulates PubSub → SQS network retries
       - Tests Stage 1 dedup (FIFO by message_id)
    
    2. APP CORRECTION DUPLICATES (app_correction_*)
       - Same idempotency_key, DIFFERENT content
       - Simulates app resending corrected data
       - Tests Stage 2 dedup (LIFO by idempotency_key)
    """

    enabled: bool = False
    
    # Network duplicates (same messageId, same content)
    network_duplicate_percentage: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of network duplicate per record (0.0 to 1.0)",
    )
    network_duplicate_max: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Maximum number of network duplicates per record",
    )
    
    # App correction duplicates (same idempotency_key, different content)
    app_correction_percentage: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of app correction duplicate (0.0 to 1.0)",
    )
    
    # Legacy field for backward compatibility
    percentage: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="(DEPRECATED) Use network_duplicate_percentage instead",
    )
    max_duplicates: int = Field(
        default=2,
        ge=1,
        le=10,
        description="(DEPRECATED) Use network_duplicate_max instead",
    )

    # -------------------------------------------------------------------------
    # HARDENING: Cross-field validation
    # -------------------------------------------------------------------------
    @model_validator(mode="after")
    def validate_duplicate_settings(self) -> "DuplicateConfig":
        """
        Ensure at least one duplicate type has percentage > 0 when enabled.
        Also handle legacy percentage field migration.
        """
        # Migrate legacy percentage to network_duplicate_percentage
        if self.percentage > 0 and self.network_duplicate_percentage == 0:
            object.__setattr__(self, 'network_duplicate_percentage', self.percentage)
            object.__setattr__(self, 'network_duplicate_max', self.max_duplicates)
        
        if self.enabled:
            if self.network_duplicate_percentage == 0 and self.app_correction_percentage == 0:
                raise ValueError(
                    "When duplicates.enabled is True, at least one of "
                    "network_duplicate_percentage or app_correction_percentage must be > 0."
                )
        return self


# ==============================================================================
# SCHEMA CONFIGURATION
# ==============================================================================

class SchemaConfig(BaseModel):
    """
    Configuration for schema loading and simulation settings.
    
    Groups together:
    - Path to the base schema file
    - Drift simulation settings
    - Duplicate simulation settings
    """

    base_schema: str = Field(description="Path to the base schema JSON file")
    drift: DriftConfig = Field(default_factory=DriftConfig)
    duplicates: DuplicateConfig = Field(default_factory=DuplicateConfig)


# ==============================================================================
# SINK CONFIGURATIONS
# ==============================================================================

class SQSConfig(BaseModel):
    """
    Configuration for AWS SQS sink.
    
    Used when sink_type is "sqs" to send generated data to an SQS queue.
    Supports both standard and FIFO queues.
    """

    target_sqs_url: str = Field(description="The SQS queue URL to send messages to")
    region: str = Field(default="us-east-1", description="AWS region")
    message_group_id: Optional[str] = Field(
        default=None,
        description="Message group ID for FIFO queues (required for .fifo queues)",
    )

    # -------------------------------------------------------------------------
    # HARDENING: Validate FIFO queue requirements
    # -------------------------------------------------------------------------
    @model_validator(mode="after")
    def validate_fifo_settings(self) -> "SQSConfig":
        """
        Ensure FIFO queues have required message_group_id.
        
        FIFO queues (.fifo suffix) require a message group ID for ordering.
        """
        if self.target_sqs_url.endswith(".fifo") and not self.message_group_id:
            raise ValueError(
                "message_group_id is required for FIFO queues. "
                f"Queue URL '{self.target_sqs_url}' appears to be a FIFO queue."
            )
        return self


class FileConfig(BaseModel):
    """
    Configuration for local file sink.
    
    Used when sink_type is "file" to write generated data to local JSON files.
    """

    output_dir: str = Field(default="./output", description="Output directory for files")


class FirehoseConfig(BaseModel):
    """
    Configuration for AWS Firehose sink.
    
    Used when sink_type is "firehose" to send generated data directly to 
    a Kinesis Data Firehose delivery stream.
    """

    delivery_stream_name: str = Field(
        description="Name of the Firehose delivery stream (not ARN)"
    )
    region: str = Field(default="us-east-1", description="AWS region")


# ==============================================================================
# MAIN JOB CONFIGURATION
# ==============================================================================


class JobConfig(BaseModel):
    """
    Main configuration model for the data injector job.
    
    This is the top-level configuration that combines all settings:
    - Execution mode (local CLI vs cloud Lambda)
    - Logging preferences
    - Output destination (sink)
    - Performance tuning (concurrency, batch size)
    - Schema and simulation settings
    """

    run_mode: Literal["local", "cloud"] = Field(
        description="Execution environment: 'local' or 'cloud'"
    )
    log_format: Literal["user", "json"] = Field(
        description="Log output format: 'user' (human-readable) or 'json' (structured)"
    )
    log_level: str = Field(default="INFO", description="Logging level")
    sink_type: Literal["console", "file", "sqs", "firehose"] = Field(
        description="Output destination: 'console', 'file', 'sqs', or 'firehose'"
    )
    concurrency: int = Field(
        default=10,
        gt=0,
        le=100,  # HARDENING: Upper limit to prevent resource exhaustion
        description="Number of concurrent batch operations (max 100)",
    )
    batch_size: int = Field(
        default=100,
        gt=0,
        le=10000,  # HARDENING: Upper limit for memory safety
        description="Number of records per batch (max 10000)",
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
    firehose_config: Optional[FirehoseConfig] = Field(
        default=None,
        description="Firehose configuration (required when sink_type is 'firehose')",
    )

    # -------------------------------------------------------------------------
    # HARDENING: Multiple cross-field validations
    # -------------------------------------------------------------------------
    
    @model_validator(mode="after")
    def validate_sqs_config(self) -> "JobConfig":
        """
        Ensure SQS config is provided when sink_type is 'sqs'.
        
        This is a critical dependency - you can't send to SQS without
        knowing which queue to target.
        """
        if self.sink_type == "sqs" and self.sqs_config is None:
            raise ValueError(
                "sqs_config is required when sink_type is 'sqs'. "
                "Please provide target_sqs_url."
            )
        if self.sink_type == "firehose" and self.firehose_config is None:
            raise ValueError(
                "firehose_config is required when sink_type is 'firehose'. "
                "Please provide delivery_stream_name."
            )
        return self

    @model_validator(mode="after")
    def validate_batch_size_vs_total(self) -> "JobConfig":
        """
        Warn if batch_size exceeds total_records.
        
        While technically valid, this is usually a configuration mistake.
        """
        if self.batch_size > self.total_records:
            raise ValueError(
                f"batch_size ({self.batch_size}) should not exceed "
                f"total_records ({self.total_records}). "
                "This would result in a single undersized batch."
            )
        return self

    @model_validator(mode="after")
    def validate_cloud_settings(self) -> "JobConfig":
        """
        Validate settings appropriate for cloud (Lambda) execution.
        
        High concurrency in Lambda can cause throttling issues.
        JSON logging is recommended for CloudWatch integration.
        """
        if self.run_mode == "cloud":
            if self.concurrency > 50:
                raise ValueError(
                    f"concurrency ({self.concurrency}) is too high for cloud mode. "
                    "Lambda environments should use concurrency <= 50 to avoid "
                    "throttling and resource contention."
                )
            if self.log_format != "json":
                # This is a warning, not an error - we allow it but log a note
                import logging
                logging.getLogger(__name__).warning(
                    "log_format='user' in cloud mode. Consider using 'json' "
                    "for better CloudWatch integration."
                )
        return self


# ==============================================================================
# CONFIG LOADER
# ==============================================================================

def load_config(config_path: str) -> JobConfig:
    """
    Load and validate configuration from a JSON file.

    This function implements the fail-fast pattern:
    - If the file doesn't exist -> FileNotFoundError immediately
    - If JSON is malformed -> JSONDecodeError immediately  
    - If config is invalid -> ValidationError immediately with details

    Args:
        config_path: Path to the JSON configuration file

    Returns:
        Validated JobConfig instance ready for use

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
        pydantic.ValidationError: If config validation fails (fail-fast)
    """
    import json
    from pathlib import Path

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, "r") as f:
        config_data = json.load(f)

    # model_validate triggers all validators including @model_validator
    return JobConfig.model_validate(config_data)
