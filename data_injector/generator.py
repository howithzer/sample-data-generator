"""
Data generator with Faker, schema drift, and duplicate simulation.

Generates realistic test data based on a JSON schema with optional
drift injection and duplicate records for testing Iceberg schema evolution
and deduplication handling.
"""

import copy
import json
import logging
import random
import string
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from .config import DriftConfig, DuplicateConfig, SchemaConfig

logger = logging.getLogger("data_injector.generator")
fake = Faker()

# Type mapping from schema types to Faker generators
TYPE_GENERATORS = {
    "string": lambda: fake.word(),
    "text": lambda: fake.text(max_nb_chars=200),
    "name": lambda: fake.name(),
    "email": lambda: fake.email(),
    "uuid": lambda: str(uuid.uuid4()),
    "integer": lambda: fake.random_int(min=1, max=100000),
    "float": lambda: round(random.uniform(0.01, 10000.0), 2),
    "decimal": lambda: round(random.uniform(0.01, 10000.0), 4),
    "boolean": lambda: fake.boolean(),
    "date": lambda: fake.date(),
    "datetime": lambda: fake.date_time().isoformat(),
    "timestamp": lambda: datetime.now(timezone.utc).isoformat(),
    "address": lambda: fake.address().replace("\n", ", "),
    "phone": lambda: fake.phone_number(),
    "url": lambda: fake.url(),
    "ipv4": lambda: fake.ipv4(),
    "currency_code": lambda: fake.currency_code(),
    "company": lambda: fake.company(),
}


def generate_value(field_spec: Any) -> Any:
    """
    Generate a value based on field specification.

    Handles both simple types (string) and complex specs (dict with type/options).
    """
    if isinstance(field_spec, str):
        # Simple type like "uuid", "timestamp"
        generator = TYPE_GENERATORS.get(field_spec, TYPE_GENERATORS["string"])
        return generator()
    elif isinstance(field_spec, dict):
        field_type = field_spec.get("type", "string")
        if field_type == "choice":
            # Random choice from options
            options = field_spec.get("options", ["default"])
            return random.choice(options)
        else:
            generator = TYPE_GENERATORS.get(field_type, TYPE_GENERATORS["string"])
            return generator()
    else:
        return fake.word()


def generate_nested_object(spec: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively generate a nested object from a schema spec.

    Args:
        spec: Dictionary specifying nested fields and their types

    Returns:
        Generated nested object
    """
    result = {}
    for key, value in spec.items():
        if isinstance(value, dict) and "type" not in value and "options" not in value:
            # Nested object
            result[key] = generate_nested_object(value)
        else:
            result[key] = generate_value(value)
    return result


class SchemaLoader:
    """Loads and parses JSON schema files with support for nested structures."""

    def __init__(self, schema_path: str):
        """
        Load schema from JSON file.

        Args:
            schema_path: Path to the schema JSON file

        Raises:
            FileNotFoundError: If schema file doesn't exist
            json.JSONDecodeError: If schema is not valid JSON
        """
        path = Path(schema_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(path, "r") as f:
            self.schema = json.load(f)

        # Support both flat (fields array) and nested (envelope/payload) schemas
        self.fields = self.schema.get("fields", [])
        self.envelope = self.schema.get("envelope", {})
        self.payload = self.schema.get("payload", {})
        self.is_nested = bool(self.envelope or self.payload)

        if self.is_nested:
            logger.info(
                f"Loaded nested schema from {schema_path} "
                f"(envelope: {len(self.envelope)} fields, payload structure)"
            )
        else:
            logger.info(f"Loaded flat schema from {schema_path} with {len(self.fields)} fields")

    def get_field_names(self) -> list[str]:
        """Get list of field names from schema."""
        if self.is_nested:
            return list(self.envelope.keys()) + ["payload"]
        return [f["name"] for f in self.fields]


class DriftSimulator:
    """Applies schema drift to records for testing schema evolution."""

    def __init__(self, config: DriftConfig):
        """
        Initialize drift simulator.

        Args:
            config: Drift configuration with enabled drift types
        """
        self.config = config
        self.drift_column_counter = 0

    def should_apply_drift(self) -> bool:
        """Determine if drift should be applied based on percentage."""
        if not self.config.enabled:
            return False
        return random.random() < self.config.percentage

    def apply_drift(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Apply random schema drift to a record.

        Drift types:
        - add_columns: Add a new column with random name and value
        - delete_columns: Remove a random existing column
        - reorder_columns: Shuffle column order
        - type_changes: Change value type (int->str, float->int, etc.)

        Args:
            record: Original record dictionary

        Returns:
            Modified record with drift applied
        """
        if not self.should_apply_drift():
            return record

        # Create a deep copy to avoid modifying original
        drifted = copy.deepcopy(record)
        drift_applied = []

        # Randomly select which drift types to apply
        if self.config.add_columns and random.random() < 0.5:
            drifted = self._add_column(drifted)
            drift_applied.append("add_column")

        if self.config.delete_columns and random.random() < 0.3:
            drifted = self._delete_column(drifted)
            drift_applied.append("delete_column")

        if self.config.reorder_columns and random.random() < 0.3:
            drifted = self._reorder_columns(drifted)
            drift_applied.append("reorder")

        if self.config.type_changes and random.random() < 0.4:
            drifted = self._change_type(drifted)
            drift_applied.append("type_change")

        if drift_applied:
            drifted["_drift_applied"] = drift_applied

        return drifted

    def _add_column(self, record: dict[str, Any]) -> dict[str, Any]:
        """Add a new column with random name and value."""
        self.drift_column_counter += 1
        random_suffix = "".join(random.choices(string.ascii_lowercase, k=4))
        col_name = f"drift_test_col_{self.drift_column_counter}_{random_suffix}"

        # Random value type
        value_type = random.choice(["string", "integer", "float", "boolean"])
        record[col_name] = TYPE_GENERATORS.get(value_type, lambda: "unknown")()

        return record

    def _delete_column(self, record: dict[str, Any]) -> dict[str, Any]:
        """Remove a random non-essential column."""
        # Don't delete essential fields
        protected_keys = {"messageId", "publishTime", "payload", "batch_id", "generated_at", "_drift_applied"}
        deletable_keys = [k for k in record.keys() if k not in protected_keys]

        if deletable_keys:
            key_to_delete = random.choice(deletable_keys)
            del record[key_to_delete]

        return record

    def _reorder_columns(self, record: dict[str, Any]) -> dict[str, Any]:
        """Shuffle column order."""
        items = list(record.items())
        random.shuffle(items)
        return dict(items)

    def _change_type(self, record: dict[str, Any]) -> dict[str, Any]:
        """Change the type of a random field."""
        protected_keys = {"messageId", "publishTime", "payload", "batch_id", "generated_at", "_drift_applied"}
        changeable_keys = [k for k in record.keys() if k not in protected_keys]

        if not changeable_keys:
            return record

        key = random.choice(changeable_keys)
        value = record[key]

        # Apply type conversion
        if isinstance(value, (int, float)):
            record[key] = str(value)  # Number to string
        elif isinstance(value, str):
            try:
                record[key] = float(value)  # Try string to float
            except ValueError:
                record[key] = len(value)  # Fallback: string length as int
        elif isinstance(value, bool):
            record[key] = 1 if value else 0  # Bool to int

        return record


class DuplicateSimulator:
    """
    Simulates two types of duplicate records for testing deduplication:
    
    1. NETWORK DUPLICATES: Same messageId, same content
       - Simulates PubSub → SQS network retries
       - Curation Stage 1 dedup should keep FIRST (FIFO by message_id)
    
    2. APP CORRECTION DUPLICATES: Same idempotency_key, DIFFERENT content
       - Simulates app resending corrected data
       - Curation Stage 2 dedup should keep LAST (LIFO by idempotency_key)
    """

    def __init__(self, config: DuplicateConfig):
        self.config = config
        self.network_duplicates_created = 0
        self.app_corrections_created = 0
        # Store idempotency keys for creating corrections later
        self._idempotency_pool: list[tuple[str, dict]] = []

    def should_create_network_duplicate(self) -> bool:
        """Determine if network duplicate should be created."""
        if not self.config.enabled:
            return False
        return random.random() < self.config.network_duplicate_percentage

    def should_create_app_correction(self) -> bool:
        """Determine if app correction duplicate should be created."""
        if not self.config.enabled:
            return False
        return random.random() < self.config.app_correction_percentage

    def create_duplicates(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Create duplicate copies of a record (network duplicates).

        Network duplicates have:
        - Same messageId
        - Same content
        - Marked with _is_network_duplicate

        Args:
            record: Original record to duplicate

        Returns:
            List containing original + duplicates
        """
        result = [record]
        
        # Store for potential app correction later
        idempotency_key = self._extract_idempotency_key(record)
        if idempotency_key and self.should_create_app_correction():
            self._idempotency_pool.append((idempotency_key, copy.deepcopy(record)))

        # Create network duplicates (same messageId, same content)
        if self.should_create_network_duplicate():
            num_duplicates = random.randint(1, self.config.network_duplicate_max)
            
            for i in range(num_duplicates):
                dup = copy.deepcopy(record)
                dup["_is_network_duplicate"] = True
                dup["_duplicate_index"] = i + 1
                # Keep same messageId - that's the point of network duplicate
                result.append(dup)
                self.network_duplicates_created += 1

        return result

    def create_app_corrections(self, batch_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Create app correction records for previously seen idempotency_keys.
        
        App corrections have:
        - Same idempotency_key as an earlier record
        - DIFFERENT content (simulating a correction)
        - NEW messageId (new message from app)
        - Marked with _is_app_correction
        
        Returns:
            List of correction records to append to batch
        """
        corrections = []
        
        for idempotency_key, original_record in self._idempotency_pool:
            correction = copy.deepcopy(original_record)
            
            # Generate NEW messageId (this is a new message from the app)
            correction["messageId"] = str(uuid.uuid4())
            
            # Keep same idempotency_key (that's the point of app correction)
            # The payload should have the same idempotency_key in _metadata
            
            # Modify payload content to simulate correction
            if "payload" in correction and isinstance(correction["payload"], dict):
                # Add correction marker and modify some values
                correction["payload"]["_corrected"] = True
                correction["payload"]["_correction_ts"] = datetime.now(timezone.utc).isoformat()
                
                # Modify a numeric value if present (simulate data correction)
                for key, value in correction["payload"].items():
                    if isinstance(value, (int, float)) and not key.startswith("_"):
                        correction["payload"][key] = value * 1.1  # 10% adjustment
                        break
            
            correction["_is_app_correction"] = True
            correction["_original_message_id"] = original_record.get("messageId")
            corrections.append(correction)
            self.app_corrections_created += 1
        
        # Clear pool after processing
        self._idempotency_pool.clear()
        
        return corrections

    def _extract_idempotency_key(self, record: dict[str, Any]) -> str | None:
        """Extract idempotency_key from record payload."""
        # Try nested payload._metadata.idempotencyKeyResource
        if "payload" in record and isinstance(record["payload"], dict):
            payload = record["payload"]
            if "_metadata" in payload and isinstance(payload["_metadata"], dict):
                return payload["_metadata"].get("idempotencyKeyResource")
            return payload.get("idempotencyKeyResource")
        return None


class DataGenerator:
    """Generates synthetic data based on schema with drift and duplicates."""

    def __init__(self, schema_config: SchemaConfig):
        """
        Initialize data generator.

        Args:
            schema_config: Schema configuration with path, drift, and duplicate settings
        """
        self.schema_loader = SchemaLoader(schema_config.base_schema)
        self.drift_simulator = DriftSimulator(schema_config.drift)
        self.duplicate_simulator = DuplicateSimulator(schema_config.duplicates)
        self.records_generated = 0

    def generate_record(self, batch_id: str) -> dict[str, Any]:
        """
        Generate a single record based on schema.

        Args:
            batch_id: ID of the batch this record belongs to

        Returns:
            Generated record dictionary with batch metadata
        """
        if self.schema_loader.is_nested:
            return self._generate_nested_record(batch_id)
        else:
            return self._generate_flat_record(batch_id)

    def _generate_nested_record(self, batch_id: str) -> dict[str, Any]:
        """Generate a nested record with envelope structure."""
        record = {}

        # Generate envelope fields (messageId, publishTime)
        for key, value_type in self.schema_loader.envelope.items():
            record[key] = generate_value(value_type)

        # Generate payload (nested structure)
        if self.schema_loader.payload:
            record["payload"] = generate_nested_object(self.schema_loader.payload)

        # Add batch metadata
        record["_batch_id"] = batch_id
        record["_generated_at"] = datetime.now(timezone.utc).isoformat()

        # Apply drift if configured
        record = self.drift_simulator.apply_drift(record)

        self.records_generated += 1
        return record

    def _generate_flat_record(self, batch_id: str) -> dict[str, Any]:
        """Generate a flat record (original behavior)."""
        record = {
            "batch_id": batch_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Generate values for each field in schema
        for field in self.schema_loader.fields:
            field_name = field["name"]
            field_type = field.get("type", "string")
            generator = TYPE_GENERATORS.get(field_type, TYPE_GENERATORS["string"])
            record[field_name] = generator()

        # Apply drift if configured
        record = self.drift_simulator.apply_drift(record)

        self.records_generated += 1
        return record

    def generate_batch(self, batch_size: int, batch_id: str) -> list[dict[str, Any]]:
        """
        Generate a batch of records with potential duplicates.

        Args:
            batch_size: Number of unique records to generate
            batch_id: Unique identifier for this batch

        Returns:
            List of generated records (may exceed batch_size if duplicates created)
        """
        records = []
        for _ in range(batch_size):
            record = self.generate_record(batch_id)
            # Create network duplicates if configured (same messageId)
            records.extend(self.duplicate_simulator.create_duplicates(record))

        # Create app correction duplicates (same idempotency_key, different content)
        app_corrections = self.duplicate_simulator.create_app_corrections(records)
        records.extend(app_corrections)

        # Count duplicates for logging
        network_dup_count = sum(1 for r in records if r.get("_is_network_duplicate"))
        app_corr_count = sum(1 for r in records if r.get("_is_app_correction"))
        
        if network_dup_count > 0 or app_corr_count > 0:
            logger.debug(
                f"Generated batch: {batch_size} unique + {network_dup_count} network dups + {app_corr_count} app corrections",
                extra={"batch_id": batch_id},
            )
        else:
            logger.debug(
                f"Generated batch of {len(records)} records",
                extra={"batch_id": batch_id},
            )

        return records


async def generate_batch_async(
    generator: DataGenerator,
    batch_size: int,
    batch_id: str,
) -> list[dict[str, Any]]:
    """
    Async wrapper for batch generation.

    This allows concurrent batch generation in the event loop.

    Args:
        generator: DataGenerator instance
        batch_size: Number of records per batch
        batch_id: Unique batch identifier

    Returns:
        List of generated records
    """
    import asyncio

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, generator.generate_batch, batch_size, batch_id
    )
