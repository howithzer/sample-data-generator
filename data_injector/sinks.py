"""
Data Sink implementations using the Adapter Pattern.

Provides a unified interface for different output destinations:
- ConsoleSink: Dry-run logging
- LocalFileSink: Write to local JSON files
- SQSSink: Send to AWS SQS using boto3
- FirehoseSink: Send directly to AWS Kinesis Data Firehose
"""

import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import JobConfig

logger = logging.getLogger("data_injector.sinks")


class DataSink(ABC):
    """Abstract base class for all data sinks."""

    @abstractmethod
    async def send_batch(self, records: list[dict], batch_id: str) -> int:
        """
        Send a batch of records to the sink.

        Args:
            records: List of record dictionaries to send
            batch_id: Unique identifier for this batch

        Returns:
            Number of records successfully sent
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up any resources held by the sink."""
        pass


class ConsoleSink(DataSink):
    """Dry-run sink that prints sample records to console."""

    def __init__(self, samples_per_batch: int = 1):
        """
        Initialize console sink.

        Args:
            samples_per_batch: Number of sample records to show per batch (default: 1)
        """
        self.samples_per_batch = samples_per_batch
        self.total_sent = 0
        self.first_batch = True

    async def send_batch(self, records: list[dict], batch_id: str) -> int:
        """Print batch summary and sample record to console."""
        logger.info(
            f"[CONSOLE] Batch {batch_id}: {len(records)} records",
            extra={"batch_id": batch_id},
        )

        # Only show sample record for first batch to avoid flooding console
        if self.first_batch and records:
            self.first_batch = False
            print("\n--- SAMPLE RECORD ---")
            print(json.dumps(records[0], indent=2, default=str))
            print("--- END SAMPLE ---\n")

        self.total_sent += len(records)
        return len(records)

    async def close(self) -> None:
        """Log summary."""
        logger.info(f"[CONSOLE] Total records processed: {self.total_sent}")


class LocalFileSink(DataSink):
    """Sink that writes batches to local JSON files."""

    def __init__(self, output_dir: str = "./output"):
        """
        Initialize file sink.

        Args:
            output_dir: Directory to write output files to
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.total_sent = 0
        self.files_written = 0

    async def send_batch(self, records: list[dict], batch_id: str) -> int:
        """Write batch to a timestamped JSON file."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{timestamp}_{batch_id}.json"
        filepath = self.output_dir / filename

        # Run file I/O in thread pool to not block event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_file, filepath, records)

        self.total_sent += len(records)
        self.files_written += 1

        logger.info(
            f"Wrote {len(records)} records to {filename}",
            extra={"batch_id": batch_id},
        )

        return len(records)

    def _write_file(self, filepath: Path, records: list[dict]) -> None:
        """Synchronous file write operation."""
        with open(filepath, "w") as f:
            json.dump(
                {
                    "batch_id": filepath.stem.split("_")[-1],
                    "record_count": len(records),
                    "records": records,
                },
                f,
                indent=2,
                default=str,
            )

    async def close(self) -> None:
        """Log summary of files written."""
        logger.info(
            f"File sink closed: {self.files_written} files, "
            f"{self.total_sent} total records written to {self.output_dir}"
        )


class SQSSink(DataSink):
    """
    Sink that sends records to AWS SQS.

    Uses boto3 with thread pool executor for async operation.
    Assumes IAM role-based authentication (Lambda execution role).
    """

    def __init__(
        self,
        queue_url: str,
        region: str = "us-east-1",
        message_group_id: str | None = None,
    ):
        """
        Initialize SQS sink.

        Args:
            queue_url: SQS queue URL
            region: AWS region
            message_group_id: Message group ID for FIFO queues
        """
        import boto3

        self.queue_url = queue_url
        self.message_group_id = message_group_id
        self.is_fifo = queue_url.endswith(".fifo")

        self.sqs_client = boto3.client("sqs", region_name=region)
        self.total_sent = 0
        self.total_failed = 0

    async def send_batch(self, records: list[dict], batch_id: str) -> int:
        """
        Send batch to SQS using batch send.

        SQS batch supports up to 10 messages per request,
        so we chunk accordingly.
        """
        loop = asyncio.get_event_loop()

        # SQS batch limit is 10 messages
        chunks = [records[i : i + 10] for i in range(0, len(records), 10)]
        sent_count = 0

        for chunk in chunks:
            try:
                result = await loop.run_in_executor(
                    None, self._send_batch_sync, chunk, batch_id
                )
                sent_count += result
            except Exception as e:
                logger.error(
                    f"Failed to send batch chunk: {e}",
                    extra={"batch_id": batch_id},
                )
                self.total_failed += len(chunk)

        self.total_sent += sent_count
        return sent_count

    def _send_batch_sync(self, records: list[dict], batch_id: str) -> int:
        """Synchronous SQS batch send operation."""
        import uuid

        entries = []
        for i, record in enumerate(records):
            entry = {
                "Id": str(i),
                "MessageBody": json.dumps(record, default=str),
                "MessageAttributes": {
                    "batch_id": {"DataType": "String", "StringValue": batch_id}
                },
            }

            # Add FIFO-specific attributes
            if self.is_fifo:
                entry["MessageGroupId"] = self.message_group_id or "default"
                entry["MessageDeduplicationId"] = str(uuid.uuid4())

            entries.append(entry)

        response = self.sqs_client.send_message_batch(
            QueueUrl=self.queue_url, Entries=entries
        )

        successful = len(response.get("Successful", []))
        failed = response.get("Failed", [])

        if failed:
            for failure in failed:
                logger.warning(
                    f"Message send failed: {failure.get('Message')}",
                    extra={"batch_id": batch_id},
                )
            self.total_failed += len(failed)

        return successful

    async def close(self) -> None:
        """Log summary of SQS operations."""
        logger.info(
            f"SQS sink closed: {self.total_sent} sent, {self.total_failed} failed"
        )


class FirehoseSink(DataSink):
    """
    Sink that sends records directly to AWS Kinesis Data Firehose.

    Uses boto3 with thread pool executor for async operation.
    Supports batching up to 500 records per request (Firehose limit).
    Each record is JSON-encoded and newline-delimited for Iceberg compatibility.
    """

    def __init__(
        self,
        delivery_stream_name: str,
        region: str = "us-east-1",
    ):
        """
        Initialize Firehose sink.

        Args:
            delivery_stream_name: Name of the Firehose delivery stream
            region: AWS region
        """
        import boto3

        self.delivery_stream_name = delivery_stream_name
        self.firehose_client = boto3.client("firehose", region_name=region)
        self.total_sent = 0
        self.total_failed = 0

    async def send_batch(self, records: list[dict], batch_id: str) -> int:
        """
        Send batch to Firehose using put_record_batch.

        Firehose supports up to 500 records per batch request,
        so we chunk accordingly.
        """
        loop = asyncio.get_event_loop()

        # Firehose batch limit is 500 records or 4MB
        chunks = [records[i : i + 500] for i in range(0, len(records), 500)]
        sent_count = 0

        for chunk in chunks:
            try:
                result = await loop.run_in_executor(
                    None, self._send_batch_sync, chunk, batch_id
                )
                sent_count += result
            except Exception as e:
                logger.error(
                    f"Failed to send Firehose batch chunk: {e}",
                    extra={"batch_id": batch_id},
                )
                self.total_failed += len(chunk)

        self.total_sent += sent_count
        return sent_count

    def _send_batch_sync(self, records: list[dict], batch_id: str) -> int:
        """Synchronous Firehose batch send operation."""
        import json

        # Firehose expects records as list of {"Data": bytes}
        # For Iceberg/Glue, each record should be newline-delimited JSON
        firehose_records = []
        for record in records:
            # Add newline for JSON Lines format (required for Iceberg)
            json_data = json.dumps(record, default=str) + "\n"
            firehose_records.append({"Data": json_data.encode("utf-8")})

        response = self.firehose_client.put_record_batch(
            DeliveryStreamName=self.delivery_stream_name,
            Records=firehose_records,
        )

        # Check for failures
        failed_count = response.get("FailedPutCount", 0)
        successful = len(records) - failed_count

        if failed_count > 0:
            logger.warning(
                f"Firehose batch partially failed: {failed_count} records failed",
                extra={"batch_id": batch_id},
            )
            self.total_failed += failed_count

        return successful

    async def close(self) -> None:
        """Log summary of Firehose operations."""
        logger.info(
            f"Firehose sink closed: {self.total_sent} sent, {self.total_failed} failed"
        )


def create_sink(config: "JobConfig") -> DataSink:
    """
    Factory function to create the appropriate sink based on configuration.

    Args:
        config: Job configuration

    Returns:
        Configured DataSink instance

    Raises:
        ValueError: If sink_type is not recognized
    """
    if config.sink_type == "console":
        return ConsoleSink()
    elif config.sink_type == "file":
        return LocalFileSink(output_dir=config.file_config.output_dir)
    elif config.sink_type == "sqs":
        if config.sqs_config is None:
            raise ValueError("SQS configuration is required for SQS sink")
        return SQSSink(
            queue_url=config.sqs_config.target_sqs_url,
            region=config.sqs_config.region,
            message_group_id=config.sqs_config.message_group_id,
        )
    elif config.sink_type == "firehose":
        if config.firehose_config is None:
            raise ValueError("Firehose configuration is required for Firehose sink")
        return FirehoseSink(
            delivery_stream_name=config.firehose_config.delivery_stream_name,
            region=config.firehose_config.region,
        )
    else:
        raise ValueError(f"Unknown sink type: {config.sink_type}")
