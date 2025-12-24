"""
Data Injector - Main Entry Point

Supports two execution modes:
- CLI: Long-running process for EC2/EKS stress testing
- Lambda: Short-lived burst testing handler
"""

import argparse
import asyncio
import logging
import sys
import time
import uuid
from typing import Any

from .config import JobConfig, load_config
from .generator import DataGenerator, generate_batch_async
from .logger import setup_logging
from .sinks import DataSink, create_sink

logger = logging.getLogger("data_injector")


class MetricsCollector:
    """Collects and reports generation metrics including drift and duplicates."""

    def __init__(self, interval_seconds: int = 5):
        self.interval_seconds = interval_seconds
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.generated_count = 0
        self.failed_count = 0
        self.batch_count = 0
        # Drift and duplicate tracking
        self.duplicate_count = 0
        self.drifted_count = 0
        self.drift_types: dict[str, int] = {
            "add_column": 0,
            "delete_column": 0,
            "reorder": 0,
            "type_change": 0,
        }

    def record_batch(self, records: list[dict], failed_count: int = 0):
        """Record a batch result and analyze records for drift/duplicates."""
        self.generated_count += len(records)
        self.failed_count += failed_count
        self.batch_count += 1

        # Analyze records
        for record in records:
            if record.get("_is_duplicate"):
                self.duplicate_count += 1
            drift_applied = record.get("_drift_applied", [])
            if drift_applied:
                self.drifted_count += 1
                for drift_type in drift_applied:
                    if drift_type in self.drift_types:
                        self.drift_types[drift_type] += 1

    def should_report(self) -> bool:
        """Check if it's time to log a metrics report."""
        return (time.time() - self.last_report_time) >= self.interval_seconds

    def report(self):
        """Log metrics summary."""
        elapsed = time.time() - self.start_time
        rate = self.generated_count / elapsed if elapsed > 0 else 0

        logger.info(
            f"METRICS | Generated: {self.generated_count}, "
            f"Failed: {self.failed_count}, "
            f"Batches: {self.batch_count}, "
            f"Rate: {rate:.1f}/sec, "
            f"Elapsed: {elapsed:.1f}s"
        )
        self.last_report_time = time.time()

    def final_report(self):
        """Log final summary with drift and duplicate breakdown."""
        elapsed = time.time() - self.start_time
        rate = self.generated_count / elapsed if elapsed > 0 else 0
        unique_count = self.generated_count - self.duplicate_count

        logger.info(
            f"FINAL | Total Records: {self.generated_count}, "
            f"Unique: {unique_count}, "
            f"Duplicates: {self.duplicate_count}, "
            f"Rate: {rate:.1f}/sec, "
            f"Time: {elapsed:.2f}s"
        )

        # Log drift breakdown if any drift occurred
        if self.drifted_count > 0:
            drift_breakdown = ", ".join(
                f"{k}: {v}" for k, v in self.drift_types.items() if v > 0
            )
            logger.info(
                f"DRIFT  | Drifted Records: {self.drifted_count}, "
                f"Breakdown: [{drift_breakdown}]"
            )


async def process_batch(
    generator: DataGenerator,
    sink: DataSink,
    batch_size: int,
    batch_id: str,
    metrics: MetricsCollector,
) -> int:
    """
    Generate and send a single batch.

    Args:
        generator: Data generator instance
        sink: Output sink instance
        batch_size: Records per batch
        batch_id: Unique batch identifier
        metrics: Metrics collector

    Returns:
        Number of records successfully processed
    """
    try:
        # Generate batch
        records = await generate_batch_async(generator, batch_size, batch_id)

        # Send to sink
        sent_count = await sink.send_batch(records, batch_id)

        # Record metrics with full record analysis
        metrics.record_batch(records)
        return sent_count

    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}", extra={"batch_id": batch_id})
        metrics.record_batch([], batch_size)
        return 0


async def run_generator(config: JobConfig) -> dict[str, Any]:
    """
    Main generator loop.

    Args:
        config: Validated job configuration

    Returns:
        Summary dict with metrics
    """
    generator = DataGenerator(config.schema_config)
    sink = create_sink(config)
    metrics = MetricsCollector(interval_seconds=config.metrics_interval_seconds)

    # Calculate batches needed
    total_batches = (config.total_records + config.batch_size - 1) // config.batch_size
    records_remaining = config.total_records

    logger.info(
        f"Starting generation: {config.total_records} records in "
        f"{total_batches} batches (batch_size={config.batch_size}, "
        f"concurrency={config.concurrency})"
    )

    try:
        # Process in concurrent chunks
        batch_num = 0
        while records_remaining > 0:
            # Determine batch of batches to run concurrently
            concurrent_batches = min(config.concurrency, total_batches - batch_num)
            tasks = []

            for i in range(concurrent_batches):
                batch_id = str(uuid.uuid4())[:8]
                batch_size = min(config.batch_size, records_remaining)
                records_remaining -= batch_size

                task = process_batch(
                    generator=generator,
                    sink=sink,
                    batch_size=batch_size,
                    batch_id=batch_id,
                    metrics=metrics,
                )
                tasks.append(task)

            # Wait for concurrent batch to complete
            await asyncio.gather(*tasks)
            batch_num += concurrent_batches

            # Report metrics periodically
            if metrics.should_report():
                metrics.report()

    except KeyboardInterrupt:
        logger.warning("Generation interrupted by user")
    except Exception as e:
        logger.error(f"Generation failed: {e}")
        raise
    finally:
        await sink.close()
        metrics.final_report()

    return {
        "total_generated": metrics.generated_count,
        "total_failed": metrics.failed_count,
        "batches": metrics.batch_count,
        "duration_seconds": time.time() - metrics.start_time,
    }


async def run_cli(config_path: str) -> None:
    """
    CLI entry point.

    Args:
        config_path: Path to configuration JSON file
    """
    try:
        # Load and validate config
        config = load_config(config_path)
        logger.info(f"Configuration loaded from {config_path}")

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)

    # Setup logging with config
    setup_logging(
        log_format=config.log_format,
        level=config.log_level,
    )

    # Run generator
    result = await run_generator(config)
    logger.info(f"Generation complete: {result}")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    AWS Lambda handler for burst testing.

    Args:
        event: Lambda event containing configuration
        context: Lambda context

    Returns:
        Result summary
    """
    try:
        # Parse config from event
        config = JobConfig.model_validate(event)

        # Setup logging for Lambda (always JSON)
        setup_logging(log_format="json", level=config.log_level)
        logger.info("Lambda handler started", extra={"request_id": context.aws_request_id})

        # Run generator with timeout awareness
        result = asyncio.run(run_generator(config))

        return {
            "statusCode": 200,
            "body": result,
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            "statusCode": 500,
            "body": {"error": str(e)},
        }


def main():
    """CLI main function."""
    parser = argparse.ArgumentParser(
        description="Data Injector - Pipeline Stress Testing Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m data_injector.main --config config.json
  python -m data_injector.main --config config.json --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to configuration JSON file",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )

    args = parser.parse_args()

    # Initial logging setup (may be reconfigured after config load)
    setup_logging(log_format="user", level=args.log_level)

    # Run async main
    asyncio.run(run_cli(args.config))


if __name__ == "__main__":
    main()
