# Data Injector

A production-grade synthetic data injection framework for stress-testing AWS data pipelines. Built with Python, AsyncIO, Pydantic, and Faker.

## Features

- High-Performance Async Generation – Concurrent batch processing with configurable parallelism
- Schema Drift Simulation – Test Iceberg schema evolution with random column additions, deletions, reordering, and type changes
- Duplicate Record Simulation – Simulate network latency scenarios with configurable duplicate rates
- Multi-Output Sinks – Console (dry-run), Local File, and AWS SQS support
- Nested JSON Schemas – Support for envelope/payload message structures
- Structured Logging – JSON or human-readable output with batch traceability
- Dual Execution Modes – CLI for long-running EC2/EKS tests, Lambda handler for burst testing

---

## Installation

### 1. Create Virtual Environment (Recommended)

```bash
cd sample-data-generator
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `pydantic>=2.0` – Configuration validation
- `faker>=20.0` – Realistic test data generation
- `boto3>=1.28.0` – AWS SQS integration (optional)

---

## Quick Start

### Run with Default Config

```bash
python -m data_injector.main --config config.json
```

### Run with Debug Logging

```bash
python -m data_injector.main --config config.json --log-level DEBUG
```

---

## Configuration

Create a JSON configuration file (see `config.json` for example):

```json
{
  "run_mode": "local",
  "log_format": "user",
  "log_level": "INFO",
  "sink_type": "console",
  "concurrency": 5,
  "batch_size": 10,
  "total_records": 50,
  "metrics_interval_seconds": 5,
  "schema_config": {
    "base_schema": "schemas/event_message_v2.json",
    "drift": {
      "enabled": true,
      "percentage": 0.5,
      "add_columns": true,
      "delete_columns": true,
      "reorder_columns": true,
      "type_changes": true
    },
    "duplicates": {
      "enabled": true,
      "percentage": 0.2,
      "max_duplicates": 3
    }
  },
  "file_config": {
    "output_dir": "./output"
  }
}
```

### Configuration Options

| Setting | Values | Description |
|---------|--------|-------------|
| `run_mode` | `local`, `cloud` | Execution environment |
| `log_format` | `user`, `json` | Log output format |
| `sink_type` | `console`, `file`, `sqs` | Output destination |
| `concurrency` | `1-N` | Number of concurrent batch operations |
| `batch_size` | `1-N` | Records per batch |
| `total_records` | `1-N` | Total records to generate |

### Schema Drift Options

| Option | Description |
|--------|-------------|
| `percentage` | Probability of drift per record (0.0-1.0) |
| `add_columns` | Randomly add new columns |
| `delete_columns` | Randomly remove columns |
| `reorder_columns` | Shuffle column order |
| `type_changes` | Change value types (int→str, etc.) |

### SQS Configuration (when `sink_type: "sqs"`)

```json
{
  "sqs_config": {
    "target_sqs_url": "https://sqs.us-east-1.amazonaws.com/123456789/sample-data-generator-queue",
    "region": "us-east-1",
    "message_group_id": "sample-data-generator-group"
  }
}
```

---

## Schema Files

Define data schemas in JSON format. Two formats are supported:

### Flat Schema (Simple)

```json
{
  "name": "transaction_v1",
  "version": "1.0.0",
  "fields": [
    { "name": "transaction_id", "type": "uuid" },
    { "name": "amount", "type": "decimal" },
    { "name": "status", "type": "string" }
  ]
}
```

### Nested Schema (Envelope + Payload)

```json
{
  "name": "event_message_v2",
  "version": "2.0.0",
  "envelope": {
    "messageId": "uuid",
    "publishTime": "timestamp"
  },
  "payload": {
    "sessionId": "uuid",
    "event": {
      "eventType": { "type": "choice", "options": ["Login", "Logout"] },
      "userId": "uuid"
    }
  }
}
```

### Supported Field Types

| Type | Description |
|------|-------------|
| `uuid` | UUID v4 string |
| `string` | Random word |
| `text` | Random text (200 chars) |
| `name` | Full name |
| `email` | Email address |
| `integer` | Random integer (1-100000) |
| `float` / `decimal` | Random decimal |
| `boolean` | True/False |
| `date` / `datetime` / `timestamp` | Date/time values |
| `address` | Street address |
| `phone` | Phone number |
| `url` | URL |
| `ipv4` | IPv4 address |
| `currency_code` | Currency code (USD, EUR, etc.) |
| `company` | Company name |

---

## Output Sinks

### Console Sink (Dry-Run)
Logs batch summaries and shows one sample record.

### File Sink
Writes JSON files to `output_dir` with format: `{timestamp}_{batch_id}.json`

### SQS Sink
Sends messages in batches of 10 (SQS limit) with support for FIFO queues.

---

## AWS Lambda Usage

Deploy as a Lambda function and invoke with configuration as the event payload:

```python
from data_injector.main import lambda_handler

# Lambda automatically uses the lambda_handler function
```

---

## Project Structure

```
sample-data-generator/
├── config.json              # Default configuration
├── config_drift_test.json   # Drift testing configuration
├── requirements.txt         # Python dependencies
├── schemas/
│   ├── event_message_v2.json    # Nested schema example
│   └── transaction_v1.json      # Flat schema example
└── data_injector/
    ├── __init__.py
    ├── main.py          # Entry point (CLI & Lambda)
    ├── config.py        # Pydantic configuration models
    ├── generator.py     # Data generation & drift simulation
    ├── sinks.py         # Output adapters (Console, File, SQS)
    └── logger.py        # Structured logging setup
```

---

## Metrics Output

The injector reports periodic metrics during execution:

```
METRICS | Generated: 500, Failed: 0, Batches: 50, Rate: 125.3/sec, Elapsed: 3.9s
FINAL   | Total Records: 500, Unique: 423, Duplicates: 77, Rate: 128.2/sec, Time: 3.9s
DRIFT   | Drifted Records: 251, Breakdown: [add_column: 126, type_change: 98, reorder: 27]
```

---

## License

MIT License - See [LICENSE](LICENSE) for details.
