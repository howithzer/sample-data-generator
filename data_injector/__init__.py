"""Data Injector - Production-Grade Data Injection Framework for Pipeline Stress Testing."""

from .config import JobConfig, load_config
from .generator import DataGenerator
from .logger import setup_logging
from .sinks import ConsoleSink, DataSink, LocalFileSink, SQSSink, create_sink

__version__ = "1.0.0"
__all__ = [
    "JobConfig",
    "load_config",
    "DataGenerator",
    "setup_logging",
    "DataSink",
    "ConsoleSink",
    "LocalFileSink",
    "SQSSink",
    "create_sink",
]
