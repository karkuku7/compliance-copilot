"""Data models for the compliance data extractor.

All models are dataclasses — no ORM, no external dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ValidationResult:
    """Result of dataset accessibility validation."""

    accessible: list[str] = field(default_factory=list)
    inaccessible: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass
class ExtractionError:
    """Error captured during extraction."""

    timestamp: datetime
    error_type: str
    affected_dataset: str | None
    message: str
    recoverable: bool
    suggested_action: str


@dataclass
class ExtractionResult:
    """Result of a data extraction operation."""

    success: bool
    records_extracted: int
    records_failed: int
    errors: list[ExtractionError] = field(default_factory=list)
    execution_duration_seconds: float = 0.0
    join_success_rate: float = 0.0


@dataclass
class QueryResult:
    """Result of a query against the data warehouse."""

    success: bool
    data: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    execution_time_seconds: float = 0.0
    error_message: str | None = None


@dataclass
class WriteResult:
    """Result of a write operation to the cache."""

    success: bool
    records_written: int = 0
    records_overflowed: int = 0
    table_name: str = ""
    error_message: str | None = None
