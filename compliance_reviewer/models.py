"""Data models for the compliance reviewer."""

from dataclasses import dataclass
from typing import Any


@dataclass
class ReviewConfig:
    """Runtime configuration from CLI arguments."""

    record_id: str
    prompt_file: str
    output_dir: str
    prior_review_path: str | None
    verbose: bool
    llm_timeout: int = 300
    checklist_only: bool = False


@dataclass
class AttestationData:
    """Fetched attestation for a compliance record."""

    record_id: str
    raw: dict[str, Any]
    data_store_count: int
    json_str: str


@dataclass
class AssembledPrompt:
    """The final prompt ready for the LLM."""

    content: str
    size_bytes: int
    has_prior_review: bool


@dataclass
class LLMResponse:
    """Response from a single LLM invocation."""

    content: str
    duration_seconds: float
    success: bool
    error_message: str | None = None


@dataclass
class ReportResult:
    """Result of writing the findings report."""

    file_path: str
    success: bool
    error_message: str | None = None
