"""Review-specific exceptions."""


class ReviewerError(Exception):
    """Base exception for all reviewer errors."""

    pass


class RecordNotFoundError(ReviewerError):
    """The requested compliance record was not found."""

    pass


class DataRetrievalError(ReviewerError):
    """Failed to retrieve attestation data from the cache API."""

    pass


class PromptFileError(ReviewerError):
    """Failed to read the review prompt file."""

    pass


class LLMInvocationError(ReviewerError):
    """The LLM invocation failed or timed out."""

    pass


class ReportWriteError(ReviewerError):
    """Failed to write the findings report."""

    pass


class PriorReviewError(ReviewerError):
    """Failed to load or parse a prior review file."""

    pass
