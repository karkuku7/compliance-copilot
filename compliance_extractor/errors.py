"""Custom exception hierarchy for the compliance data extractor.

Every exception carries `recoverable` and `suggested_action` attributes
so callers can make informed decisions about retry vs. abort.

Hierarchy:
    ExtractorError (base)
    ├── ConnectionError
    │   ├── AuthenticationError
    │   ├── ConnectionTimeoutError
    │   └── SessionExpiredError
    ├── DatasetAccessError
    │   ├── DatasetNotFoundError
    │   ├── PermissionDeniedError
    │   └── SchemaChangedError
    ├── QueryError
    │   ├── QueryTimeoutError
    │   ├── ResourceExhaustedError
    │   └── JoinFailureError
    └── DataQualityError
        ├── NullKeyError
        ├── DuplicateKeyError
        └── TypeMismatchError
"""

from __future__ import annotations


class ExtractorError(Exception):
    """Base exception for all extractor errors."""

    def __init__(
        self,
        message: str,
        recoverable: bool = False,
        suggested_action: str = "",
    ):
        super().__init__(message)
        self.recoverable = recoverable
        self.suggested_action = suggested_action


# --- Connection Errors ---


class ConnectionError(ExtractorError):
    """Base for connection-related errors."""

    pass


class AuthenticationError(ConnectionError):
    """Credentials are invalid or expired."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Check credentials and re-authenticate",
        )


class ConnectionTimeoutError(ConnectionError):
    """Connection to the data warehouse timed out."""

    def __init__(self, message: str = "Connection timed out"):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Retry with increased timeout or check network",
        )


class SessionExpiredError(ConnectionError):
    """The warehouse session has expired."""

    def __init__(self, message: str = "Session expired"):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Reconnect to establish a new session",
        )


# --- Dataset Access Errors ---


class DatasetAccessError(ExtractorError):
    """Base for dataset access errors."""

    pass


class DatasetNotFoundError(DatasetAccessError):
    """A required dataset/table does not exist."""

    def __init__(self, message: str = "Dataset not found"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Verify dataset name and catalog configuration",
        )


class PermissionDeniedError(DatasetAccessError):
    """Insufficient permissions to access the dataset."""

    def __init__(self, message: str = "Permission denied"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Request access grants for the dataset",
        )


class SchemaChangedError(DatasetAccessError):
    """The dataset schema has changed unexpectedly."""

    def __init__(self, message: str = "Schema changed"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Update join keys and column mappings",
        )


# --- Query Errors ---


class QueryError(ExtractorError):
    """Base for query execution errors."""

    pass


class QueryTimeoutError(QueryError):
    """A query exceeded its timeout limit."""

    def __init__(self, message: str = "Query timed out", query_id: str | None = None):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Retry with per-table strategy or reduce dataset scope",
        )
        self.query_id = query_id


class ResourceExhaustedError(QueryError):
    """The warehouse ran out of resources (memory, compute)."""

    def __init__(self, message: str = "Resource exhausted"):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Reduce query scope or increase warehouse capacity",
        )


class JoinFailureError(QueryError):
    """The multi-table join failed."""

    def __init__(self, message: str = "Join failed"):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Try per-table query strategy instead",
        )


# --- Data Quality Errors ---


class DataQualityError(ExtractorError):
    """Base for data quality issues."""

    pass


class NullKeyError(DataQualityError):
    """A required join key is NULL."""

    def __init__(self, message: str = "Null key encountered"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Investigate source data for missing keys",
        )


class DuplicateKeyError(DataQualityError):
    """Unexpected duplicate keys after deduplication."""

    def __init__(self, message: str = "Duplicate key"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Check deduplication logic and snapshot ordering",
        )


class TypeMismatchError(DataQualityError):
    """A column value doesn't match the expected type."""

    def __init__(self, message: str = "Type mismatch"):
        super().__init__(
            message,
            recoverable=False,
            suggested_action="Check column type mappings and warehouse encoding",
        )
