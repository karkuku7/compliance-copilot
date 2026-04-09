"""Dataset accessibility validator.

Validates that all required datasets are accessible before extraction.
Uses lightweight probe queries and validates dataset names to prevent SQL injection.
"""

import logging
import re
from typing import Any

from compliance_extractor.connection import WarehouseSession
from compliance_extractor.constants import ALL_TABLES, WAREHOUSE_DATABASE
from compliance_extractor.errors import DatasetNotFoundError, PermissionDeniedError
from compliance_extractor.models import ValidationResult

logger = logging.getLogger(__name__)

# Only allow alphanumeric, underscores, and dots in dataset names
_VALID_DATASET_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.]+$")


class DatasetValidator:
    """Validates dataset accessibility before extraction."""

    def validate(
        self,
        session: WarehouseSession,
        datasets: list[str] | None = None,
    ) -> ValidationResult:
        """Validate that all required datasets are accessible.

        Args:
            session: Active warehouse session.
            datasets: List of dataset names to validate (defaults to ALL_TABLES).

        Returns:
            ValidationResult with accessible/inaccessible lists and error details.
        """
        datasets = datasets or ALL_TABLES
        result = ValidationResult()

        for dataset in datasets:
            # Validate name format (SQL injection prevention)
            if not _VALID_DATASET_NAME.match(dataset):
                result.inaccessible.append(dataset)
                result.errors[dataset] = f"Invalid dataset name format: {dataset}"
                logger.warning("Invalid dataset name: %s", dataset)
                continue

            try:
                self._probe_dataset(session, dataset)
                result.accessible.append(dataset)
                logger.info("Dataset accessible: %s", dataset)
            except Exception as exc:
                result.inaccessible.append(dataset)
                result.errors[dataset] = str(exc)
                logger.warning("Dataset inaccessible: %s — %s", dataset, exc)

        return result

    def _probe_dataset(self, session: WarehouseSession, dataset: str) -> None:
        """Execute a lightweight probe query to verify dataset access."""
        query = f'SELECT 1 FROM "{WAREHOUSE_DATABASE}"."{dataset}" LIMIT 1'
        result = session.execute_query(query, timeout_seconds=30)
        if not result["success"]:
            raise PermissionDeniedError(
                f"Cannot access dataset {dataset}: {result.get('error_message')}"
            )
