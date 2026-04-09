"""Data warehouse connection manager.

Establishes and manages connections to a SQL data warehouse (e.g., Athena).
Handles session lifecycle, query execution with polling, pagination, and timeouts.
"""

import logging
import os
import time
from typing import Any

from compliance_extractor.constants import (
    CONNECTION_TIMEOUT_SECONDS,
    QUERY_TIMEOUT_SECONDS,
    WAREHOUSE_CATALOG,
    WAREHOUSE_DATABASE,
    WAREHOUSE_OUTPUT_LOCATION,
    WAREHOUSE_WORKGROUP,
    AWS_REGION,
)
from compliance_extractor.errors import (
    AuthenticationError,
    ConnectionTimeoutError,
    SessionExpiredError,
    QueryTimeoutError,
)

logger = logging.getLogger(__name__)

try:
    import boto3

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


class WarehouseSession:
    """An active session for query execution against the data warehouse."""

    def __init__(
        self,
        session_id: str,
        created_at: float,
        client: Any = None,
        database: str | None = None,
        workgroup: str | None = None,
        output_location: str | None = None,
        catalog: str | None = None,
    ):
        self.session_id = session_id
        self.created_at = created_at
        self._client = client
        self._database = database or WAREHOUSE_DATABASE
        self._workgroup = workgroup or WAREHOUSE_WORKGROUP
        self._output_location = output_location or WAREHOUSE_OUTPUT_LOCATION
        self._catalog = catalog or WAREHOUSE_CATALOG
        self._is_active = True

    @property
    def is_active(self) -> bool:
        return self._is_active

    def execute_query(
        self, query: str, timeout_seconds: int = QUERY_TIMEOUT_SECONDS
    ) -> dict[str, Any]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query string.
            timeout_seconds: Maximum wait time for query completion.

        Returns:
            Dict with keys: success, data (list of row dicts), row_count, columns.

        Raises:
            SessionExpiredError: If the session is no longer active.
            QueryTimeoutError: If the query exceeds the timeout.
        """
        if not self._is_active:
            raise SessionExpiredError("Session is no longer active")

        if not self._client:
            raise SessionExpiredError("No warehouse client available")

        logger.info("Executing query (%d chars)", len(query))
        start = time.monotonic()

        try:
            # Start query execution
            response = self._client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    "Database": self._database,
                    "Catalog": self._catalog,
                },
                WorkGroup=self._workgroup,
                ResultConfiguration={"OutputLocation": self._output_location},
            )
            query_id = response["QueryExecutionId"]
            logger.info("Query started: %s", query_id)

            # Poll for completion
            while True:
                elapsed = time.monotonic() - start
                if elapsed > timeout_seconds:
                    raise QueryTimeoutError(
                        f"Query {query_id} timed out after {timeout_seconds}s",
                        query_id=query_id,
                    )

                status = self._client.get_query_execution(
                    QueryExecutionId=query_id
                )
                state = status["QueryExecution"]["Status"]["State"]

                if state == "SUCCEEDED":
                    break
                elif state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get(
                        "StateChangeReason", "Unknown"
                    )
                    raise QueryTimeoutError(f"Query {state}: {reason}", query_id=query_id)

                time.sleep(1)

            # Fetch results with pagination
            rows: list[dict[str, Any]] = []
            columns: list[str] = []
            next_token = None
            first_page = True

            while True:
                kwargs: dict[str, Any] = {
                    "QueryExecutionId": query_id,
                    "MaxResults": 1000,
                }
                if next_token:
                    kwargs["NextToken"] = next_token

                result = self._client.get_query_results(**kwargs)

                if first_page:
                    columns = [
                        col["Name"]
                        for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                    ]
                    # Skip header row on first page
                    data_rows = result["ResultSet"]["Rows"][1:]
                    first_page = False
                else:
                    data_rows = result["ResultSet"]["Rows"]

                for row in data_rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        cell = row["Data"][i] if i < len(row["Data"]) else {}
                        row_dict[col] = cell.get("VarCharValue")
                    rows.append(row_dict)

                next_token = result.get("NextToken")
                if not next_token:
                    break

            duration = time.monotonic() - start
            logger.info(
                "Query %s completed: %d rows in %.1fs", query_id, len(rows), duration
            )

            return {
                "success": True,
                "data": rows,
                "row_count": len(rows),
                "columns": columns,
                "execution_time_seconds": duration,
            }

        except (QueryTimeoutError, SessionExpiredError):
            raise
        except Exception as exc:
            raise QueryTimeoutError(f"Query execution failed: {exc}")

    def close(self) -> None:
        """Mark the session as inactive."""
        self._is_active = False


class ConnectionManager:
    """Manages connections to the data warehouse.

    Establishes sessions via boto3 (for Athena) and verifies connectivity
    before returning a session.
    """

    def __init__(
        self,
        database: str | None = None,
        region: str | None = None,
        workgroup: str | None = None,
    ):
        self._database = database or WAREHOUSE_DATABASE
        self._region = region or AWS_REGION
        self._workgroup = workgroup or WAREHOUSE_WORKGROUP

    def connect(
        self, timeout_seconds: int = CONNECTION_TIMEOUT_SECONDS
    ) -> WarehouseSession:
        """Establish a connection and return a session.

        Args:
            timeout_seconds: Maximum time to wait for connection.

        Returns:
            An active WarehouseSession.

        Raises:
            AuthenticationError: If credentials are invalid.
            ConnectionTimeoutError: If connection times out.
        """
        if not HAS_BOTO3:
            raise AuthenticationError(
                "boto3 is not installed. Install it with: pip install boto3"
            )

        logger.info(
            "Connecting to warehouse (database=%s, region=%s, workgroup=%s)",
            self._database,
            self._region,
            self._workgroup,
        )

        start = time.monotonic()
        try:
            client = boto3.client("athena", region_name=self._region)

            # Verify connectivity
            client.get_work_group(WorkGroup=self._workgroup)

            elapsed = time.monotonic() - start
            if elapsed > timeout_seconds:
                raise ConnectionTimeoutError(
                    f"Connection took {elapsed:.1f}s (limit: {timeout_seconds}s)"
                )

            session_id = f"session-{int(time.time())}"
            logger.info("Connected in %.1fs (session: %s)", elapsed, session_id)

            return WarehouseSession(
                session_id=session_id,
                created_at=time.time(),
                client=client,
                database=self._database,
                workgroup=self._workgroup,
            )

        except ConnectionTimeoutError:
            raise
        except Exception as exc:
            error_msg = str(exc).lower()
            if "credential" in error_msg or "auth" in error_msg or "token" in error_msg:
                raise AuthenticationError(f"Authentication failed: {exc}")
            if "timeout" in error_msg:
                raise ConnectionTimeoutError(f"Connection timed out: {exc}")
            raise ConnectionTimeoutError(f"Failed to connect: {exc}")
