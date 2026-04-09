"""Hierarchical join engine for multi-table compliance data.

Supports two query strategies:
1. Single SQL JOIN — fast for small/medium datasets
2. Per-table queries joined in Python — handles large datasets that timeout

Both strategies produce the same output: flat rows with columns from all four tables.
"""

import logging
from typing import Any

from compliance_extractor.connection import WarehouseSession
from compliance_extractor.constants import (
    TABLE_APPLICATIONS,
    TABLE_DATA_STORES,
    TABLE_DATA_OBJECTS,
    TABLE_OBJECT_FIELDS,
    WAREHOUSE_DATABASE,
    quote_table,
)
from compliance_extractor.errors import JoinFailureError, QueryTimeoutError

logger = logging.getLogger(__name__)


class JoinEngine:
    """Orchestrates multi-table joins with fallback strategies."""

    def build_join_query(
        self,
        record_ids: list[str] | None = None,
        owner_logins: list[str] | None = None,
    ) -> str:
        """Build a four-table LEFT JOIN query with ROW_NUMBER deduplication.

        Args:
            record_ids: Optional filter by specific record IDs.
            owner_logins: Optional filter by owner hierarchy.

        Returns:
            SQL query string.
        """
        app_table = quote_table(TABLE_APPLICATIONS)
        store_table = quote_table(TABLE_DATA_STORES)
        object_table = quote_table(TABLE_DATA_OBJECTS)
        field_table = quote_table(TABLE_OBJECT_FIELDS)

        # Build WHERE clause
        where_clauses: list[str] = []
        if record_ids:
            escaped = [name.replace("'", "''") for name in record_ids]
            in_list = ", ".join(f"'{name}'" for name in escaped)
            where_clauses.append(f"app.record_id IN ({in_list})")

        if owner_logins:
            escaped = [login.replace("'", "''") for login in owner_logins]
            owner_conditions = []
            for login in escaped:
                owner_conditions.append(f"app.owner_login = '{login}'")
                owner_conditions.append(f"app.supervisor_login = '{login}'")
                # Check up to 6 levels of reporting hierarchy
                for level in range(1, 7):
                    owner_conditions.append(
                        f"app.reports_to_level_{level}_login = '{login}'"
                    )
            where_clauses.append(f"({' OR '.join(owner_conditions)})")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
        WITH deduped AS (
            SELECT
                app.*,
                ds.store_name AS data_store_name,
                ds.store_id AS data_store_id,
                ds.technology AS store_technology,
                ds.has_sensitive_data AS store_has_sensitive_data,
                do.object_name AS data_object_name,
                do.object_id,
                do.has_sensitive_data AS object_has_sensitive_data,
                do.retention_days,
                f.field_name,
                f.field_description,
                f.field_type,
                ROW_NUMBER() OVER (
                    PARTITION BY app.record_id, ds.store_id, do.object_id, f.field_name
                    ORDER BY app.snapshot_date DESC, app.snapshot_hour DESC
                ) AS rn
            FROM {app_table} app
            LEFT JOIN {store_table} ds
                ON app.record_id = ds.application_name
            LEFT JOIN {object_table} do
                ON ds.store_id = do.data_store_id
                AND ds.application_name = do.application_name
            LEFT JOIN {field_table} f
                ON do.object_id = f.object_id
                AND do.application_name = f.application_name
            {where_sql}
        )
        SELECT * FROM deduped WHERE rn = 1
        """
        return query.strip()

    def execute_join(
        self,
        session: WarehouseSession,
        record_ids: list[str] | None = None,
        owner_logins: list[str] | None = None,
        timeout_seconds: int = 300,
    ) -> list[dict[str, Any]]:
        """Execute the four-table JOIN query.

        Args:
            session: Active warehouse session.
            record_ids: Optional filter by record IDs.
            owner_logins: Optional filter by owner hierarchy.
            timeout_seconds: Query timeout.

        Returns:
            List of flat row dicts.

        Raises:
            QueryTimeoutError: If the query times out.
            JoinFailureError: If the join fails for other reasons.
        """
        query = self.build_join_query(record_ids, owner_logins)
        logger.info("Executing four-table JOIN (%d chars)", len(query))

        try:
            result = session.execute_query(query, timeout_seconds=timeout_seconds)
            if not result["success"]:
                raise JoinFailureError(f"JOIN query failed: {result.get('error_message')}")
            logger.info("JOIN returned %d rows", result["row_count"])
            return result["data"]
        except QueryTimeoutError:
            raise
        except Exception as exc:
            raise JoinFailureError(f"JOIN execution failed: {exc}")

    def execute_per_table(
        self,
        session: WarehouseSession,
        record_ids: list[str] | None = None,
        owner_logins: list[str] | None = None,
        timeout_seconds: int = 300,
    ) -> list[dict[str, Any]]:
        """Query each table separately, then join in Python.

        Fallback strategy for when the four-table SQL JOIN times out.
        Slower for small datasets but handles arbitrarily large ones.

        Args:
            session: Active warehouse session.
            record_ids: Optional filter by record IDs.
            owner_logins: Optional filter by owner hierarchy.
            timeout_seconds: Per-query timeout.

        Returns:
            List of flat row dicts (same format as execute_join).
        """
        logger.info("Executing per-table strategy (fallback)")

        # Step 1: Query applications
        app_query = self._build_app_query(record_ids, owner_logins)
        app_result = session.execute_query(app_query, timeout_seconds=timeout_seconds)
        apps = app_result["data"]
        logger.info("Apps: %d rows", len(apps))

        if not apps:
            return []

        # Extract record IDs for filtering subsequent queries
        app_names = list({row.get("record_id", "") for row in apps if row.get("record_id")})

        # Step 2: Query data stores filtered by app names
        stores = self._query_filtered(
            session, TABLE_DATA_STORES, "application_name", app_names, timeout_seconds
        )
        logger.info("Stores: %d rows", len(stores))

        # Step 3: Query data objects
        objects = self._query_filtered(
            session, TABLE_DATA_OBJECTS, "application_name", app_names, timeout_seconds
        )
        logger.info("Objects: %d rows", len(objects))

        # Step 4: Query fields
        fields = self._query_filtered(
            session, TABLE_OBJECT_FIELDS, "application_name", app_names, timeout_seconds
        )
        logger.info("Fields: %d rows", len(fields))

        # Step 5: Join in Python
        return self._join_in_python(apps, stores, objects, fields)

    def _build_app_query(
        self,
        record_ids: list[str] | None,
        owner_logins: list[str] | None,
    ) -> str:
        """Build the applications query with optional filters."""
        table = quote_table(TABLE_APPLICATIONS)
        where_parts: list[str] = []

        if record_ids:
            escaped = [n.replace("'", "''") for n in record_ids]
            in_list = ", ".join(f"'{n}'" for n in escaped)
            where_parts.append(f"record_id IN ({in_list})")

        if owner_logins:
            escaped = [l.replace("'", "''") for l in owner_logins]
            conditions = []
            for login in escaped:
                conditions.append(f"owner_login = '{login}'")
                conditions.append(f"supervisor_login = '{login}'")
                for level in range(1, 7):
                    conditions.append(f"reports_to_level_{level}_login = '{login}'")
            where_parts.append(f"({' OR '.join(conditions)})")

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        return f"SELECT * FROM {table} {where_sql}"

    def _query_filtered(
        self,
        session: WarehouseSession,
        table_name: str,
        filter_column: str,
        filter_values: list[str],
        timeout_seconds: int,
    ) -> list[dict[str, Any]]:
        """Query a table filtered by a list of values."""
        table = quote_table(table_name)
        escaped = [v.replace("'", "''") for v in filter_values]
        in_list = ", ".join(f"'{v}'" for v in escaped)
        query = f"SELECT * FROM {table} WHERE {filter_column} IN ({in_list})"
        result = session.execute_query(query, timeout_seconds=timeout_seconds)
        return result["data"]

    def _join_in_python(
        self,
        apps: list[dict],
        stores: list[dict],
        objects: list[dict],
        fields: list[dict],
    ) -> list[dict[str, Any]]:
        """Join four tables in Python using dict indexing (LEFT JOIN semantics)."""
        # Index stores by application_name
        stores_by_app: dict[str, list[dict]] = {}
        for s in stores:
            key = s.get("application_name", "")
            stores_by_app.setdefault(key, []).append(s)

        # Index objects by (application_name, data_store_id)
        objects_by_store: dict[tuple[str, str], list[dict]] = {}
        for o in objects:
            key = (o.get("application_name", ""), o.get("data_store_id", ""))
            objects_by_store.setdefault(key, []).append(o)

        # Index fields by (application_name, object_id)
        fields_by_object: dict[tuple[str, str], list[dict]] = {}
        for f in fields:
            key = (f.get("application_name", ""), f.get("object_id", ""))
            fields_by_object.setdefault(key, []).append(f)

        # Produce flat rows (LEFT JOIN semantics)
        rows: list[dict[str, Any]] = []
        for app in apps:
            app_name = app.get("record_id", "")
            app_stores = stores_by_app.get(app_name, [{}])

            for store in app_stores:
                store_id = store.get("store_id", "")
                store_objects = objects_by_store.get((app_name, store_id), [{}])

                for obj in store_objects:
                    obj_id = obj.get("object_id", "")
                    obj_fields = fields_by_object.get((app_name, obj_id), [{}])

                    for field in obj_fields:
                        row = {**app}
                        row.update(
                            {
                                "data_store_name": store.get("store_name"),
                                "data_store_id": store_id,
                                "store_technology": store.get("technology"),
                                "store_has_sensitive_data": store.get("has_sensitive_data"),
                                "data_object_name": obj.get("object_name"),
                                "object_id": obj_id,
                                "object_has_sensitive_data": obj.get("has_sensitive_data"),
                                "retention_days": obj.get("retention_days"),
                                "field_name": field.get("field_name"),
                                "field_description": field.get("field_description"),
                                "field_type": field.get("field_type"),
                            }
                        )
                        rows.append(row)

        return rows
