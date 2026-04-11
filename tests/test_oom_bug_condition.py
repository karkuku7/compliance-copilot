"""Bug condition exploration tests — chunked seed OOM on outlier records.

These tests encode the EXPECTED (fixed) behavior. They MUST FAIL on the
current unfixed code, confirming the bug exists:

1. execute_per_table() loads all rows for the entire chunk into memory
   without isolating outlier records (no sub-chunking).
2. _run_chunked() has no try/except — one chunk failure kills all
   subsequent chunks.
3. _estimate_chunk_size() only takes app_count, ignoring per-record data
   volume entirely.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from compliance_extractor.join_engine import JoinEngine
from scripts.seed_cache import _estimate_chunk_size, _run_chunked


# ── Helpers ───────────────────────────────────────────────────────────

def _make_app_row(record_id: str) -> dict:
    """Build a minimal application row dict."""
    return {
        "record_id": record_id,
        "app_description": "desc",
        "ownership": "N/A",
        "processes_sensitive_data": None,
        "stores_sensitive_data": None,
    }


def _make_do_row(record_id: str, ds_id: str, obj_id: str) -> dict:
    """Build a minimal data object row dict."""
    return {
        "application_name": record_id,
        "data_store_id": ds_id,
        "object_name": f"obj-{obj_id}",
        "object_id": obj_id,
        "has_sensitive_data": None,
        "retention_days": "",
    }


def _make_field_row(record_id: str, obj_id: str, field_name: str) -> dict:
    """Build a minimal object field row dict."""
    return {
        "application_name": record_id,
        "object_id": obj_id,
        "field_name": field_name,
        "field_description": "fd",
        "field_type": "string",
    }


def _make_ds_row(record_id: str, ds_id: str) -> dict:
    """Build a minimal data store row dict."""
    return {
        "application_name": record_id,
        "store_name": f"store-{ds_id}",
        "store_id": ds_id,
        "technology": "S3",
        "has_sensitive_data": None,
    }


def _build_mock_session(record_ids, outlier_record, outlier_do_count):
    """Build a mock session that returns large data for the outlier record.

    The mock routes queries based on the SQL table name in the query string:
    - Applications query → returns app rows for all records
    - Data stores query → returns 1 store per record
    - Data objects query → returns outlier_do_count rows for outlier_record,
      10 rows for each normal record
    - Object fields query → returns 1 field per data object
    """
    mock_session = MagicMock()
    mock_session.is_active = True

    def side_effect(query, **kwargs):
        q = query.lower()
        if "object_fields" in q or "field_name" in q:
            rows = []
            for rid in record_ids:
                count = outlier_do_count if rid == outlier_record else 10
                for i in range(count):
                    rows.append(_make_field_row(rid, str(i), f"field-{i}"))
            return {"success": True, "data": rows, "row_count": len(rows)}
        elif "data_objects" in q or "object_id" in q:
            rows = []
            for rid in record_ids:
                count = outlier_do_count if rid == outlier_record else 10
                for i in range(count):
                    rows.append(_make_do_row(rid, "ds-1", str(i)))
            return {"success": True, "data": rows, "row_count": len(rows)}
        elif "data_stores" in q or "store_id" in q:
            rows = [_make_ds_row(rid, "ds-1") for rid in record_ids]
            return {"success": True, "data": rows, "row_count": len(rows)}
        elif "count" in q and "group by" in q:
            # Probe query
            rows = []
            for rid in record_ids:
                count = outlier_do_count if rid == outlier_record else 10
                rows.append({"application_name": rid, "do_count": count})
            return {"success": True, "data": rows, "row_count": len(rows)}
        else:
            rows = [_make_app_row(rid) for rid in record_ids]
            return {"success": True, "data": rows, "row_count": len(rows)}

    mock_session.execute_query = MagicMock(side_effect=side_effect)
    return mock_session


# ── Test 1: Outlier record in chunk — no isolation on unfixed code ────

class TestOutlierRecordIsolation:
    """execute_per_table() should isolate outlier records into sub-chunks.

    On UNFIXED code, there is no isolation logic — the outlier record's 500K+
    rows are loaded alongside normal records in a single bulk query. The fixed
    code should detect the outlier via a COUNT(*) probe and process it
    separately.
    """

    def test_outlier_record_detected_and_isolated(self):
        """Fixed execute_per_table() should issue a COUNT probe and isolate
        outlier records into single-record sub-chunks.

        On unfixed code, this FAILS because there is no probe query and no
        isolation — all records are queried together in bulk.
        """
        record_ids = ["small-record-1", "outlier-record", "small-record-2"]
        outlier_do_count = 500_000

        mock_session = _build_mock_session(record_ids, "outlier-record", outlier_do_count)
        engine = JoinEngine()

        result = engine.execute_per_table(
            mock_session,
            record_ids=record_ids,
            timeout_seconds=600,
        )

        # The result should contain rows for ALL records
        assert len(result) > 0
        result_record_ids = set(row.get("record_id") for row in result)
        assert result_record_ids == set(record_ids), (
            f"Expected all records {set(record_ids)} in result, got {result_record_ids}"
        )

        # Key assertion: the fixed code should have issued a COUNT(*) probe
        # query to detect the outlier. On unfixed code, there are exactly 4
        # queries (apps, data_stores, data_objects, object_fields) with no
        # probe. The fixed code should have MORE than 4 calls because it
        # issues a COUNT probe + separate queries for the outlier record.
        call_count = mock_session.execute_query.call_count
        assert call_count > 4, (
            f"Expected more than 4 session.execute_query() calls "
            f"(probe + isolation), but got {call_count}. "
            f"This means execute_per_table() did NOT probe for outlier records "
            f"and did NOT isolate them — the bug condition is confirmed."
        )


# ── Test 2: Chunk failure cascade — no recovery on unfixed code ───────

class TestChunkFailureCascade:
    """_run_chunked() should continue processing after a chunk failure.

    On UNFIXED code, there is no try/except around chunk processing — an
    exception in chunk 2 kills chunks 3–5 entirely.
    """

    def test_remaining_chunks_execute_after_failure(self):
        """If chunk 2 of 5 fails, chunks 3–5 should still execute.

        On unfixed code, this FAILS because _run_chunked() has no try/except
        and the exception propagates, killing all remaining chunks.
        """
        from compliance_extractor.retry import RetryConfig

        # Build args for 50 records with chunk_size=10 → 5 chunks
        args = MagicMock()
        args.chunk_size = 10
        args.timeout = 600
        args.per_table = True
        args.dry_run = False

        # Mock session: record ID query returns 50 records
        mock_session = MagicMock()
        all_records = [f"record-{i:03d}" for i in range(50)]
        mock_session.execute_query.return_value = {
            "success": True,
            "data": [{"record_id": name} for name in all_records],
            "row_count": len(all_records),
        }

        engine = JoinEngine()
        call_counter = {"count": 0}

        def mock_execute_per_table(session, record_ids=None, **kwargs):
            call_counter["count"] += 1
            if call_counter["count"] == 2:
                raise RuntimeError("Simulated OOM on chunk 2")
            return []

        engine.execute_per_table = mock_execute_per_table
        config = RetryConfig(max_retries=1)

        # On unfixed code, this raises RuntimeError from chunk 2.
        # On fixed code, it catches the error and continues to chunks 3–5.
        try:
            _run_chunked(args, mock_session, engine, None, None, config)
        except RuntimeError:
            pytest.fail(
                "RuntimeError propagated from chunk 2 — _run_chunked() has no "
                "try/except for chunk failures. Chunks 3–5 were never processed. "
                "This confirms the bug: chunk failure cascade."
            )

        # If we get here, error recovery worked. Verify chunks 3–5 ran.
        assert call_counter["count"] >= 5, (
            f"Expected at least 5 execute_per_table() calls (all chunks), "
            f"but got {call_counter['count']}. Some chunks were skipped."
        )


# ── Test 3: Volume-blind chunk sizing ─────────────────────────────────

class TestVolumeBlindChunkSizing:
    """_estimate_chunk_size() should accept volume data and adjust sizing.

    On UNFIXED code, the function signature is _estimate_chunk_size(app_count)
    — it only takes an int and has no awareness of per-record data volume.
    """

    def test_estimate_chunk_size_accepts_volume_data(self):
        """Fixed _estimate_chunk_size() should accept a volume_data parameter.

        On unfixed code, this FAILS because the function only accepts
        app_count (a single int) and has no volume_data parameter.
        """
        sig = inspect.signature(_estimate_chunk_size)
        param_names = list(sig.parameters.keys())

        assert "volume_data" in param_names, (
            f"_estimate_chunk_size() signature is {sig} — it does not accept "
            f"a 'volume_data' parameter. This confirms the bug: chunk sizing "
            f"is volume-blind and only considers record count."
        )

    def test_estimate_chunk_size_returns_smaller_for_outlier_volume(self):
        """When volume_data shows an outlier record, chunk size should be smaller.

        On unfixed code, this FAILS because the function ignores volume data.
        """
        # 300 records → unfixed code returns 100 (AUTO_CHUNK_DEFAULT_SIZE)
        baseline = _estimate_chunk_size(300)

        # With volume data showing an outlier, the fixed function should
        # return a smaller chunk size to account for the large record.
        volume_data = {f"record-{i}": 100 for i in range(299)}
        volume_data["outlier-record"] = 812_000

        try:
            adjusted = _estimate_chunk_size(300, volume_data=volume_data)
        except TypeError:
            pytest.fail(
                "_estimate_chunk_size() does not accept volume_data keyword "
                "argument. This confirms the bug: chunk sizing is volume-blind."
            )

        assert adjusted < baseline, (
            f"Expected chunk size with outlier volume data ({adjusted}) to be "
            f"smaller than baseline ({baseline}), but it wasn't. "
            f"_estimate_chunk_size() ignores per-record data volume."
        )
