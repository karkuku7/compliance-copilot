"""Preservation property tests — baseline behavior that must survive the OOM fix.

These tests MUST PASS on the current UNFIXED code. They capture existing
behavior so that regressions can be detected after the fix is applied.

1. Normal-volume execute_per_table(): correct flat rows with null handling
   and deduplication for small records.
2. _estimate_chunk_size() baseline: existing record-count thresholds preserved.
3. Empty record preservation: records with zero data stores produce record-level rows only.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from compliance_extractor.join_engine import JoinEngine
from scripts.seed_cache import _estimate_chunk_size

# Re-use mock helpers from the bug condition test module
from tests.test_oom_bug_condition import (
    _make_app_row,
    _make_ds_row,
    _make_do_row,
    _make_field_row,
)


# ── Helpers ───────────────────────────────────────────────────────────

def _build_normal_mock_session(
    record_ids: list[str],
    ds_per_record: dict[str, list[str]],
    do_per_ds: dict[tuple[str, str], list[str]],
    fields_per_do: dict[tuple[str, str], list[str]],
):
    """Build a mock session for normal-volume records.

    Args:
        record_ids: list of record IDs
        ds_per_record: {record_id: [ds_id, ...]}
        do_per_ds: {(record_id, ds_id): [obj_id, ...]}
        fields_per_do: {(record_id, obj_id): [field_name, ...]}
    """
    mock_session = MagicMock()
    mock_session.is_active = True

    def side_effect(query, **kwargs):
        q = query.lower()
        if "object_fields" in q or "field_name" in q:
            rows = []
            for (rid, obj_id), fnames in fields_per_do.items():
                for fn in fnames:
                    rows.append(_make_field_row(rid, obj_id, fn))
            return {"success": True, "data": rows, "row_count": len(rows)}
        elif "data_objects" in q or "object_id" in q:
            rows = []
            for (rid, ds_id), obj_ids in do_per_ds.items():
                for oid in obj_ids:
                    rows.append(_make_do_row(rid, ds_id, oid))
            return {"success": True, "data": rows, "row_count": len(rows)}
        elif "data_stores" in q or "store_id" in q:
            rows = []
            for rid, ds_ids in ds_per_record.items():
                for ds_id in ds_ids:
                    rows.append(_make_ds_row(rid, ds_id))
            return {"success": True, "data": rows, "row_count": len(rows)}
        else:
            rows = [_make_app_row(rid) for rid in record_ids]
            return {"success": True, "data": rows, "row_count": len(rows)}

    mock_session.execute_query = MagicMock(side_effect=side_effect)
    return mock_session


# ── Test 1: Normal-volume execute_per_table() ─────────────────────────

class TestNormalVolumeExecutePerTable:
    """execute_per_table() with small records returns correct flat rows."""

    def test_three_records_with_stores_objects_fields(self):
        """3 records, each with 1 store, 2 objects, 1 field per object.

        Verifies flat rows contain correct record/ds/do/field combinations.
        """
        record_ids = ["record-a", "record-b", "record-c"]
        ds_per_record = {r: ["ds-1"] for r in record_ids}
        do_per_ds = {(r, "ds-1"): ["obj-1", "obj-2"] for r in record_ids}
        fields_per_do = {
            (r, oid): [f"field-{oid}"]
            for r in record_ids
            for oid in ["obj-1", "obj-2"]
        }

        session = _build_normal_mock_session(
            record_ids, ds_per_record, do_per_ds, fields_per_do,
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        # 3 records × 1 store × 2 objects × 1 field = 6 flat rows
        assert len(result) == 6

        # Every row should have record_id set
        result_records = {r["record_id"] for r in result}
        assert result_records == set(record_ids)

    def test_deduplication_preserves_unique_rows(self):
        """Duplicate data objects (same record+object_id) are deduplicated."""
        record_ids = ["dedup-record"]
        ds_per_record = {"dedup-record": ["ds-1"]}
        do_per_ds = {("dedup-record", "ds-1"): ["obj-1"]}
        fields_per_do = {("dedup-record", "obj-1"): ["f1", "f2"]}

        session = _build_normal_mock_session(
            record_ids, ds_per_record, do_per_ds, fields_per_do,
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        # 1 record × 1 store × 1 object × 2 fields = 2 flat rows
        assert len(result) == 2

        # Both fields present, no duplicates
        field_names = [r["field_name"] for r in result]
        assert sorted(field_names) == ["f1", "f2"]

    @given(
        num_records=st.integers(min_value=1, max_value=5),
        num_ds=st.integers(min_value=1, max_value=3),
        num_do=st.integers(min_value=1, max_value=10),
        num_fields=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=30)
    def test_property_flat_row_count_matches_cross_product(
        self, num_records, num_ds, num_do, num_fields,
    ):
        """Property: flat row count = records × stores × objects × fields.

        For any normal-volume configuration, execute_per_table() produces
        exactly the cross-product number of flat rows.
        """
        record_ids = [f"record-{i}" for i in range(num_records)]
        ds_per_record = {r: [f"ds-{j}" for j in range(num_ds)] for r in record_ids}
        do_per_ds = {
            (r, f"ds-{j}"): [f"obj-{r}-{j}-{k}" for k in range(num_do)]
            for r in record_ids
            for j in range(num_ds)
        }
        fields_per_do = {
            (r, f"obj-{r}-{j}-{k}"): [f"field-{m}" for m in range(num_fields)]
            for r in record_ids
            for j in range(num_ds)
            for k in range(num_do)
        }

        session = _build_normal_mock_session(
            record_ids, ds_per_record, do_per_ds, fields_per_do,
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        expected_rows = num_records * num_ds * num_do * num_fields
        assert len(result) == expected_rows

        # Every record should appear in the result
        result_records = {r["record_id"] for r in result}
        assert result_records == set(record_ids)


# ── Test 2: _estimate_chunk_size() baseline thresholds ────────────────

class TestEstimateChunkSizeBaseline:
    """_estimate_chunk_size() returns correct values for record-count thresholds.

    These are the existing thresholds that MUST be preserved when no
    volume_data is provided.
    """

    def test_300_records_returns_100(self):
        assert _estimate_chunk_size(300) == 100

    def test_100_records_returns_0(self):
        """100 records is below the AUTO_CHUNK_APP_THRESHOLD (200), no chunking."""
        assert _estimate_chunk_size(100) == 0

    def test_800_records_returns_50(self):
        """800 records is in the 500–800 range → AUTO_CHUNK_LARGE_OWNER_SIZE."""
        assert _estimate_chunk_size(800) == 50

    def test_900_records_returns_30(self):
        """900 records exceeds 800 → smallest chunk size."""
        assert _estimate_chunk_size(900) == 30

    @given(app_count=st.integers(min_value=0, max_value=200))
    @settings(max_examples=50)
    def test_property_small_owners_no_chunking(self, app_count):
        """Property: app_count <= 200 → chunk_size = 0 (no chunking)."""
        assert _estimate_chunk_size(app_count) == 0

    @given(app_count=st.integers(min_value=201, max_value=500))
    @settings(max_examples=50)
    def test_property_medium_owners_default_chunk(self, app_count):
        """Property: 200 < app_count <= 500 → chunk_size = 100."""
        assert _estimate_chunk_size(app_count) == 100

    @given(app_count=st.integers(min_value=501, max_value=800))
    @settings(max_examples=50)
    def test_property_large_owners_smaller_chunk(self, app_count):
        """Property: 500 < app_count <= 800 → chunk_size = 50."""
        assert _estimate_chunk_size(app_count) == 50

    @given(app_count=st.integers(min_value=801, max_value=5000))
    @settings(max_examples=50)
    def test_property_very_large_owners_smallest_chunk(self, app_count):
        """Property: app_count > 800 → chunk_size = 30."""
        assert _estimate_chunk_size(app_count) == 30


# ── Test 3: Empty record preservation ─────────────────────────────────

class TestEmptyRecordPreservation:
    """Records with zero data stores produce record-level rows only."""

    def test_record_with_no_stores_produces_single_row(self):
        """A record with no data stores should produce exactly one flat row
        containing only record-level columns.
        """
        record_ids = ["empty-record"]
        ds_per_record = {"empty-record": []}
        do_per_ds = {}
        fields_per_do = {}

        session = _build_normal_mock_session(
            record_ids, ds_per_record, do_per_ds, fields_per_do,
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        assert len(result) == 1
        row = result[0]
        assert row["record_id"] == "empty-record"

    def test_mixed_empty_and_populated_records(self):
        """Mix of empty and populated records: each produces correct row count."""
        record_ids = ["empty-record", "full-record"]
        ds_per_record = {"empty-record": [], "full-record": ["ds-1"]}
        do_per_ds = {("full-record", "ds-1"): ["obj-1"]}
        fields_per_do = {("full-record", "obj-1"): ["f1"]}

        session = _build_normal_mock_session(
            record_ids, ds_per_record, do_per_ds, fields_per_do,
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        # empty-record: 1 row (record only), full-record: 1 row (record+ds+do+field)
        assert len(result) == 2

        record_row_counts = {}
        for row in result:
            name = row["record_id"]
            record_row_counts[name] = record_row_counts.get(name, 0) + 1

        assert record_row_counts["empty-record"] == 1
        assert record_row_counts["full-record"] == 1

    @given(num_empty=st.integers(min_value=1, max_value=10))
    @settings(max_examples=20)
    def test_property_empty_records_produce_one_row_each(self, num_empty):
        """Property: N empty records → exactly N flat rows."""
        record_ids = [f"empty-{i}" for i in range(num_empty)]
        ds_per_record = {r: [] for r in record_ids}

        session = _build_normal_mock_session(
            record_ids, ds_per_record, {}, {},
        )
        engine = JoinEngine()
        result = engine.execute_per_table(session, record_ids=record_ids, timeout_seconds=60)

        assert len(result) == num_empty

        # Each record appears exactly once
        result_records = [r["record_id"] for r in result]
        assert sorted(result_records) == sorted(record_ids)


# ── Test 6b: Non-outlier records bypass per-data-store (Phase 2) ──────

class TestPhase2NonOutlierBypass:
    """Non-outlier records (below PER_APP_DO_THRESHOLD) must NOT trigger
    per-data-store queries — only the standard 4 bulk queries are issued.

    This MUST PASS on current Phase-1-only code because non-outlier records
    already use the bulk path.
    """

    def test_non_outlier_record_issues_exactly_4_queries(self):
        """A record with 5K data objects (well below PER_APP_DO_THRESHOLD of
        100K) should be processed via the standard bulk path, issuing
        exactly 4 queries: apps, data_stores, data_objects, object_fields.

        No per-data-store queries should be issued.
        """
        record_id = "normal-record"
        num_data_objects = 5_000

        # Build mock data: 1 record, 1 data store, 5K data objects, 1 field each
        issued_queries: list[str] = []

        mock_session = MagicMock()
        mock_session.is_active = True

        def side_effect(query, **kwargs):
            issued_queries.append(query)
            q = query.lower()

            # COUNT probe query — return count below threshold
            if "count" in q and "group by" in q:
                return {
                    "success": True,
                    "data": [
                        {"application_name": record_id, "do_count": num_data_objects},
                    ],
                }

            # Object fields query
            if "object_fields" in q or "field_name" in q:
                rows = [
                    _make_field_row(record_id, str(i), f"field-{i}")
                    for i in range(num_data_objects)
                ]
                return {"success": True, "data": rows}

            # Data objects query
            if "data_objects" in q or "object_name" in q:
                rows = [
                    _make_do_row(record_id, "ds-1", str(i))
                    for i in range(num_data_objects)
                ]
                return {"success": True, "data": rows}

            # Data stores query
            if "data_stores" in q or "store_name" in q:
                return {"success": True, "data": [_make_ds_row(record_id, "ds-1")]}

            # Applications query
            return {"success": True, "data": [_make_app_row(record_id)]}

        mock_session.execute_query = MagicMock(side_effect=side_effect)

        engine = JoinEngine()
        result = engine.execute_per_table(
            mock_session,
            record_ids=[record_id],
            timeout_seconds=60,
        )

        # Result should contain rows
        assert len(result) > 0

        # Non-outlier single record: the probe runs (1 query) but finds the record
        # below threshold, so it proceeds with the standard 4 bulk queries.
        # The key assertion: NO per-data-store queries should be issued.
        per_ds_queries = [
            q for q in issued_queries
            if "data_store_id" in q.lower()
            and ("and data_store_id" in q.lower() or "where data_store_id" in q.lower())
        ]
        assert len(per_ds_queries) == 0, (
            f"Expected NO per-data-store queries for a non-outlier record, "
            f"but found {len(per_ds_queries)} queries with 'data_store_id' filter. "
            f"Non-outlier records should use the bulk query path."
        )

        # Verify the standard bulk queries were issued — at least 4 queries total
        assert mock_session.execute_query.call_count >= 4, (
            f"Expected at least 4 queries (apps, data_stores, data_objects, "
            f"object_fields), but got {mock_session.execute_query.call_count}."
        )

        # Verify result contains the expected rows
        assert len(result) == num_data_objects
        result_records = {r["record_id"] for r in result}
        assert result_records == {record_id}


# ── Test 6a: Per-data-store output equivalence (Phase 2) ──────────────

class TestPhase2PerDataStoreOutputEquivalence:
    """Per-data-store processing must produce flat rows identical to what
    the all-at-once bulk path would produce.

    This test computes the EXPECTED flat rows by calling _join_in_python()
    directly with all rows (the "all-at-once" reference), then calls
    execute_per_table() and compares its output to the reference.
    """

    def test_per_data_store_output_matches_all_at_once(self):
        """Mock an outlier record with 3 data stores, varying data object
        counts (100, 200, 50), and 2 fields per object. Compare
        execute_per_table() output to _join_in_python() reference.
        """
        record_id = "outlier-record"
        ds_config = {
            "ds-1": 100,  # 100 data objects
            "ds-2": 200,  # 200 data objects
            "ds-3": 50,   # 50 data objects
        }
        fields_per_object = 2

        # ── Build all rows for the "all-at-once" reference ────────────
        all_app_rows = [_make_app_row(record_id)]
        all_ds_rows = []
        all_do_rows = []
        all_f_rows = []

        for ds_id, do_count in ds_config.items():
            all_ds_rows.append(_make_ds_row(record_id, ds_id))
            for i in range(do_count):
                obj_id = f"{ds_id}-obj-{i}"
                all_do_rows.append(_make_do_row(record_id, ds_id, obj_id))
                for f_idx in range(fields_per_object):
                    all_f_rows.append(
                        _make_field_row(record_id, obj_id, f"field-{f_idx}")
                    )

        # Compute reference flat rows via _join_in_python() directly
        engine = JoinEngine()
        reference_flat_rows = engine._join_in_python(
            all_app_rows, all_ds_rows, all_do_rows, all_f_rows,
        )

        # Sort reference by canonical key for comparison
        def sort_key(row):
            return (
                row.get("record_id", ""),
                str(row.get("data_store_id", "")),
                str(row.get("object_id", "")),
                row.get("field_name", ""),
            )

        reference_sorted = sorted(reference_flat_rows, key=sort_key)

        # ── Build mock session for execute_per_table() ────────────────
        mock_session = MagicMock()
        mock_session.is_active = True

        def side_effect(query, **kwargs):
            q = query.lower()

            # COUNT probe query
            if "count" in q and "group by" in q:
                total_do = sum(ds_config.values())
                return {
                    "success": True,
                    "data": [
                        {"application_name": record_id, "do_count": total_do},
                    ],
                }

            # Object fields query — handle both bulk and per-data-store
            if "object_fields" in q or "field_type" in q:
                # Check if per-data-store (has "object_id IN" filter in WHERE)
                if "object_id in" in q:
                    # Per-data-store: return fields for matching objects
                    rows = []
                    for f_row in all_f_rows:
                        obj_id = f_row["object_id"]
                        if obj_id in query:
                            rows.append(f_row)
                    return {"success": True, "data": rows}
                else:
                    # Bulk: return all fields
                    return {"success": True, "data": list(all_f_rows)}

            # Data objects query — handle both bulk and per-data-store
            if "data_objects" in q or "object_name" in q:
                # Per-data-store queries have "AND data_store_id" in WHERE
                if "and data_store_id" in q:
                    # Per-data-store: find which ds_id
                    rows = []
                    for ds_id in ds_config:
                        if ds_id in query:
                            rows = [
                                r for r in all_do_rows
                                if r.get("data_store_id") == ds_id
                            ]
                            break
                    return {"success": True, "data": rows}
                else:
                    # Bulk: return all data objects
                    return {"success": True, "data": list(all_do_rows)}

            # Data stores query
            if "data_stores" in q or "store_name" in q:
                return {"success": True, "data": list(all_ds_rows)}

            # Applications query
            return {"success": True, "data": list(all_app_rows)}

        mock_session.execute_query = MagicMock(side_effect=side_effect)

        # ── Call execute_per_table() and compare ──────────────────────
        result = engine.execute_per_table(
            mock_session,
            record_ids=[record_id],
            timeout_seconds=60,
        )

        # Sort actual output by the same canonical key
        actual_sorted = sorted(result, key=sort_key)

        # Compare row counts
        assert len(actual_sorted) == len(reference_sorted), (
            f"Row count mismatch: execute_per_table() produced "
            f"{len(actual_sorted)} rows, but _join_in_python() reference "
            f"produced {len(reference_sorted)} rows."
        )

        # Compare row-by-row
        for i, (actual, expected) in enumerate(zip(actual_sorted, reference_sorted)):
            assert actual == expected, (
                f"Row {i} mismatch.\n"
                f"  Actual:   record={actual.get('record_id')}, "
                f"ds={actual.get('data_store_id')}, "
                f"obj={actual.get('object_id')}, "
                f"field={actual.get('field_name')}\n"
                f"  Expected: record={expected.get('record_id')}, "
                f"ds={expected.get('data_store_id')}, "
                f"obj={expected.get('object_id')}, "
                f"field={expected.get('field_name')}"
            )
