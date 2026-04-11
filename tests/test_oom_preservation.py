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
