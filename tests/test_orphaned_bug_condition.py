"""Bug condition exploration tests — orphaned data objects dropped by pipeline.

These tests encode the EXPECTED (fixed) behavior. They confirm that
orphaned data objects (those with no data_store_id) are properly handled
at each pipeline stage:

1. _transform_rows_to_hierarchical() places orphaned DOs under a synthetic
   __ORPHANED__ data store instead of dropping them.
2. _join_in_python() produces flat rows for orphaned DOs with synthetic
   DS columns instead of silently skipping them.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

import pytest

from compliance_extractor.join_engine import JoinEngine
from compliance_extractor.constants import ORPHANED_SENTINEL
from compliance_extractor.transform import transform_rows_to_hierarchical


# ── Hypothesis strategies ─────────────────────────────────────────────

_identifier = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P"), min_codepoint=32, max_codepoint=126),
    min_size=1,
    max_size=30,
).filter(lambda s: s.strip() != "")

_app_name_st = _identifier
_object_id_st = _identifier
_field_name_st = _identifier
_technology_st = st.sampled_from(["DynamoDB", "S3", "RDS", "Redshift", "Warehouse"])


def _build_orphaned_flat_row(
    app_name: str,
    object_id: str,
    field_name: str,
    technology: str = "Warehouse",
) -> dict:
    """Build a flat row representing an orphaned data object.

    Uses the compliance-copilot transform schema column names.
    The row has data_store_id = None (the bug condition) but valid
    object_id and record_id.
    """
    return {
        "record_id": app_name,
        "app_description": "Test app",
        "ownership": "N/A",
        "processes_sensitive_data": False,
        "stores_sensitive_data": False,
        # Data store columns — NULL (orphaned)
        "data_store_name": None,
        "data_store_id": None,
        "store_technology": None,
        "store_has_sensitive_data": None,
        # Data object columns — valid (orphaned DO)
        "data_object_name": f"table-{object_id}",
        "object_id": object_id,
        "object_has_sensitive_data": True,
        "retention_days": "",
        # Field columns — valid
        "field_name": field_name,
        "field_description": "test field",
        "field_type": "string",
    }


# ── Test 1: Transform places orphaned rows under synthetic store ──────

class TestTransformOrphanedRows:
    """_transform_rows_to_hierarchical() should place orphaned DOs under
    a synthetic __ORPHANED__ data store.
    """

    @given(
        app_name=_app_name_st,
        object_id=_object_id_st,
        field_name=_field_name_st,
        technology=_technology_st,
    )
    @settings(max_examples=50, deadline=None)
    def test_orphaned_rows_appear_under_synthetic_data_store(
        self, app_name, object_id, field_name, technology
    ):
        """For any flat row where data_store_id is None but object_id and
        app_name are valid, the hierarchical output SHOULD contain the data
        object under a synthetic __ORPHANED__ data store.
        """
        row = _build_orphaned_flat_row(app_name, object_id, field_name, technology)
        result = transform_rows_to_hierarchical([row])

        assert app_name in result, (
            f"App '{app_name}' not found in transform output."
        )

        app_data = result[app_name]
        data_stores = app_data.get("data_stores", [])

        assert len(data_stores) > 0, (
            f"App '{app_name}' has 0 data stores but had an orphaned DO "
            f"with object_id='{object_id}'."
        )

        orphaned_stores = [
            ds for ds in data_stores
            if ds.get("store_id") == ORPHANED_SENTINEL
        ]
        assert len(orphaned_stores) == 1, (
            f"Expected exactly 1 '__ORPHANED__' synthetic data store, "
            f"found {len(orphaned_stores)}."
        )

        orphaned_ds = orphaned_stores[0]
        assert orphaned_ds.get("store_name") == ORPHANED_SENTINEL

        data_objects = orphaned_ds.get("data_objects", [])
        obj_ids = [do.get("object_id") for do in data_objects]
        assert object_id in obj_ids, (
            f"Orphaned data object '{object_id}' not found under __ORPHANED__ "
            f"data store."
        )


# ── Test 2: Python join produces flat rows for orphaned DOs ───────────

class TestPythonJoinOrphanedDOs:
    """_join_in_python() should produce flat rows for orphaned DOs with
    synthetic DS columns.
    """

    @given(
        app_name=_app_name_st,
        object_id=_object_id_st,
        field_name=_field_name_st,
        technology=_technology_st,
    )
    @settings(max_examples=50, deadline=None)
    def test_orphaned_do_produces_flat_rows_with_synthetic_ds(
        self, app_name, object_id, field_name, technology
    ):
        """For any DO row where data_store_id is None/empty, the Python
        join SHOULD produce flat rows with synthetic DS columns
        (data_store_name='__ORPHANED__', data_store_id='__ORPHANED__').
        """
        # Build the 4 input tables for _join_in_python
        # compliance-copilot schema: record_id for apps, application_name for child tables
        app_rows = [{
            "record_id": app_name,
            "description": "Test app",
        }]

        # No data stores for this app (orphaned-only)
        ds_rows = []

        # Orphaned DO — data_store_id is None
        do_rows = [{
            "application_name": app_name,
            "data_store_id": None,
            "object_name": f"table-{object_id}",
            "object_id": object_id,
            "has_sensitive_data": True,
            "technology": technology,
            "retention_days": "",
        }]

        # Field for the orphaned DO
        f_rows = [{
            "application_name": app_name,
            "object_id": object_id,
            "field_name": field_name,
            "field_description": "test field",
            "field_type": "string",
        }]

        engine = JoinEngine()
        flat_rows = engine._join_in_python(app_rows, ds_rows, do_rows, f_rows)

        rows_with_object = [
            r for r in flat_rows if r.get("object_id") == object_id
        ]
        assert len(rows_with_object) > 0, (
            f"Orphaned DO with object_id='{object_id}' not found in flat output. "
            f"_join_in_python produced {len(flat_rows)} rows, none with this object_id."
        )

        for row in rows_with_object:
            assert row.get("data_store_name") == ORPHANED_SENTINEL, (
                f"Expected data_store_name='__ORPHANED__' for orphaned DO, "
                f"got '{row.get('data_store_name')}'"
            )
            assert row.get("data_store_id") == ORPHANED_SENTINEL, (
                f"Expected data_store_id='__ORPHANED__' for orphaned DO, "
                f"got '{row.get('data_store_id')}'"
            )

        rows_with_field = [
            r for r in rows_with_object if r.get("field_name") == field_name
        ]
        assert len(rows_with_field) > 0, (
            f"Field '{field_name}' not found in flat rows for orphaned DO."
        )
