"""Preservation property tests — non-orphaned data objects unchanged.

These tests MUST PASS on the current code. They capture existing baseline
behavior for data objects with valid (non-NULL) data_store_id values,
ensuring the orphaned-DO fix does not regress any existing paths.

1. Transform preservation: rows with valid data_store_id produce correct
   app → data_stores[] → data_objects[] → fields[] hierarchy.
2. Python join preservation: app/ds/do/field rows with valid data_store_id
   produce correct flat rows with all columns present.
"""

from __future__ import annotations

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from compliance_extractor.join_engine import JoinEngine
from compliance_extractor.transform import transform_rows_to_hierarchical


# ── Hypothesis strategies ─────────────────────────────────────────────

_identifier = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), min_codepoint=48, max_codepoint=122),
    min_size=1,
    max_size=20,
).filter(lambda s: s.strip() != "")

_app_name_st = _identifier
_ds_id_st = _identifier
_ds_name_st = _identifier
_object_id_st = _identifier
_object_name_st = _identifier
_field_name_st = _identifier
_technology_st = st.sampled_from(["S3", "DynamoDB", "RDS", "Redshift", "Warehouse"])


# ── Row builders ──────────────────────────────────────────────────────


def _build_normal_flat_row(
    app_name: str,
    data_store_id: str,
    data_store_name: str,
    object_id: str,
    field_name: str,
    technology: str = "S3",
) -> dict:
    """Build a flat row with a VALID data_store_id (non-orphaned).

    Uses the compliance-copilot transform schema column names.
    """
    return {
        "record_id": app_name,
        "app_description": "Test app",
        "ownership": "N/A",
        "processes_sensitive_data": False,
        "stores_sensitive_data": False,
        # Data store columns — VALID (non-orphaned)
        "data_store_name": data_store_name,
        "data_store_id": data_store_id,
        "store_technology": technology,
        "store_has_sensitive_data": True,
        # Data object columns — valid
        "data_object_name": f"table-{object_id}",
        "object_id": object_id,
        "object_has_sensitive_data": True,
        "retention_days": "",
        # Field columns — valid
        "field_name": field_name,
        "field_description": "test field",
        "field_type": "string",
    }


# ── Test 1: Transform preservation ────────────────────────────────────

class TestTransformPreservation:
    """_transform_rows_to_hierarchical() with valid data_store_id rows
    produces correct app → data_stores[] → data_objects[] → fields[]
    hierarchy.
    """

    @given(
        app_name=_app_name_st,
        ds_id=_ds_id_st,
        ds_name=_ds_name_st,
        object_id=_object_id_st,
        field_name=_field_name_st,
        technology=_technology_st,
    )
    @settings(max_examples=50, deadline=None)
    def test_normal_rows_grouped_under_correct_data_store(
        self, app_name, ds_id, ds_name, object_id, field_name, technology
    ):
        """For any flat row with a valid data_store_id, the hierarchical
        output contains the data object under its correct parent data store.
        """
        row = _build_normal_flat_row(app_name, ds_id, ds_name, object_id, field_name, technology)
        result = transform_rows_to_hierarchical([row])

        assert app_name in result
        app_data = result[app_name]
        data_stores = app_data.get("data_stores", [])

        assert len(data_stores) == 1
        ds = data_stores[0]
        assert ds["store_id"] == ds_id
        assert ds["store_name"] == ds_name

        data_objects = ds.get("data_objects", [])
        assert len(data_objects) == 1
        do = data_objects[0]
        assert do["object_id"] == object_id

        fields = do.get("fields", [])
        assert len(fields) == 1
        assert fields[0]["field_name"] == field_name

    @given(
        app_name=_app_name_st,
        ds_ids=st.lists(_ds_id_st, min_size=2, max_size=4, unique=True),
        object_id=_object_id_st,
        field_name=_field_name_st,
    )
    @settings(max_examples=30, deadline=None)
    def test_multiple_data_stores_all_present(
        self, app_name, ds_ids, object_id, field_name
    ):
        """Multiple data stores for the same app all appear in the hierarchy."""
        rows = [
            _build_normal_flat_row(
                app_name, ds_id, f"store-{ds_id}",
                f"{object_id}-{ds_id}", field_name, "S3"
            )
            for ds_id in ds_ids
        ]
        result = transform_rows_to_hierarchical(rows)

        assert app_name in result
        data_stores = result[app_name]["data_stores"]
        assert len(data_stores) == len(ds_ids)

        result_ds_ids = {ds["store_id"] for ds in data_stores}
        assert result_ds_ids == set(ds_ids)


# ── Test 2: Python join preservation ──────────────────────────────────

class TestPythonJoinPreservation:
    """_join_in_python() with valid data_store_id rows produces correct
    flat rows with all DS/DO/field columns present.
    """

    @given(
        app_name=_app_name_st,
        ds_id=_ds_id_st,
        ds_name=_ds_name_st,
        object_id=_object_id_st,
        field_name=_field_name_st,
        technology=_technology_st,
    )
    @settings(max_examples=50, deadline=None)
    def test_normal_join_produces_flat_rows_with_all_columns(
        self, app_name, ds_id, ds_name, object_id, field_name, technology
    ):
        """For app/ds/do/field rows with valid data_store_id, the Python
        join produces flat rows containing DS, DO, and field columns.
        """
        # compliance-copilot schema uses record_id, application_name, store_id etc.
        app_rows = [{
            "record_id": app_name,
            "description": "Test app",
        }]

        ds_rows = [{
            "application_name": app_name,
            "store_name": ds_name,
            "store_id": ds_id,
            "technology": technology,
            "has_sensitive_data": True,
        }]

        do_rows = [{
            "application_name": app_name,
            "data_store_id": ds_id,
            "object_name": f"table-{object_id}",
            "object_id": object_id,
            "has_sensitive_data": True,
            "retention_days": "",
        }]

        f_rows = [{
            "application_name": app_name,
            "object_id": object_id,
            "field_name": field_name,
            "field_description": "test field",
            "field_type": "string",
        }]

        engine = JoinEngine()
        flat_rows = engine._join_in_python(app_rows, ds_rows, do_rows, f_rows)

        assert len(flat_rows) == 1

        row = flat_rows[0]
        assert row["record_id"] == app_name
        assert row["data_store_name"] == ds_name
        assert row["data_store_id"] == ds_id
        assert row["store_technology"] == technology
        assert row["object_id"] == object_id
        assert row["data_object_name"] == f"table-{object_id}"
        assert row["field_name"] == field_name

    @given(
        app_name=_app_name_st,
        ds_id=_ds_id_st,
        num_objects=st.integers(min_value=1, max_value=5),
        num_fields=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=30, deadline=None)
    def test_cross_product_row_count(
        self, app_name, ds_id, num_objects, num_fields
    ):
        """The flat row count equals objects × fields for a single data store."""
        app_rows = [{
            "record_id": app_name,
            "description": "Test app",
        }]

        ds_rows = [{
            "application_name": app_name,
            "store_name": f"store-{ds_id}",
            "store_id": ds_id,
            "technology": "S3",
            "has_sensitive_data": True,
        }]

        do_rows = []
        f_rows = []
        for i in range(num_objects):
            obj_id = f"obj-{i}"
            do_rows.append({
                "application_name": app_name,
                "data_store_id": ds_id,
                "object_name": f"table-{obj_id}",
                "object_id": obj_id,
                "has_sensitive_data": True,
                "retention_days": "",
            })
            for j in range(num_fields):
                f_rows.append({
                    "application_name": app_name,
                    "object_id": obj_id,
                    "field_name": f"field-{j}",
                    "field_description": "test",
                    "field_type": "string",
                })

        engine = JoinEngine()
        flat_rows = engine._join_in_python(app_rows, ds_rows, do_rows, f_rows)

        expected = num_objects * num_fields
        assert len(flat_rows) == expected, (
            f"Expected {expected} flat rows ({num_objects} objects × "
            f"{num_fields} fields), got {len(flat_rows)}"
        )
