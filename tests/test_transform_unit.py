"""Unit tests for data transformation."""

import pytest

from compliance_extractor.transform import (
    parse_struct,
    to_bool,
    transform_rows_to_hierarchical,
)


class TestToBool:
    def test_true_string(self):
        assert to_bool("true") is True

    def test_one_string(self):
        assert to_bool("1") is True

    def test_yes_string(self):
        assert to_bool("yes") is True

    def test_false_string(self):
        assert to_bool("false") is False

    def test_zero_string(self):
        assert to_bool("0") is False

    def test_empty_string(self):
        assert to_bool("") is False

    def test_none(self):
        assert to_bool(None) is False

    def test_case_insensitive(self):
        assert to_bool("TRUE") is True
        assert to_bool("True") is True
        assert to_bool("YES") is True

    def test_whitespace(self):
        assert to_bool("  true  ") is True
        assert to_bool("  false  ") is False


class TestParseStruct:
    def test_simple_struct(self):
        result = parse_struct("{name=alice, role=admin}")
        assert result == {"name": "alice", "role": "admin"}

    def test_nested_struct(self):
        result = parse_struct("{owner={login=alice, level=6}}")
        assert result["owner"]["login"] == "alice"

    def test_array_value(self):
        result = parse_struct("{tags=[a, b, c]}")
        assert result["tags"] == ["a", "b", "c"]

    def test_null_value(self):
        result = parse_struct("{name=null}")
        assert result["name"] is None

    def test_empty_input(self):
        assert parse_struct("") == {}
        assert parse_struct("null") == {}
        assert parse_struct("NULL") == {}

    def test_deeply_nested(self):
        result = parse_struct("{a={b={c=deep}}}")
        assert result["a"]["b"]["c"] == "deep"


class TestTransformRowsToHierarchical:
    def test_single_complete_row(self):
        rows = [{
            "record_id": "App1",
            "app_description": "Test app",
            "processes_sensitive_data": "true",
            "stores_sensitive_data": "false",
            "data_store_id": "100",
            "data_store_name": "UserDB",
            "store_technology": "PostgreSQL",
            "store_has_sensitive_data": "true",
            "object_id": "200",
            "data_object_name": "Users",
            "object_has_sensitive_data": "true",
            "retention_days": "365",
            "field_name": "email",
            "field_description": "User email",
            "field_type": "PII",
        }]

        result = transform_rows_to_hierarchical(rows)

        assert "App1" in result
        app = result["App1"]
        assert app["processes_sensitive_data"] is True
        assert app["stores_sensitive_data"] is False
        assert len(app["data_stores"]) == 1
        assert app["data_stores"][0]["store_name"] == "UserDB"
        assert len(app["data_stores"][0]["data_objects"]) == 1
        assert app["data_stores"][0]["data_objects"][0]["fields"][0]["field_name"] == "email"

    def test_multiple_fields_same_object(self):
        rows = [
            {
                "record_id": "App1",
                "data_store_id": "100",
                "data_store_name": "DB",
                "object_id": "200",
                "data_object_name": "Users",
                "field_name": "email",
            },
            {
                "record_id": "App1",
                "data_store_id": "100",
                "data_store_name": "DB",
                "object_id": "200",
                "data_object_name": "Users",
                "field_name": "phone",
            },
        ]

        result = transform_rows_to_hierarchical(rows)
        fields = result["App1"]["data_stores"][0]["data_objects"][0]["fields"]
        assert len(fields) == 2
        field_names = {f["field_name"] for f in fields}
        assert field_names == {"email", "phone"}

    def test_deduplicates_fields(self):
        rows = [
            {"record_id": "App1", "data_store_id": "1", "object_id": "2", "field_name": "email"},
            {"record_id": "App1", "data_store_id": "1", "object_id": "2", "field_name": "email"},
        ]

        result = transform_rows_to_hierarchical(rows)
        fields = result["App1"]["data_stores"][0]["data_objects"][0]["fields"]
        assert len(fields) == 1

    def test_empty_input(self):
        assert transform_rows_to_hierarchical([]) == {}

    def test_app_with_no_stores(self):
        rows = [{"record_id": "App1", "app_description": "Empty app"}]
        result = transform_rows_to_hierarchical(rows)
        assert result["App1"]["data_stores"] == []
