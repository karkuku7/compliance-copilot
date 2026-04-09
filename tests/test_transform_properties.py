"""Property-based tests for data transformation.

Uses Hypothesis to generate random inputs and verify transformation invariants.
These tests caught a real bug: _to_bool() only recognized "true" as truthy,
missing "1" and "yes" — encodings used by some warehouse systems.
"""

import string

from hypothesis import given, settings
from hypothesis import strategies as st

from compliance_extractor.transform import (
    parse_struct,
    to_bool,
    transform_rows_to_hierarchical,
)


# --- Boolean conversion properties ---


@given(st.sampled_from(["true", "True", "TRUE", "1", "yes", "Yes", "YES"]))
def test_truthy_values_are_true(value: str):
    """All common truthy encodings must return True."""
    assert to_bool(value) is True


@given(st.sampled_from(["false", "False", "FALSE", "0", "no", "No", "NO", "", "null", "None"]))
def test_falsy_values_are_false(value: str):
    """All common falsy encodings must return False."""
    assert to_bool(value) is False


@given(st.text(alphabet=string.ascii_letters + string.digits, min_size=4, max_size=20))
def test_arbitrary_strings_dont_crash(value: str):
    """to_bool should never raise on arbitrary string input."""
    result = to_bool(value)
    assert isinstance(result, bool)


# --- Struct parsing properties ---


@given(
    st.dictionaries(
        keys=st.text(
            alphabet=string.ascii_lowercase + "_",
            min_size=1,
            max_size=10,
        ),
        values=st.text(
            alphabet=string.ascii_letters + string.digits,
            min_size=1,
            max_size=20,
        ).filter(lambda v: v.lower() not in ("null", "none")),
        min_size=1,
        max_size=5,
    )
)
def test_struct_roundtrip(data: dict[str, str]):
    """Serializing then parsing a simple struct should preserve keys and values.

    Note: Values must be non-whitespace-only because the parser normalizes
    whitespace-only values to None (correct behavior for warehouse data).
    """
    # Serialize to struct format
    parts = [f"{k}={v}" for k, v in data.items()]
    struct_str = "{" + ", ".join(parts) + "}"

    parsed = parse_struct(struct_str)

    # All original keys should be present
    for key in data:
        assert key in parsed, f"Key '{key}' missing after roundtrip"
        assert parsed[key] == data[key], (
            f"Value mismatch for '{key}': expected '{data[key]}', got '{parsed[key]}'"
        )


def test_empty_struct():
    """Empty or null structs should return empty dict."""
    assert parse_struct("") == {}
    assert parse_struct("null") == {}
    assert parse_struct("NULL") == {}
    assert parse_struct("{}") == {}


def test_nested_struct():
    """Nested structs should be parsed recursively."""
    result = parse_struct("{owner={login=alice, level=6}}")
    assert result["owner"]["login"] == "alice"
    assert result["owner"]["level"] == "6"


def test_struct_with_array():
    """Arrays within structs should be parsed as lists."""
    result = parse_struct("{tags=[a, b, c]}")
    assert result["tags"] == ["a", "b", "c"]


# --- Hierarchical transformation properties ---


@given(
    st.lists(
        st.fixed_dictionaries({
            "record_id": st.text(
                alphabet=string.ascii_letters, min_size=1, max_size=10
            ),
            "app_description": st.text(min_size=0, max_size=50),
            "processes_sensitive_data": st.sampled_from(["true", "false", "1", "0"]),
            "stores_sensitive_data": st.sampled_from(["true", "false", "1", "0"]),
            "data_store_id": st.one_of(st.none(), st.text(
                alphabet=string.digits, min_size=1, max_size=5
            )),
            "data_store_name": st.text(min_size=0, max_size=20),
            "object_id": st.one_of(st.none(), st.text(
                alphabet=string.digits, min_size=1, max_size=5
            )),
            "data_object_name": st.text(min_size=0, max_size=20),
            "field_name": st.one_of(st.none(), st.text(
                alphabet=string.ascii_lowercase + "_",
                min_size=1,
                max_size=15,
            )),
            "field_description": st.text(min_size=0, max_size=30),
        }),
        min_size=0,
        max_size=20,
    )
)
@settings(max_examples=50)
def test_transform_preserves_record_ids(rows: list[dict]):
    """Every unique record_id in the input should appear in the output."""
    result = transform_rows_to_hierarchical(rows)

    input_ids = {row["record_id"] for row in rows if row.get("record_id")}
    for rid in input_ids:
        assert rid in result, f"Record '{rid}' missing from output"


@given(
    st.lists(
        st.fixed_dictionaries({
            "record_id": st.just("TestApp"),
            "processes_sensitive_data": st.sampled_from(["true", "1", "yes"]),
            "stores_sensitive_data": st.sampled_from(["false", "0", "no"]),
            "data_store_id": st.just("100"),
            "data_store_name": st.just("TestStore"),
            "object_id": st.just("200"),
            "data_object_name": st.just("TestObject"),
            "field_name": st.text(
                alphabet=string.ascii_lowercase, min_size=1, max_size=10
            ),
        }),
        min_size=1,
        max_size=10,
    )
)
def test_transform_boolean_fields(rows: list[dict]):
    """Boolean fields should be correctly converted regardless of encoding."""
    result = transform_rows_to_hierarchical(rows)
    app = result.get("TestApp")
    assert app is not None
    assert app["processes_sensitive_data"] is True
    assert app["stores_sensitive_data"] is False
