"""Data transformation utilities.

Handles:
- Recursive struct parsing (warehouse nested key-value types)
- Boolean encoding normalization
- Flat rows → hierarchical JSON transformation
"""

from typing import Any

from compliance_extractor.constants import ORPHANED_SENTINEL


def parse_struct(value: str) -> dict[str, Any]:
    """Recursively parse a warehouse struct string into a Python dict.

    Handles nested structs, arrays, NULL values, and escaped characters.

    Examples:
        >>> parse_struct("{name=alice, role=admin}")
        {'name': 'alice', 'role': 'admin'}

        >>> parse_struct("{owner={login=alice, level=6}}")
        {'owner': {'login': 'alice', 'level': '6'}}

        >>> parse_struct("{tags=[a, b, c]}")
        {'tags': ['a', 'b', 'c']}
    """
    if not value or value.strip() in ("", "null", "NULL", "None"):
        return {}

    value = value.strip()
    if value.startswith("{") and value.endswith("}"):
        value = value[1:-1].strip()

    result: dict[str, Any] = {}
    i = 0
    current_key = ""
    current_value = ""
    depth = 0
    in_key = True

    while i < len(value):
        char = value[i]

        if char in ("{", "["):
            depth += 1
            current_value += char
        elif char in ("}", "]"):
            depth -= 1
            current_value += char
        elif char == "=" and depth == 0 and in_key:
            current_key = current_value.strip()
            current_value = ""
            in_key = False
        elif char == "," and depth == 0:
            if current_key:
                result[current_key] = _parse_value(current_value.strip())
            current_key = ""
            current_value = ""
            in_key = True
        else:
            current_value += char

        i += 1

    # Handle last key-value pair
    if current_key:
        result[current_key] = _parse_value(current_value.strip())

    return result


def _parse_value(value: str) -> Any:
    """Parse a single value — may be a nested struct, array, or scalar."""
    if value is None:
        return None
    if value.strip() == "" or value.strip() in ("null", "NULL", "None"):
        return None
    if value.startswith("{") and value.endswith("}"):
        return parse_struct(value)
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_value(item.strip()) for item in _split_top_level(inner)]
    return value


def _split_top_level(text: str) -> list[str]:
    """Split a comma-separated string respecting nested braces/brackets."""
    parts: list[str] = []
    current = ""
    depth = 0
    for char in text:
        if char in ("{", "["):
            depth += 1
            current += char
        elif char in ("}", "]"):
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    if current:
        parts.append(current)
    return parts


def to_bool(value: Any) -> bool:
    """Convert a warehouse value to a Python boolean.

    Handles all common encodings: true/false, 1/0, yes/no, True/False.
    This is critical — different warehouse systems use different encodings.
    A bug here silently corrupts boolean fields across the entire cache.
    """
    return str(value).strip().lower() in ("true", "1", "yes")


def transform_rows_to_hierarchical(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Transform flat joined rows into nested hierarchical JSON.

    Input: flat rows from a multi-table JOIN, each containing columns from
    all four tables (applications, data_stores, data_objects, object_fields).

    Output: dict keyed by record_id, each containing nested data_stores →
    data_objects → fields.

    Example output structure:
        {
            "MyApp": {
                "record_id": "MyApp",
                "description": "...",
                "processes_sensitive_data": True,
                "data_stores": [
                    {
                        "store_name": "UserDB",
                        "store_id": "123",
                        "data_objects": [
                            {
                                "object_name": "Users",
                                "object_id": "456",
                                "fields": [
                                    {"field_name": "email", "field_type": "PII"}
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    """
    apps: dict[str, dict[str, Any]] = {}

    for row in rows:
        record_id = row.get("record_id") or row.get("app_name", "")
        if not record_id:
            continue

        # Initialize app if first time seeing it
        if record_id not in apps:
            apps[record_id] = {
                "record_id": record_id,
                "description": row.get("app_description", ""),
                "owner": _parse_owner(row.get("ownership", "")),
                "processes_sensitive_data": to_bool(
                    row.get("processes_sensitive_data", False)
                ),
                "stores_sensitive_data": to_bool(
                    row.get("stores_sensitive_data", False)
                ),
                "data_stores": [],
                "_store_index": {},  # Temporary index for dedup
            }

        app = apps[record_id]
        store_id = row.get("data_store_id")
        object_id = row.get("object_id")

        # Route orphaned rows (no data_store_id but valid object_id)
        # to a synthetic __ORPHANED__ data store instead of dropping them.
        if not store_id:
            if not object_id:
                # No data_store_id AND no object_id — nothing to nest
                continue
            # Orphaned row: use sentinel values
            store_id = ORPHANED_SENTINEL

        # Initialize store if first time seeing it
        if store_id not in app["_store_index"]:
            if store_id == ORPHANED_SENTINEL:
                store = {
                    "store_name": ORPHANED_SENTINEL,
                    "store_id": ORPHANED_SENTINEL,
                    "technology": row.get("store_technology", ""),
                    "has_sensitive_data": False,
                    "data_objects": [],
                    "_object_index": {},
                }
            else:
                store = {
                    "store_name": row.get("data_store_name", ""),
                    "store_id": store_id,
                    "technology": row.get("store_technology", ""),
                    "has_sensitive_data": to_bool(
                        row.get("store_has_sensitive_data", False)
                    ),
                    "data_objects": [],
                    "_object_index": {},
                }
            app["data_stores"].append(store)
            app["_store_index"][store_id] = store

        store = app["_store_index"][store_id]
        if not object_id:
            continue

        # Initialize object if first time seeing it
        if object_id not in store["_object_index"]:
            obj = {
                "object_name": row.get("data_object_name", ""),
                "object_id": object_id,
                "has_sensitive_data": to_bool(
                    row.get("object_has_sensitive_data", False)
                ),
                "retention_days": row.get("retention_days", ""),
                "fields": [],
                "_field_index": set(),
            }
            store["data_objects"].append(obj)
            store["_object_index"][object_id] = obj

        obj = store["_object_index"][object_id]
        field_name = row.get("field_name")
        if not field_name or field_name in obj["_field_index"]:
            continue

        obj["fields"].append(
            {
                "field_name": field_name,
                "field_description": row.get("field_description", ""),
                "field_type": row.get("field_type", ""),
            }
        )
        obj["_field_index"].add(field_name)

    # Clean up temporary indices
    for app in apps.values():
        del app["_store_index"]
        for store in app["data_stores"]:
            del store["_object_index"]
            for obj in store["data_objects"]:
                del obj["_field_index"]

    return apps


def _parse_owner(value: str) -> str:
    """Extract owner login from an ownership struct or plain string."""
    if not value:
        return ""
    if value.startswith("{"):
        parsed = parse_struct(value)
        return parsed.get("owner_login", parsed.get("login", ""))
    return value
