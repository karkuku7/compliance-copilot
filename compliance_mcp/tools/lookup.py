"""Lookup Tool — look up a compliance record by ID."""

import json

from compliance_mcp.api_client import APIError, RecordNotFoundError, api_get
from compliance_mcp.config import MCPConfig

_config = MCPConfig()


def compliance_lookup(record_id: str, partial: bool = False) -> str:
    """Look up a compliance record by ID.

    Args:
        record_id: Record identifier.
        partial: If true, do substring match returning a list of matches.

    Returns:
        JSON string with record data (exact) or list of matches (partial).
    """
    if not record_id or not record_id.strip():
        return json.dumps(
            {"error": "InvalidParams", "message": "record_id must be a non-empty string."}
        )

    record_id = record_id.strip()
    params = {"record_id": record_id}
    if partial:
        params["partial"] = "true"

    try:
        data = api_get(_config.api_url, "/lookup", params=params, tool_name="compliance_lookup")
        return json.dumps(data, indent=2, default=str)
    except RecordNotFoundError:
        return json.dumps({
            "error": "RecordNotFound",
            "message": f"Record '{record_id}' not found in cache.",
            "suggestion": f"Try a partial search with partial=true to find records matching '{record_id}'.",
        })
    except APIError as exc:
        return json.dumps({"error": "APIError", "message": str(exc)})
