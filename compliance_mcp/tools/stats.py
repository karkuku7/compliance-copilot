"""Stats Tool — retrieve cache and usage statistics."""

import json

from compliance_mcp.api_client import APIError, api_get
from compliance_mcp.config import MCPConfig

_config = MCPConfig()
VALID_STAT_TYPES = {"count", "list", "usage", "cache-stats", "reviewer-groups"}


def compliance_stats(stat_type: str) -> str:
    """Retrieve read-only cache and usage statistics.

    Args:
        stat_type: One of: count, list, usage, cache-stats.

    Returns:
        JSON string with the requested statistics.
    """
    if not stat_type or stat_type.strip() not in VALID_STAT_TYPES:
        return json.dumps({
            "error": "InvalidParams",
            "message": f"Invalid stat_type '{stat_type}'. Valid: {', '.join(sorted(VALID_STAT_TYPES))}",
        })

    try:
        data = api_get(
            _config.api_url, "/stats",
            params={"type": stat_type.strip()},
            tool_name="compliance_stats",
        )
        return json.dumps(data, indent=2, default=str)
    except APIError as exc:
        return json.dumps({"error": "APIError", "message": str(exc)})
