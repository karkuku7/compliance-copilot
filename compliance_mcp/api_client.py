"""Shared HTTP client for the Compliance Cache API.

Makes HTTPS calls with X-Source/X-Tool headers for source tracking.
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


class RecordNotFoundError(Exception):
    """Raised when a record is not found in the cache."""

    pass


class APIError(Exception):
    """Raised when an API call fails."""

    pass


def api_get(
    base_url: str,
    path: str,
    params: dict[str, str] | None = None,
    tool_name: str = "",
    timeout: int = 30,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Make a GET request to the Cache API with source tracking headers.

    Args:
        base_url: API base URL.
        path: URL path (e.g., "/lookup", "/stats").
        params: Query parameters.
        tool_name: MCP tool name for X-Tool header.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response.

    Raises:
        RecordNotFoundError: If the API returns 404.
        APIError: If the API returns an error or is unreachable.
    """
    query = "&".join(
        f"{k}={urllib.parse.quote(str(v), safe='')}"
        for k, v in (params or {}).items()
    )
    url = f"{base_url}{path}" + (f"?{query}" if query else "")

    logger.info("API GET %s", url)

    req = urllib.request.Request(url)
    req.add_header("X-Source", "mcp")
    req.add_header("X-Tool-Version", "mcp-1.0")
    if tool_name:
        req.add_header("X-Tool", tool_name)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RecordNotFoundError(f"Not found: {url}")
        raise APIError(f"API returned HTTP {exc.code}: {exc.reason}")
    except urllib.error.URLError as exc:
        raise APIError(f"Cannot reach API at {base_url}: {exc.reason}")
    except json.JSONDecodeError:
        raise APIError("API returned unexpected format (not JSON)")
