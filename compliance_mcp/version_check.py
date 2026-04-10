"""Standalone version check module for Compliance Copilot tools.

This module provides semantic version comparison, structured result types,
and version check interfaces (sync + async) for detecting available updates
and enforcing mandatory minimum versions.

Dependencies: Python stdlib only (no MCP, no CLI framework imports).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.example.com"

_HTTP_TIMEOUT_SECONDS = 5


@dataclass
class VersionCheckResult:
    """Result of a version check operation.

    Attributes:
        status: Outcome — one of "up_to_date", "update_available", "blocked", "error".
        local_version: The locally installed version string.
        latest_version: Latest version from the API, or None on error.
        minimum_required_version: Minimum required version from the API, or None on error.
        message: Human-readable notification or error message.
    """

    status: Literal["up_to_date", "update_available", "blocked", "error"]
    local_version: str
    latest_version: str | None
    minimum_required_version: str | None
    message: str


def _parse_semver(version: str) -> tuple[int, int, int] | None:
    """Parse a MAJOR.MINOR.PATCH version string into an integer tuple.

    Returns None if the string is not valid semver.
    """
    parts = version.strip().split(".")
    if len(parts) != 3:
        return None
    try:
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None
    if major < 0 or minor < 0 or patch < 0:
        return None
    return (major, minor, patch)


def compare_versions(local: str, remote: str) -> int:
    """Compare two semantic version strings.

    Returns:
        -1 if local < remote
         0 if local == remote
         1 if local > remote

    If either version is not valid semver, logs a warning and returns 0
    (treat as equal / up-to-date).
    """
    local_tuple = _parse_semver(local)
    remote_tuple = _parse_semver(remote)

    if local_tuple is None:
        logger.warning("Invalid semver for local version: %r — treating as equal", local)
        return 0
    if remote_tuple is None:
        logger.warning("Invalid semver for remote version: %r — treating as equal", remote)
        return 0

    if local_tuple < remote_tuple:
        return -1
    elif local_tuple > remote_tuple:
        return 1
    else:
        return 0


# Known pip-name → Brazil package name mappings.
_BRAZIL_PACKAGE_MAP: dict[str, str] = {
    "compliance-copilot": "ComplianceCopilot",
}


def _tool_name_to_brazil_package(tool_name: str) -> str:
    """Convert a pip-style tool name to a Brazil package name.

    Uses a known mapping for ecosystem tools, falls back to
    title-casing each hyphen-separated segment.

    Example: "compliance-copilot" -> "ComplianceCopilot"
    """
    if tool_name in _BRAZIL_PACKAGE_MAP:
        return _BRAZIL_PACKAGE_MAP[tool_name]
    parts = tool_name.split("-")
    return "".join(p.capitalize() for p in parts)


def _upgrade_instructions(tool_name: str, indent: str = "  ") -> str:
    """Return formatted upgrade instructions for all distribution methods."""
    brazil_pkg = _tool_name_to_brazil_package(tool_name)
    return (
        f"{indent}pip:     pip install --upgrade {tool_name}\n"
        f"{indent}brazil:  brazil ws use --package {brazil_pkg}\n"
        f"{indent}toolbox: toolbox install {tool_name}"
    )


def format_update_notification(
    local_version: str,
    latest_version: str,
    tool_name: str,
) -> str:
    """Format the Update_Notification message with upgrade instructions."""
    return (
        f"\u26a0\ufe0f  Update available: {tool_name} {local_version} \u2192 {latest_version}\n"
        f"\n"
        f"{_upgrade_instructions(tool_name, indent='  ')}"
    )


def format_block_message(
    local_version: str,
    minimum_required_version: str,
    tool_name: str,
) -> str:
    """Format the Update_Block_Message with upgrade instructions."""
    return (
        f"\U0001f6ab {tool_name} {local_version} is no longer supported.\n"
        f"   Minimum required version: {minimum_required_version}\n"
        f"\n"
        f"   Please update immediately:\n"
        f"{_upgrade_instructions(tool_name, indent='     ')}"
    )


# ---------------------------------------------------------------------------
# File-based cache layer
# ---------------------------------------------------------------------------

_DEFAULT_CACHE_TTL_SECONDS = 3600


def _get_cache_path() -> Path:
    """Return the path to the version cache JSON file.

    Location: ``~/.cache/compliance-copilot/version_cache.json``
    """
    return Path.home() / ".cache" / "compliance-copilot" / "version_cache.json"


def _read_cache(
    tool_name: str,
    cache_ttl_seconds: int = _DEFAULT_CACHE_TTL_SECONDS,
) -> dict | None:
    """Read a cached version entry for *tool_name*.

    Returns a dict with ``latest_version``, ``minimum_required_version``, and
    ``fetched_at`` if the entry exists and is still within the TTL window.

    Returns ``None`` when:
    - the cache file does not exist,
    - the file contains invalid JSON (corrupted → file is deleted),
    - the entry for *tool_name* is missing, or
    - the entry has expired (``time.time() - fetched_at > cache_ttl_seconds``).
    """
    cache_path = _get_cache_path()

    if not cache_path.is_file():
        return None

    try:
        raw = cache_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("Corrupted version cache — deleting: %s", exc)
        try:
            cache_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    if not isinstance(data, dict):
        logger.warning("Corrupted version cache (not a dict) — deleting")
        try:
            cache_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    entry = data.get(tool_name)
    if not isinstance(entry, dict):
        return None

    # Validate required keys
    for key in ("latest_version", "minimum_required_version", "fetched_at"):
        if key not in entry:
            return None

    fetched_at = entry.get("fetched_at", 0)
    if not isinstance(fetched_at, (int, float)):
        return None

    if time.time() - fetched_at > cache_ttl_seconds:
        return None

    return entry


def _write_cache(
    tool_name: str,
    latest_version: str,
    minimum_required_version: str,
) -> None:
    """Write (or update) a cache entry for *tool_name*.

    The cache file is written atomically via ``tempfile`` + ``os.replace``
    to avoid corruption from concurrent writes or crashes.  The cache
    directory is created on first write if it does not exist.
    """
    cache_path = _get_cache_path()
    cache_dir = cache_path.parent

    # Ensure the cache directory exists.
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Load existing cache (if any) so we preserve entries for other tools.
    existing: dict = {}
    if cache_path.is_file():
        try:
            raw = cache_path.read_text(encoding="utf-8")
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                existing = loaded
        except (OSError, json.JSONDecodeError, ValueError):
            # Corrupted — start fresh.
            existing = {}

    existing[tool_name] = {
        "latest_version": latest_version,
        "minimum_required_version": minimum_required_version,
        "fetched_at": time.time(),
    }

    # Atomic write: write to a temp file in the same directory, then replace.
    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=str(cache_dir), suffix=".tmp")
        os.write(fd, json.dumps(existing, indent=2).encode("utf-8"))
        os.close(fd)
        fd = None  # Mark as closed so the finally block doesn't double-close.
        os.replace(tmp_path, str(cache_path))
        tmp_path = None  # Replaced successfully — nothing to clean up.
    except OSError as exc:
        logger.warning("Failed to write version cache: %s", exc)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass



# ---------------------------------------------------------------------------
# Synchronous and asynchronous version check interfaces
# ---------------------------------------------------------------------------


def check_version_sync(
    tool_name: str,
    local_version: str,
    api_url: str = DEFAULT_API_URL,
    cache_ttl_seconds: int = 3600,
    skip_cache: bool = False,
) -> VersionCheckResult:
    """Synchronous version check for CLI tools.

    1. Check cache first (unless *skip_cache* is True).
    2. On cache miss, call ``GET {api_url}/version?tool_name={tool_name}``
       with a 5-second timeout.
    3. Parse JSON response for ``latest_version`` and ``minimum_required_version``.
    4. Write successful response to cache.
    5. Compare versions and return the appropriate status.
    6. On any error, return ``status="error"`` — caller decides behaviour.
    """
    # 1. Try cache
    if not skip_cache:
        cached = _read_cache(tool_name, cache_ttl_seconds=cache_ttl_seconds)
        if cached is not None:
            return _build_result(
                local_version=local_version,
                latest_version=cached["latest_version"],
                minimum_required_version=cached["minimum_required_version"],
                tool_name=tool_name,
            )

    # 2. Fetch from API
    url = f"{api_url}/version?tool_name={urllib.request.quote(tool_name, safe='')}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        logger.warning("Version check failed for %s: %s", tool_name, exc)
        return VersionCheckResult(
            status="error",
            local_version=local_version,
            latest_version=None,
            minimum_required_version=None,
            message=f"Version check failed: {exc}",
        )
    except json.JSONDecodeError as exc:
        logger.warning("Malformed JSON from version endpoint for %s: %s", tool_name, exc)
        return VersionCheckResult(
            status="error",
            local_version=local_version,
            latest_version=None,
            minimum_required_version=None,
            message=f"Version check failed: malformed response",
        )

    # 3. Validate response
    latest_version = data.get("latest_version")
    minimum_required_version = data.get("minimum_required_version")

    if not latest_version or not minimum_required_version:
        logger.warning("Incomplete version response for %s: %s", tool_name, data)
        return VersionCheckResult(
            status="error",
            local_version=local_version,
            latest_version=latest_version,
            minimum_required_version=minimum_required_version,
            message="Version check failed: incomplete response from server",
        )

    # 4. Write to cache
    try:
        _write_cache(tool_name, latest_version, minimum_required_version)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to write version cache: %s", exc)

    # 5. Build result with decision logic
    return _build_result(
        local_version=local_version,
        latest_version=latest_version,
        minimum_required_version=minimum_required_version,
        tool_name=tool_name,
    )


async def check_version_async(
    tool_name: str,
    local_version: str,
    api_url: str = DEFAULT_API_URL,
    cache_ttl_seconds: int = 3600,
    skip_cache: bool = False,
) -> VersionCheckResult:
    """Async version check for MCP servers.

    Runs the synchronous HTTP call in a thread executor via
    ``asyncio.to_thread`` so it doesn't block the event loop.
    """
    return await asyncio.to_thread(
        check_version_sync,
        tool_name,
        local_version,
        api_url=api_url,
        cache_ttl_seconds=cache_ttl_seconds,
        skip_cache=skip_cache,
    )


def _build_result(
    *,
    local_version: str,
    latest_version: str,
    minimum_required_version: str,
    tool_name: str,
) -> VersionCheckResult:
    """Apply the version decision logic and return a ``VersionCheckResult``.

    Decision rules:
    - If ``local < minimum_required`` → ``"blocked"``
    - Else if ``local < latest`` → ``"update_available"``
    - Else → ``"up_to_date"``
    """
    # Check if local is below minimum required version
    cmp_min = compare_versions(local_version, minimum_required_version)
    if cmp_min < 0:
        return VersionCheckResult(
            status="blocked",
            local_version=local_version,
            latest_version=latest_version,
            minimum_required_version=minimum_required_version,
            message=format_block_message(local_version, minimum_required_version, tool_name),
        )

    # Check if local is below latest version
    cmp_latest = compare_versions(local_version, latest_version)
    if cmp_latest < 0:
        return VersionCheckResult(
            status="update_available",
            local_version=local_version,
            latest_version=latest_version,
            minimum_required_version=minimum_required_version,
            message=format_update_notification(local_version, latest_version, tool_name),
        )

    # Up to date
    return VersionCheckResult(
        status="up_to_date",
        local_version=local_version,
        latest_version=latest_version,
        minimum_required_version=minimum_required_version,
        message=f"{tool_name} {local_version} is up to date.",
    )
