"""Compliance Copilot MCP Server — main entry point.

Exposes four tools: compliance_lookup, compliance_review,
compliance_findings, compliance_stats.

Uses FastMCP with stdio transport.
"""

import logging
import os
import signal

from anyio import create_task_group, open_signal_receiver, run
from anyio.abc import CancelScope
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools import Tool

from compliance_mcp.tools.findings import compliance_findings
from compliance_mcp.tools.lookup import compliance_lookup
from compliance_mcp.tools.review import compliance_review
from compliance_mcp.tools.stats import compliance_stats
from compliance_mcp.utils import get_package_version
from compliance_mcp.version_check import check_version_async

logger = logging.getLogger(__name__)


async def signal_handler(scope: CancelScope):
    """Handle SIGINT and SIGTERM signals."""
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for _ in signals:
            print("Shutting down MCP server...")
            os._exit(0)


def _block_all_tools(mcp: FastMCP, block_message: str) -> None:
    """Replace all registered tool handlers with a blocker that returns the block message.

    Iterates over the internal tool registry and swaps each tool's ``fn``
    attribute with an async wrapper that always raises ``ToolError`` with
    the provided *block_message*.  This ensures every tool invocation
    returns the update-required error to the caller.
    """
    from mcp.server.fastmcp.exceptions import ToolError

    tools = mcp._tool_manager._tools

    for tool_name, tool in tools.items():

        async def _blocked_handler(**kwargs: object) -> str:
            raise ToolError(block_message)

        tool.fn = _blocked_handler
        tool.is_async = True


async def run_server():
    """Run the MCP server with signal handling."""
    mcp = FastMCP(
        name="ComplianceCopilot",
        instructions=(
            "Compliance Copilot MCP server. Provides compliance record lookup, "
            "automated structured reviews, findings interpretation, and cache statistics."
        ),
    )

    mcp._mcp_server.version = get_package_version()

    tools = [
        Tool.from_function(
            fn=compliance_lookup,
            name="compliance_lookup",
            description=(
                "Look up a compliance record by ID. "
                "Returns hierarchical attestation data (exact match) or a list of "
                "matching records (partial/substring match)."
            ),
        ),
        Tool.from_function(
            fn=compliance_review,
            name="compliance_review",
            description=(
                "Run a structured 5-gate compliance review of a record "
                "via LLM. Returns the review report as markdown."
            ),
        ),
        Tool.from_function(
            fn=compliance_findings,
            name="compliance_findings",
            description=(
                "Parse and summarize review findings from a review report. "
                "Supports filtering by gate number (1-5) or data store/table name."
            ),
        ),
        Tool.from_function(
            fn=compliance_stats,
            name="compliance_stats",
            description=(
                "Retrieve read-only cache and usage statistics. "
                "Returns counts, record lists, usage metrics, cache health, "
                "or per-reviewer-group aggregated statistics."
            ),
        ),
    ]

    for tool in tools:
        mcp.add_tool(
            fn=tool.fn,
            name=tool.name,
            description=tool.description,
            annotations=tool.annotations,
        )

    # Version check (non-blocking, 5-second timeout enforced by version_check module)
    try:
        version_result = await check_version_async(
            "compliance-copilot", get_package_version()
        )

        if version_result.status == "blocked":
            _block_all_tools(mcp, version_result.message)
            logger.warning(
                "Version blocked: %s", version_result.message
            )
        elif version_result.status == "update_available":
            logger.info(version_result.message)
        elif version_result.status == "error":
            logger.warning(
                "Version check error, continuing normally: %s",
                version_result.message,
            )
    except Exception:
        logger.warning("Version check failed unexpectedly, continuing normally", exc_info=True)

    async with create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        await mcp.run_stdio_async()


def main():
    """Entry point for the MCP server."""
    run(run_server)
