"""Compliance Copilot MCP Server — main entry point.

Exposes four tools: compliance_lookup, compliance_review,
compliance_findings, compliance_stats.

Uses FastMCP with stdio transport.
"""

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


async def signal_handler(scope: CancelScope):
    """Handle SIGINT and SIGTERM signals."""
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for _ in signals:
            print("Shutting down MCP server...")
            os._exit(0)


async def run_server():
    """Run the MCP server with signal handling."""
    mcp = FastMCP(
        name="ComplianceCopilot",
        instructions=(
            "Compliance Copilot MCP server. Provides compliance record lookup, "
            "automated structured reviews, findings interpretation, and cache statistics."
        ),
    )

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
                "Returns counts, record lists, usage metrics, or cache health."
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

    async with create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        await mcp.run_stdio_async()


def main():
    """Entry point for the MCP server."""
    run(run_server)
