"""MCP server configuration.

All settings are overridable via environment variables.
"""

import os
from dataclasses import dataclass, field


@dataclass
class MCPConfig:
    """Runtime configuration for the MCP server."""

    api_url: str = field(default_factory=lambda: "")
    review_timeout: int = field(default_factory=lambda: 0)
    verbose: bool = field(default_factory=lambda: False)

    def __post_init__(self) -> None:
        self.api_url = os.environ.get(
            "COMPLIANCE_API_URL", "http://localhost:8080"
        )
        self.review_timeout = int(
            os.environ.get("COMPLIANCE_REVIEW_TIMEOUT", "300")
        )
        self.verbose = os.environ.get("COMPLIANCE_MCP_VERBOSE", "false").lower() in (
            "true", "1", "yes",
        )
