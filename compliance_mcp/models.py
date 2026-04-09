"""Data models for the MCP findings tool."""

from dataclasses import dataclass, field


@dataclass
class FindingItem:
    """A single finding/action item from a review report."""

    gate: int
    severity: str
    description: str
    table: str


@dataclass
class GateSummary:
    """Summary of findings for a single gate."""

    gate_number: int
    title: str
    finding_count: int
    tables: list[str] = field(default_factory=list)
    action_items: list[FindingItem] = field(default_factory=list)


@dataclass
class ParsedFindings:
    """Complete parsed findings from a review report."""

    total_findings: int
    gates: dict[int, GateSummary] = field(default_factory=dict)
    affected_tables: list[str] = field(default_factory=list)
    action_items: list[FindingItem] = field(default_factory=list)
