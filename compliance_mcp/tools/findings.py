"""Findings Tool — parse and filter review findings by gate or table."""

import json
import re
from dataclasses import asdict

from compliance_mcp.models import FindingItem, GateSummary, ParsedFindings

_GATE_HEADING_RE = re.compile(
    r"^#{2,3}\s+[Gg][Aa][Tt][Ee]\s+(\d)\s*[:\-—]?\s*(.*)", re.MULTILINE
)
_TABLE_HEADING_RE = re.compile(
    r"^#{2,3}\s+(?:Data\s+Store|Table)\s*[:\-—]\s*(.*)", re.MULTILINE | re.IGNORECASE
)
_TABLE_INLINE_RE = re.compile(
    r"(?:[Ii]n|[Ff]or|[Tt]able|[Dd]ata\s+[Ss]tore)\s+[:\-]?\s*[`\"']?([A-Za-z][A-Za-z0-9_\-]+)[`\"']?"
)
_ACTION_ITEM_RE = re.compile(
    r"^\s*\d+[\.\)]\s*(.*?\[(?:HIGH|MEDIUM|LOW)\].*)", re.MULTILINE | re.IGNORECASE
)
_SEVERITY_RE = re.compile(r"\[(HIGH|MEDIUM|LOW)\]", re.IGNORECASE)
_CHECKLIST_RE = re.compile(r"^\s*(?:\d+[\.\)]|-)\s*\[([xX ])\]\s*(.*)", re.MULTILINE)

GATE_TITLES = {
    1: "Completion Check",
    2: "External Findings",
    3: "Sensitive Data Validation",
    4: "Evidence of Controls",
    5: "Compliance Onboarding",
}

_NOISE_WORDS = {"the", "this", "that", "and", "for", "not", "with", "from"}


def _split_by_gates(content: str) -> dict[int, str]:
    matches = list(_GATE_HEADING_RE.finditer(content))
    if not matches:
        return {}
    sections: dict[int, str] = {}
    for i, match in enumerate(matches):
        gate_num = int(match.group(1))
        if gate_num < 1 or gate_num > 5:
            continue
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        sections[gate_num] = content[start:end]
    return sections


def _extract_tables(text: str) -> list[str]:
    tables: list[str] = []
    for m in _TABLE_HEADING_RE.finditer(text):
        name = m.group(1).strip().strip("`\"'")
        if name and name not in tables:
            tables.append(name)
    for m in _TABLE_INLINE_RE.finditer(text):
        name = m.group(1).strip().strip("`\"'")
        if name and len(name) > 2 and name.lower() not in _NOISE_WORDS and name not in tables:
            tables.append(name)
    return tables


def _find_table_context(text: str, pos: int) -> str:
    headings = list(_TABLE_HEADING_RE.finditer(text[:pos]))
    return headings[-1].group(1).strip().strip("`\"'") if headings else ""


def _extract_action_items(text: str, gate: int) -> list[FindingItem]:
    items = []
    for m in _ACTION_ITEM_RE.finditer(text):
        line = m.group(1).strip()
        sev = _SEVERITY_RE.search(line)
        severity = sev.group(1).upper() if sev else ""
        desc = _SEVERITY_RE.sub("", line).strip().strip("- :")
        items.append(FindingItem(
            gate=gate, severity=severity, description=desc,
            table=_find_table_context(text, m.start()),
        ))
    return items


def _extract_checklist_items(text: str, gate: int) -> list[FindingItem]:
    items = []
    for m in _CHECKLIST_RE.finditer(text):
        if m.group(1).lower() == "x":
            continue
        desc = m.group(2).strip()
        if not desc:
            continue
        sev = _SEVERITY_RE.search(desc)
        severity = sev.group(1).upper() if sev else ""
        if severity:
            desc = _SEVERITY_RE.sub("", desc).strip().strip("- :")
        items.append(FindingItem(
            gate=gate, severity=severity, description=desc,
            table=_find_table_context(text, m.start()),
        ))
    return items


def _parse_review(content: str) -> ParsedFindings:
    gate_sections = _split_by_gates(content)
    all_items: list[FindingItem] = []
    all_tables: list[str] = []
    gates: dict[int, GateSummary] = {}

    for g in range(1, 6):
        text = gate_sections.get(g, "")
        tables = _extract_tables(text) if text else []
        items = (_extract_action_items(text, g) + _extract_checklist_items(text, g)) if text else []
        gates[g] = GateSummary(
            gate_number=g, title=GATE_TITLES.get(g, f"Gate {g}"),
            finding_count=len(items), tables=tables, action_items=items,
        )
        all_items.extend(items)
        for t in tables:
            if t not in all_tables:
                all_tables.append(t)

    return ParsedFindings(
        total_findings=len(all_items), gates=gates,
        affected_tables=all_tables, action_items=all_items,
    )


def compliance_findings(
    review_content: str,
    gate: int | None = None,
    table_name: str | None = None,
) -> str:
    """Parse and summarize review findings. Filter by gate (1-5) or table name.

    Args:
        review_content: Review report markdown content.
        gate: Filter to specific gate (1-5).
        table_name: Filter to specific data store/table.

    Returns:
        JSON string with structured findings.
    """
    if not review_content or not review_content.strip():
        return json.dumps(
            {"error": "InvalidParams", "message": "review_content must be non-empty."}
        )

    if gate is not None:
        try:
            gate = int(gate)
        except (TypeError, ValueError):
            return json.dumps(
                {"error": "InvalidParams", "message": f"Gate must be 1-5. Got: {gate}"}
            )
        if gate < 1 or gate > 5:
            return json.dumps(
                {"error": "InvalidParams", "message": f"Gate must be 1-5. Got: {gate}"}
            )

    findings = _parse_review(review_content)

    # Apply filters
    if gate is not None or table_name:
        filtered_gates: dict[int, GateSummary] = {}
        filtered_items: list[FindingItem] = []
        filtered_tables: list[str] = []

        for g in [gate] if gate else range(1, 6):
            s = findings.gates.get(g)
            if not s:
                continue
            items = (
                [i for i in s.action_items if i.table.lower() == table_name.lower()]
                if table_name else list(s.action_items)
            )
            tables = (
                [t for t in s.tables if t.lower() == table_name.lower()]
                if table_name else list(s.tables)
            )
            filtered_gates[g] = GateSummary(
                gate_number=s.gate_number, title=s.title,
                finding_count=len(items), tables=tables, action_items=items,
            )
            filtered_items.extend(items)
            for t in tables:
                if t not in filtered_tables:
                    filtered_tables.append(t)

        findings = ParsedFindings(
            total_findings=len(filtered_items), gates=filtered_gates,
            affected_tables=filtered_tables, action_items=filtered_items,
        )

    gates_dict = {}
    for gn, s in findings.gates.items():
        gates_dict[str(gn)] = {
            "title": s.title, "finding_count": s.finding_count,
            "tables": s.tables, "action_items": [asdict(i) for i in s.action_items],
        }

    return json.dumps({
        "total_findings": findings.total_findings,
        "gates": gates_dict,
        "affected_tables": findings.affected_tables,
        "action_items": [asdict(i) for i in findings.action_items],
    }, indent=2)
