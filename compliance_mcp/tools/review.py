"""Review Tool — run a structured compliance review via LLM.

Self-contained: fetches data via API, assembles prompt inline,
invokes LLM subprocess. No dependency on compliance_reviewer package.
"""

import json
import logging
import os
import subprocess
import time

from compliance_mcp.api_client import APIError, RecordNotFoundError, api_get
from compliance_mcp.config import MCPConfig

logger = logging.getLogger(__name__)
_config = MCPConfig()

_DEFAULT_PROMPT = """You are a compliance reviewer performing a structured 5-gate review.

The 5 gates are:
1. Completion Check — validate attestation has minimum required info
2. External Findings — cross-reference external scan results
3. Sensitive Data Validation — cross-check claims against evidence
4. Evidence of Controls — validate retention, deletion, and access controls
5. Compliance Onboarding — check tool registration and automation status

For each gate, list findings with severity (HIGH/MEDIUM/LOW) and recommended actions.
End with an actionable checklist of specific changes to make.
"""


def compliance_review(
    record_id: str,
    checklist_only: bool = False,
    prior_review_content: str = "",
) -> str:
    """Run a structured compliance review of a record.

    Args:
        record_id: Record identifier to review.
        checklist_only: If true, return only the actionable checklist.
        prior_review_content: Raw prior review text for delta analysis.

    Returns:
        Review report as markdown, or JSON error on failure.
    """
    if not record_id or not record_id.strip():
        return json.dumps(
            {"error": "InvalidParams", "message": "record_id must be a non-empty string."}
        )

    record_id = record_id.strip()

    # Step 1: Fetch attestation
    logger.info("Fetching attestation for '%s'...", record_id)
    try:
        attestation = api_get(
            _config.api_url, "/lookup",
            params={"record_id": record_id},
            tool_name="compliance_review",
        )
    except RecordNotFoundError:
        return json.dumps({
            "error": "RecordNotFound",
            "message": f"Record '{record_id}' not found in cache.",
        })
    except APIError as exc:
        return json.dumps({"error": "APIError", "message": str(exc)})

    attestation_json = json.dumps(attestation, indent=2, default=str)
    logger.info("Attestation fetched (%d bytes)", len(attestation_json))

    # Step 2: Load prompt
    prompt_file = os.environ.get("COMPLIANCE_PROMPT_FILE")
    if prompt_file:
        try:
            with open(prompt_file, encoding="utf-8") as f:
                prompt_template = f.read()
        except OSError as exc:
            return json.dumps({
                "error": "PromptFileError",
                "message": f"Cannot read prompt file {prompt_file}: {exc}",
            })
    else:
        prompt_template = _DEFAULT_PROMPT

    # Step 3: Assemble prompt
    mode_instruction = ""
    if checklist_only:
        mode_instruction = (
            "\n\nIMPORTANT: Output ONLY the actionable checklist. "
            "Do NOT include the full gate-by-gate review.\n"
        )

    prior_section = ""
    if prior_review_content:
        prior_section = (
            f"\n\n--- PRIOR REVIEW ---\n{prior_review_content}\n--- END PRIOR REVIEW ---\n"
            "Compare against the prior review and highlight what changed.\n"
        )

    prompt = (
        f"{prompt_template}\n\n"
        f"--- ATTESTATION DATA ---\n{attestation_json}\n--- END ATTESTATION DATA ---\n"
        f"{prior_section}{mode_instruction}"
    )

    # Step 4: Invoke LLM
    llm_command = os.environ.get("COMPLIANCE_LLM_COMMAND", "llm-cli")
    logger.info("Invoking %s (timeout=%ds)...", llm_command, _config.review_timeout)
    start = time.monotonic()

    try:
        result = subprocess.run(
            [llm_command, "chat", "--no-interactive"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=_config.review_timeout,
        )
    except FileNotFoundError:
        return json.dumps({
            "error": "LLMNotFound",
            "message": f"'{llm_command}' not found on PATH. Install it or set COMPLIANCE_LLM_COMMAND.",
        })
    except subprocess.TimeoutExpired:
        return json.dumps({
            "error": "Timeout",
            "message": f"LLM timed out after {_config.review_timeout}s.",
        })

    duration = time.monotonic() - start
    logger.info("LLM completed in %.1fs", duration)

    if result.returncode != 0:
        return json.dumps({
            "error": "LLMError",
            "message": f"LLM exited with code {result.returncode}: {result.stderr[:500]}",
        })

    return result.stdout
