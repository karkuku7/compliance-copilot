"""CLI entry point for the compliance reviewer.

Orchestrates: parse args → fetch attestation → assemble prompt → invoke LLM → write report.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from compliance_reviewer.errors import (
    DataRetrievalError,
    LLMInvocationError,
    PromptFileError,
    RecordNotFoundError,
    ReportWriteError,
    ReviewerError,
)

logger = logging.getLogger(__name__)

DEFAULT_API_URL = os.environ.get(
    "COMPLIANCE_API_URL", "http://localhost:8080"
)
DEFAULT_PROMPT_FILE = os.path.join(
    os.path.dirname(__file__), "..", "prompts", "default_review.md"
)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compliance-review",
        description="LLM-powered compliance attestation review",
    )
    parser.add_argument(
        "--record", "--app", required=True, dest="record_id",
        help="Compliance record ID to review",
    )
    parser.add_argument(
        "--prior-review", "--pr", default=None, dest="prior_review",
        help="Path to a prior review file for delta analysis (.md, .txt)",
    )
    parser.add_argument(
        "--prompt-file", "--prompt", default=None, dest="prompt_file",
        help="Path to a custom review prompt file",
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory for the report (default: current directory)",
    )
    parser.add_argument(
        "--checklist-only", "--cl", action="store_true", default=False,
        dest="checklist_only",
        help="Output only the actionable checklist (not the full review)",
    )
    parser.add_argument(
        "--llm-command", default="llm-cli",
        help="LLM CLI command to use (default: llm-cli)",
    )
    parser.add_argument(
        "--llm-timeout", type=int, default=300,
        help="LLM invocation timeout in seconds (default: 300)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", default=False,
        help="Enable debug logging",
    )
    return parser


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        _run_review(args)
    except ReviewerError as exc:
        logger.error("Review failed: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)


def _run_review(args: argparse.Namespace) -> None:
    """Execute the full review pipeline."""
    record_id = args.record_id.strip()

    # Step 1: Fetch attestation from cache API
    logger.info("Fetching attestation for '%s'...", record_id)
    attestation = _fetch_attestation(record_id)
    attestation_json = json.dumps(attestation, indent=2, default=str)
    logger.info("Attestation fetched (%d bytes)", len(attestation_json))

    # Step 2: Load prompt
    prompt_file = args.prompt_file or DEFAULT_PROMPT_FILE
    logger.info("Loading prompt from %s", prompt_file)
    try:
        with open(prompt_file, encoding="utf-8") as f:
            prompt_template = f.read()
    except OSError as exc:
        raise PromptFileError(f"Cannot read prompt file {prompt_file}: {exc}")

    # Step 3: Load prior review (optional)
    prior_content = ""
    if args.prior_review:
        logger.info("Loading prior review from %s", args.prior_review)
        try:
            with open(args.prior_review, encoding="utf-8") as f:
                prior_content = f.read()
        except OSError as exc:
            logger.warning("Could not load prior review: %s", exc)

    # Step 4: Assemble prompt
    prompt = _assemble_prompt(
        prompt_template, attestation_json, prior_content, args.checklist_only
    )
    logger.info("Prompt assembled (%d bytes)", len(prompt))

    # Step 5: Invoke LLM
    logger.info("Invoking LLM (timeout=%ds)...", args.llm_timeout)
    start = time.monotonic()
    review_output = _invoke_llm(prompt, args.llm_command, args.llm_timeout)
    duration = time.monotonic() - start
    logger.info("LLM completed in %.1fs", duration)

    # Step 6: Write report
    output_dir = args.output_dir or os.getcwd()
    date_str = datetime.now().strftime("%Y%m%d")
    filename = f"{record_id}_review_{date_str}.md"
    filepath = os.path.join(output_dir, filename)

    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(review_output)
        logger.info("Report written to %s", filepath)
        print(f"Review saved: {filepath}")
    except OSError as exc:
        raise ReportWriteError(f"Cannot write report: {exc}")


def _fetch_attestation(record_id: str) -> dict:
    """Fetch attestation data from the cache API."""
    url = f"{DEFAULT_API_URL}/lookup?record_id={record_id}"
    req = urllib.request.Request(url)
    req.add_header("X-Source", "cli")
    req.add_header("X-Tool", "compliance-review")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RecordNotFoundError(f"Record '{record_id}' not found in cache")
        raise DataRetrievalError(f"API returned HTTP {exc.code}")
    except urllib.error.URLError as exc:
        raise DataRetrievalError(f"Cannot reach API: {exc.reason}")


def _assemble_prompt(
    template: str,
    attestation_json: str,
    prior_review: str,
    checklist_only: bool,
) -> str:
    """Assemble the final prompt for the LLM."""
    parts = [template]

    parts.append(f"\n\n--- ATTESTATION DATA ---\n{attestation_json}\n--- END ATTESTATION DATA ---\n")

    if prior_review:
        parts.append(
            f"\n--- PRIOR REVIEW ---\n{prior_review}\n--- END PRIOR REVIEW ---\n"
            "Compare this attestation against the prior review and highlight what changed.\n"
        )

    if checklist_only:
        parts.append(
            "\nIMPORTANT: Output ONLY the actionable checklist. "
            "Do NOT include the full gate-by-gate review.\n"
        )

    return "".join(parts)


def _invoke_llm(prompt: str, command: str, timeout: int) -> str:
    """Invoke the LLM via subprocess."""
    try:
        result = subprocess.run(
            [command, "chat", "--no-interactive"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        raise LLMInvocationError(
            f"'{command}' not found on PATH. Install it or set --llm-command."
        )
    except subprocess.TimeoutExpired:
        raise LLMInvocationError(f"LLM timed out after {timeout}s")

    if result.returncode != 0:
        raise LLMInvocationError(
            f"LLM exited with code {result.returncode}: {result.stderr[:500]}"
        )

    return result.stdout


if __name__ == "__main__":
    main()
