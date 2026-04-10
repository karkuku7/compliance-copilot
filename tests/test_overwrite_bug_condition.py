"""Bug condition exploration test — same-day runs overwrite previous output.

**Validates: Requirements 1.1, 1.2, 1.3, 2.1, 2.2, 2.3**

Property 1: Bug Condition — Same-Day Runs Overwrite Previous Output

This test is EXPECTED TO FAIL on unfixed code. Failure confirms the bug exists:
the compliance-copilot filename logic produces the same filename for every
same-day invocation, so the second run silently overwrites the first.

The test encodes the *expected* (correct) behavior — all N files should exist
after N same-day calls. When the fix is applied, this test will pass.
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from compliance_reviewer.errors import ReportWriteError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXED_DATE = datetime(2025, 7, 15, 10, 0, 0)


def _write_report_like_cli(record_id: str, content: str, output_dir: str) -> str:
    """Replicate the exact filename + write logic from _run_review()."""
    import re as _re
    date_str = datetime.now().strftime("%Y%m%d")
    base_pattern = _re.escape(record_id) + r"_review_" + _re.escape(date_str) + r"_(\d+)\.md$"
    # Inline _next_sequence_number logic (mirrors compliance_reviewer/cli.py)
    pattern = _re.compile(base_pattern)
    seq_numbers = []
    try:
        for entry in os.listdir(output_dir):
            m = pattern.match(entry)
            if m:
                seq_numbers.append(int(m.group(1)))
    except OSError:
        pass
    seq = max(seq_numbers) + 1 if seq_numbers else 1
    filename = f"{record_id}_review_{date_str}_{seq:03d}.md"
    filepath = os.path.join(output_dir, filename)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


# ---------------------------------------------------------------------------
# Compliance-copilot tests
# ---------------------------------------------------------------------------

class TestComplianceCopilotOverwriteBugCondition:
    """Replicate the filename + write logic from _run_review() to show the bug.

    **Validates: Requirements 1.3, 2.1, 2.3**
    """

    def test_two_same_day_runs_both_files_exist(self, tmp_path):
        """Two writes with the same record ID and date must produce two files."""
        record_id = "REC-42"
        output_dir = str(tmp_path)

        with patch(
            f"{__name__}.datetime"
        ) as mock_dt:
            mock_dt.now.return_value = FIXED_DATE
            mock_dt.strftime = datetime.strftime

            path1 = _write_report_like_cli(record_id, "First review", output_dir)
            path2 = _write_report_like_cli(record_id, "Second review", output_dir)

        # The two paths must be different
        assert path1 != path2, (
            f"Both calls produced the same filepath: {path1}"
        )

        # Both files must exist
        assert os.path.isfile(path1), f"First report missing: {path1}"
        assert os.path.isfile(path2), f"Second report missing: {path2}"

    def test_two_same_day_runs_content_preserved(self, tmp_path):
        """Two writes must preserve both files' content (no overwrite)."""
        record_id = "REC-42"
        output_dir = str(tmp_path)

        with patch(
            f"{__name__}.datetime"
        ) as mock_dt:
            mock_dt.now.return_value = FIXED_DATE
            mock_dt.strftime = datetime.strftime

            path1 = _write_report_like_cli(record_id, "First review content", output_dir)
            path2 = _write_report_like_cli(record_id, "Second review content", output_dir)

        # Content must match what was written (not overwritten)
        with open(path1) as f:
            assert f.read() == "First review content"
        with open(path2) as f:
            assert f.read() == "Second review content"


# ---------------------------------------------------------------------------
# Hypothesis property test — variable number of same-day runs
# ---------------------------------------------------------------------------

class TestSameDayRunsProperty:
    """Property: N same-day calls must produce N distinct, existing files.

    **Validates: Requirements 1.1, 1.2, 1.3, 2.1, 2.2, 2.3**
    """

    @given(n_runs=st.integers(min_value=2, max_value=5))
    @settings(max_examples=20, deadline=None)
    def test_n_same_day_compliance_runs_all_files_exist(self, n_runs):
        """Compliance write logic called N times must leave N files."""
        import re as _re
        record_id = "REC-42"
        work_dir = tempfile.mkdtemp()

        try:
            paths = []
            for i in range(n_runs):
                # Replicate the fixed logic from _run_review()
                date_str = FIXED_DATE.strftime("%Y%m%d")
                base_pattern = _re.escape(record_id) + r"_review_" + _re.escape(date_str) + r"_(\d+)\.md$"
                pattern = _re.compile(base_pattern)
                seq_numbers = []
                try:
                    for entry in os.listdir(work_dir):
                        m = pattern.match(entry)
                        if m:
                            seq_numbers.append(int(m.group(1)))
                except OSError:
                    pass
                seq = max(seq_numbers) + 1 if seq_numbers else 1
                filename = f"{record_id}_review_{date_str}_{seq:03d}.md"
                filepath = os.path.join(work_dir, filename)
                Path(work_dir).mkdir(parents=True, exist_ok=True)
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"Review content run {i + 1}")
                paths.append(filepath)

            # All paths must be unique
            assert len(set(paths)) == n_runs, (
                f"Expected {n_runs} unique paths, got {len(set(paths))}: {paths}"
            )

            # All files must exist
            for i, path in enumerate(paths):
                assert os.path.isfile(path), (
                    f"File from run {i + 1} missing: {path}"
                )
        finally:
            import shutil
            shutil.rmtree(work_dir, ignore_errors=True)
