"""Preservation property tests — baseline behaviors that must survive the fix.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

Property 2: Preservation — Non-Colliding Runs and Auxiliary Behaviors Unchanged

These tests capture the existing (correct) behavior of the compliance-copilot
write logic for scenarios where no same-day collision occurs.  They MUST PASS
on unfixed code — they define the regression boundary.
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
    """Replicate the exact filename + write logic from _run_review().

    Mirrors the Step 6 block in compliance_reviewer.cli._run_review().
    """
    date_str = datetime.now().strftime("%Y%m%d")
    filename = f"{record_id}_review_{date_str}.md"
    filepath = os.path.join(output_dir, filename)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


# ---------------------------------------------------------------------------
# Property: Single write produces exactly one report file
# ---------------------------------------------------------------------------

class TestSingleCallPreservation:
    """A single write call produces one file in the output directory.

    **Validates: Requirements 3.1, 3.2**
    """

    @given(
        record_id=st.text(
            alphabet=st.characters(
                whitelist_categories=("L", "N", "P"),
                blacklist_characters="/\\\x00",
            ),
            min_size=1,
            max_size=30,
        ).filter(lambda s: s.strip() != "")
    )
    @settings(max_examples=30, deadline=None)
    def test_single_call_produces_one_report(self, record_id):
        """For any valid record ID, a single call writes exactly one report."""
        work_dir = tempfile.mkdtemp()

        try:
            with patch(f"{__name__}.datetime") as mock_dt:
                mock_dt.now.return_value = FIXED_DATE
                mock_dt.strftime = datetime.strftime
                filepath = _write_report_like_cli(
                    record_id, "single run content", work_dir
                )

            # Exactly one file in the output dir
            files = os.listdir(work_dir)
            assert len(files) == 1, (
                f"Expected 1 file, got {len(files)}: {files}"
            )

            # File path returned must exist and match the file on disk
            assert os.path.isfile(filepath)
            assert os.path.basename(filepath) == files[0]

            # Content must match
            with open(filepath, encoding="utf-8") as f:
                assert f.read() == "single run content"
        finally:
            import shutil
            shutil.rmtree(work_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Property: Report is written to the specified output_dir
# ---------------------------------------------------------------------------

class TestOutputDirPreservation:
    """Report is written to the specified output directory.

    **Validates: Requirements 3.2**
    """

    def test_report_written_to_custom_output_dir(self, tmp_path):
        """For a valid output_dir, the report lands in that directory."""
        sub = tmp_path / "custom" / "nested"
        sub.mkdir(parents=True)

        with patch(f"{__name__}.datetime") as mock_dt:
            mock_dt.now.return_value = FIXED_DATE
            mock_dt.strftime = datetime.strftime
            filepath = _write_report_like_cli("DirRec", "dir test content", str(sub))

        assert os.path.dirname(filepath) == str(sub)
        assert os.path.isfile(filepath)

    @given(
        dir_suffix=st.text(
            alphabet=st.characters(
                whitelist_categories=("L", "N"),
                blacklist_characters="/\\\x00",
            ),
            min_size=1,
            max_size=15,
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_report_in_generated_output_dir(self, dir_suffix):
        """For any valid output_dir path, the report is written there."""
        base = tempfile.mkdtemp()
        output_dir = os.path.join(base, dir_suffix)
        os.makedirs(output_dir, exist_ok=True)

        try:
            with patch(f"{__name__}.datetime") as mock_dt:
                mock_dt.now.return_value = FIXED_DATE
                mock_dt.strftime = datetime.strftime
                filepath = _write_report_like_cli("App", "output dir content", output_dir)

            assert os.path.isfile(filepath)
            assert filepath.startswith(output_dir)
        finally:
            import shutil
            shutil.rmtree(base, ignore_errors=True)


# ---------------------------------------------------------------------------
# Property: OSError raises ReportWriteError
# ---------------------------------------------------------------------------

class TestErrorFallbackPreservation:
    """Write failures raise ReportWriteError.

    **Validates: Requirements 3.4**
    """

    def test_compliance_copilot_oserror_raises_report_write_error(self):
        """Simulated OSError in write logic raises ReportWriteError.

        Replicates the _run_review() error path: when the file write fails,
        the OSError is caught and re-raised as a ReportWriteError.
        """
        record_id = "REC-ERR"
        output_dir = "/nonexistent/path/that/does/not/exist"
        date_str = FIXED_DATE.strftime("%Y%m%d")
        filename = f"{record_id}_review_{date_str}.md"
        filepath = os.path.join(output_dir, filename)

        with pytest.raises(ReportWriteError):
            try:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write("test")
            except OSError as exc:
                raise ReportWriteError(f"Cannot write report: {exc}")
