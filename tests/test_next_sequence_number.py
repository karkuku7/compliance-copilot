"""Unit tests for _next_sequence_number() helper.

Tests the sequence-number scanning logic that prevents same-day report
overwrites.

Validates: Requirements 2.1, 2.2, 2.3
"""

import re

import pytest

from compliance_reviewer.cli import _next_sequence_number


# Base pattern used by most tests — matches "TestRecord_review_20250715_NNN.md"
BASE_PATTERN = (
    re.escape("TestRecord") + r"_review_" + re.escape("20250715") + r"_(\d+)\.md$"
)


class TestEmptyDirectory:
    """Empty directory → returns 1."""

    def test_returns_1_for_empty_dir(self, tmp_path):
        assert _next_sequence_number(str(tmp_path), BASE_PATTERN) == 1


class TestContiguousSequence:
    """Existing _001, _002 files → returns 3."""

    def test_returns_next_after_contiguous(self, tmp_path):
        (tmp_path / "TestRecord_review_20250715_001.md").write_text("r1")
        (tmp_path / "TestRecord_review_20250715_002.md").write_text("r2")
        assert _next_sequence_number(str(tmp_path), BASE_PATTERN) == 3


class TestNonContiguousSequence:
    """Non-contiguous _001, _005 → returns 6 (max + 1)."""

    def test_returns_max_plus_one(self, tmp_path):
        (tmp_path / "TestRecord_review_20250715_001.md").write_text("r1")
        (tmp_path / "TestRecord_review_20250715_005.md").write_text("r5")
        assert _next_sequence_number(str(tmp_path), BASE_PATTERN) == 6


class TestUnrelatedFiles:
    """Unrelated files in directory → returns 1."""

    def test_ignores_unrelated_files(self, tmp_path):
        (tmp_path / "README.md").write_text("readme")
        (tmp_path / "OtherRecord_review_20250715_003.md").write_text("other")
        (tmp_path / "TestRecord_review_20250714_001.md").write_text("diff date")
        (tmp_path / "notes.txt").write_text("notes")
        assert _next_sequence_number(str(tmp_path), BASE_PATTERN) == 1


class TestZeroPaddedSuffix:
    """Filename produces correct _NNN zero-padded suffix."""

    def test_seq_1_pads_to_001(self, tmp_path):
        seq = _next_sequence_number(str(tmp_path), BASE_PATTERN)
        filename = f"TestRecord_review_20250715_{seq:03d}.md"
        assert filename == "TestRecord_review_20250715_001.md"

    def test_seq_10_pads_to_010(self, tmp_path):
        for i in range(1, 10):
            (tmp_path / f"TestRecord_review_20250715_{i:03d}.md").write_text(f"r{i}")
        seq = _next_sequence_number(str(tmp_path), BASE_PATTERN)
        filename = f"TestRecord_review_20250715_{seq:03d}.md"
        assert filename == "TestRecord_review_20250715_010.md"


class TestNoArtificialCap:
    """999 existing files → returns 1000 (no artificial cap)."""

    def test_seq_after_999_is_1000(self, tmp_path):
        (tmp_path / "TestRecord_review_20250715_999.md").write_text("r999")
        assert _next_sequence_number(str(tmp_path), BASE_PATTERN) == 1000

    def test_filename_with_1000_has_four_digits(self, tmp_path):
        (tmp_path / "TestRecord_review_20250715_999.md").write_text("r999")
        seq = _next_sequence_number(str(tmp_path), BASE_PATTERN)
        filename = f"TestRecord_review_20250715_{seq:03d}.md"
        assert filename == "TestRecord_review_20250715_1000.md"


class TestNonexistentDirectory:
    """Non-existent directory → returns 1 (OSError handled gracefully)."""

    def test_returns_1_for_missing_dir(self, tmp_path):
        missing = str(tmp_path / "does_not_exist")
        assert _next_sequence_number(missing, BASE_PATTERN) == 1
