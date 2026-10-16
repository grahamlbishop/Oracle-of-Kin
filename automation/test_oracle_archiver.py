"""Tests for oracle_archiver.py"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from oracle_archiver import OracleArchiver


class MarkdownDateTests(unittest.TestCase):
    """Date extraction from markdown sessions"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.archiver = OracleArchiver(base_path=self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, name: str, content: str) -> Path:
        path = self.tmp / name
        path.write_text(content, encoding='utf-8')
        return path

    def test_markdown_without_date_falls_back(self):
        path = self._write("no_date.md", "🕯️ **Oracle of Kin Ritual**\n\nNo date here.\n")
        metadata, _ = self.archiver.process_file(path)
        self.assertEqual(metadata.date, 'unknown_date')
        self.assertTrue(metadata.session_id.startswith('unknown_date_'))

    def test_markdown_with_date_is_extracted(self):
        path = self._write("dated.md", "**Date:** 2025-07-06  \n")
        metadata, _ = self.archiver.process_file(path)
        self.assertEqual(metadata.date, '2025-07-06')


if __name__ == "__main__":
    unittest.main()
