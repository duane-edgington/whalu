"""Unit tests for whalu.cli.scan (parser and pure helpers)."""

import argparse

import pytest

from whalu.cli.scan import _month_range, _parse_ym, build_parser


class TestParseYm:
    """Tests for _parse_ym."""

    def test_valid_date(self):
        assert _parse_ym("2026-03") == (2026, 3)

    def test_january(self):
        assert _parse_ym("2023-01") == (2023, 1)

    def test_december(self):
        assert _parse_ym("2023-12") == (2023, 12)

    def test_invalid_format_raises(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_ym("2026/03")

    def test_missing_month_raises(self):
        with pytest.raises((argparse.ArgumentTypeError, ValueError)):
            _parse_ym("2026")

    def test_non_numeric_raises(self):
        with pytest.raises((argparse.ArgumentTypeError, ValueError)):
            _parse_ym("YYYY-MM")


class TestMonthRange:
    """Tests for _month_range."""

    def test_single_month(self):
        assert _month_range((2026, 3), (2026, 3)) == [(2026, 3)]

    def test_two_months(self):
        assert _month_range((2026, 3), (2026, 4)) == [(2026, 3), (2026, 4)]

    def test_crosses_year_boundary(self):
        result = _month_range((2025, 11), (2026, 2))
        assert result == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]

    def test_full_year(self):
        result = _month_range((2024, 1), (2024, 12))
        assert len(result) == 12
        assert result[0] == (2024, 1)
        assert result[-1] == (2024, 12)

    def test_end_before_start_returns_empty(self):
        assert _month_range((2026, 5), (2026, 3)) == []

    def test_multi_year_range(self):
        result = _month_range((2023, 7), (2024, 6))
        assert len(result) == 12
        assert result[0] == (2023, 7)
        assert result[-1] == (2024, 6)


class TestBuildParser:
    """Tests for build_parser and argument parsing."""

    def setup_method(self):
        self.parser = build_parser()

    def test_returns_argument_parser(self):
        assert isinstance(self.parser, argparse.ArgumentParser)

    def test_verbose_flag(self):
        args = self.parser.parse_args(["-v", "info"])
        assert args.verbose is True

    def test_verbose_default_false(self):
        args = self.parser.parse_args(["info"])
        assert args.verbose is False

    def test_scan_mbari_start_required(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["scan", "mbari"])

    def test_scan_mbari_with_start(self):
        args = self.parser.parse_args(["scan", "mbari", "--start", "2026-03"])
        assert args.start == "2026-03"
        assert args.source == "mbari"
        assert args.command == "scan"

    def test_scan_mbari_end_optional(self):
        args = self.parser.parse_args(["scan", "mbari", "--start", "2026-03"])
        assert args.end is None

    def test_scan_mbari_max_files(self):
        args = self.parser.parse_args(
            ["scan", "mbari", "--start", "2026-03", "--max-files", "2"]
        )
        assert args.max_files == 2

    def test_scan_mbari_limit_hours(self):
        args = self.parser.parse_args(
            ["scan", "mbari", "--start", "2026-03", "--limit-hours", "1.5"]
        )
        assert args.limit_hours == 1.5

    def test_scan_mbari_default_output_dir(self):
        args = self.parser.parse_args(["scan", "mbari", "--start", "2026-03"])
        assert "mbari" in args.output_dir

    def test_scan_orcasound_defaults(self):
        args = self.parser.parse_args(["scan", "orcasound"])
        assert args.source == "orcasound"
        assert args.key is None

    def test_scan_orcasound_key(self):
        args = self.parser.parse_args(["scan", "orcasound", "--key", "my/key.wav"])
        assert args.key == "my/key.wav"

    def test_analyze_defaults(self):
        args = self.parser.parse_args(["analyze"])
        assert args.command == "analyze"
        assert args.top_n == 5

    def test_analyze_top_n(self):
        args = self.parser.parse_args(["analyze", "--top-n", "10"])
        assert args.top_n == 10

    def test_analyze_input_dir(self):
        args = self.parser.parse_args(["analyze", "--input-dir", "/tmp/detections"])
        assert args.input_dir == "/tmp/detections"

    def test_info_no_source(self):
        args = self.parser.parse_args(["info"])
        assert args.command == "info"
        assert args.info_source is None

    def test_info_mbari(self):
        args = self.parser.parse_args(["info", "mbari"])
        assert args.info_source == "mbari"

    def test_info_orcasound(self):
        args = self.parser.parse_args(["info", "orcasound"])
        assert args.info_source == "orcasound"

    def test_info_invalid_source(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["info", "invalid_source"])

    def test_no_command_sets_command_none(self):
        args = self.parser.parse_args([])
        assert args.command is None
