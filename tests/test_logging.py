"""Unit tests for whalu._logging."""

import logging

from whalu._logging import setup_logging


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_runs_without_error(self):
        setup_logging()

    def test_accepts_debug_level(self):
        setup_logging(level=logging.DEBUG)

    def test_accepts_info_level(self):
        setup_logging(level=logging.INFO)

    def test_suppresses_botocore(self):
        setup_logging()
        assert logging.getLogger("botocore").level == logging.WARNING

    def test_suppresses_boto3(self):
        setup_logging()
        assert logging.getLogger("boto3").level == logging.WARNING

    def test_suppresses_urllib3(self):
        setup_logging()
        assert logging.getLogger("urllib3").level == logging.WARNING

    def test_suppresses_tensorflow(self):
        setup_logging()
        assert logging.getLogger("tensorflow").level == logging.ERROR

    def test_suppresses_absl(self):
        setup_logging()
        assert logging.getLogger("absl").level == logging.ERROR
