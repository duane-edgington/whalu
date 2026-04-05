"""Unit tests for whalu.models.loader."""

from unittest.mock import MagicMock, patch

import whalu.models.loader as loader_module
from whalu.models.loader import get_whale_model


class TestGetWhaleModel:
    """Tests for get_whale_model."""

    def setup_method(self):
        # Reset singleton before each test
        loader_module._whale_model = None

    def test_returns_model(self):
        mock_model = MagicMock()
        with patch("whalu.models.loader.load_model_by_name", return_value=mock_model):
            result = get_whale_model()
        assert result is mock_model

    def test_calls_load_with_correct_name(self):
        mock_model = MagicMock()
        with patch("whalu.models.loader.load_model_by_name", return_value=mock_model) as mock_load:
            get_whale_model()
        mock_load.assert_called_once_with("multispecies_whale")

    def test_caches_model_on_second_call(self):
        mock_model = MagicMock()
        with patch("whalu.models.loader.load_model_by_name", return_value=mock_model) as mock_load:
            result1 = get_whale_model()
            result2 = get_whale_model()
        assert mock_load.call_count == 1
        assert result1 is result2

    def test_returns_same_instance_across_calls(self):
        mock_model = MagicMock()
        with patch("whalu.models.loader.load_model_by_name", return_value=mock_model):
            assert get_whale_model() is get_whale_model()
