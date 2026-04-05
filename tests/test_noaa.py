"""Unit tests for whalu.data.noaa.

GCS I/O is fully mocked so no network access is required.
"""

from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from whalu.data.noaa import (
    BUCKET,
    NRS_SITES,
    SANCTSOUND_SITES,
    download_audio,
    list_deployments,
    list_files,
    parse_timestamp,
    stream_chunks,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_bucket_name(self):
        assert BUCKET == "noaa-passive-bioacoustic"

    def test_nrs_has_twelve_stations(self):
        assert len(NRS_SITES) == 12

    def test_nrs_keys_are_zero_padded(self):
        for key in NRS_SITES:
            assert len(key) == 2
            assert key.isdigit()

    def test_sanctsound_known_sites(self):
        for code in ("mb", "hi", "sb", "ci", "fk", "oc", "gr", "pm"):
            assert code in SANCTSOUND_SITES


# ---------------------------------------------------------------------------
# parse_timestamp
# ---------------------------------------------------------------------------


class TestParseTimestamp:
    @pytest.mark.parametrize(
        "blob_name, expected",
        [
            # NRS full path
            (
                "nrs/audio/01/nrs_01_2014-2015/audio/NRS01_20141014_234015.flac",
                "20141014T234015",
            ),
            # NRS filename only
            ("NRS09_20230601_120000.flac", "20230601T120000"),
            # SanctSound with Z suffix
            (
                "sanctsound/audio/mb01/sanctsound_mb01_01/audio/"
                "SanctSound_MB01_01_671399971_20181115T000002Z.flac",
                "20181115T000002",
            ),
            # SanctSound without Z
            (
                "SanctSound_HI05_01_123456_20190301T060000.flac",
                "20190301T060000",
            ),
        ],
    )
    def test_known_formats(self, blob_name: str, expected: str):
        assert parse_timestamp(blob_name) == expected

    def test_unknown_format_returns_none(self):
        assert parse_timestamp("unknown_file_20230101.flac") is None

    def test_empty_string_returns_none(self):
        assert parse_timestamp("") is None

    def test_uses_basename_only(self):
        # Ensures directory components do not confuse the regex
        result = parse_timestamp("a/b/c/NRS03_20220715_083000.flac")
        assert result == "20220715T083000"


# ---------------------------------------------------------------------------
# list_deployments
# ---------------------------------------------------------------------------


def _make_mock_iterator(prefixes: list[str]):
    """Return a mock iterator whose .prefixes attribute is set after iteration."""
    it = MagicMock()
    it.prefixes = prefixes
    it.__iter__ = MagicMock(return_value=iter([]))
    return it


class TestListDeployments:
    @patch("whalu.data.noaa._gcs")
    def test_returns_sorted_deployment_names(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        mock_client.list_blobs.return_value = _make_mock_iterator(
            [
                "nrs/audio/01/nrs_01_2016-2017/",
                "nrs/audio/01/nrs_01_2014-2015/",
            ]
        )

        result = list_deployments("nrs", "01")

        assert result == ["nrs_01_2014-2015", "nrs_01_2016-2017"]

    @patch("whalu.data.noaa._gcs")
    def test_queries_correct_prefix(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        mock_client.list_blobs.return_value = _make_mock_iterator([])

        list_deployments("sanctsound", "mb01")

        _, kwargs = mock_client.list_blobs.call_args
        assert kwargs["prefix"] == "sanctsound/audio/mb01/"

    @patch("whalu.data.noaa._gcs")
    def test_empty_when_no_deployments(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        mock_client.list_blobs.return_value = _make_mock_iterator([])

        assert list_deployments("nrs", "99") == []


# ---------------------------------------------------------------------------
# list_files
# ---------------------------------------------------------------------------


def _make_blob(name: str) -> MagicMock:
    b = MagicMock()
    b.name = name
    return b


class TestListFiles:
    @patch("whalu.data.noaa._gcs")
    def test_returns_sorted_flac_names(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        blobs = [
            _make_blob(
                "nrs/audio/01/nrs_01_2014-2015/audio/NRS01_20141015_000000.flac"
            ),
            _make_blob(
                "nrs/audio/01/nrs_01_2014-2015/audio/NRS01_20141014_234015.flac"
            ),
        ]
        mock_client.list_blobs.return_value = iter(blobs)

        result = list_files("nrs", "01", "nrs_01_2014-2015")

        assert result == [
            "nrs/audio/01/nrs_01_2014-2015/audio/NRS01_20141014_234015.flac",
            "nrs/audio/01/nrs_01_2014-2015/audio/NRS01_20141015_000000.flac",
        ]

    @patch("whalu.data.noaa._gcs")
    def test_filters_out_non_flac(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        blobs = [
            _make_blob("nrs/audio/01/dep/audio/file.flac"),
            _make_blob("nrs/audio/01/dep/audio/file.txt"),
            _make_blob("nrs/audio/01/dep/metadata/meta.json"),
        ]
        mock_client.list_blobs.return_value = iter(blobs)

        result = list_files("nrs", "01", "dep")

        assert all(n.endswith(".flac") for n in result)
        assert len(result) == 1

    @patch("whalu.data.noaa._gcs")
    def test_queries_correct_prefix(self, mock_gcs):
        mock_client = MagicMock()
        mock_gcs.return_value = mock_client
        mock_client.list_blobs.return_value = iter([])

        list_files("sanctsound", "mb01", "sanctsound_mb01_01")

        _, kwargs = mock_client.list_blobs.call_args
        assert kwargs["prefix"] == "sanctsound/audio/mb01/sanctsound_mb01_01/audio/"


# ---------------------------------------------------------------------------
# download_audio
# ---------------------------------------------------------------------------


class TestDownloadAudio:
    """download_audio: GCS blob download and librosa.load are both mocked.

    This avoids importing audioread (which emits DeprecationWarnings for
    stdlib modules removed in Python 3.13) and keeps tests fast and pure.
    """

    def _make_mocks(self, audio: np.ndarray, sr: int):
        """Return (mock_gcs_patcher, mock_librosa_patcher) context managers."""
        blob = MagicMock()
        blob.size = 512
        blob.download_to_filename = MagicMock()

        mock_client = MagicMock()
        mock_client.bucket.return_value.blob.return_value = blob

        librosa_return = (audio, sr)
        return mock_client, librosa_return

    @patch("whalu.data.noaa.librosa.load")
    @patch("whalu.data.noaa._gcs")
    def test_returns_float32_array(self, mock_gcs, mock_load):
        sr = 5000
        audio = np.zeros(sr * 4, dtype=np.float32)
        mock_client, lr = self._make_mocks(audio, sr)
        mock_gcs.return_value = mock_client
        mock_load.return_value = lr

        result, dur = download_audio("nrs/audio/01/dep/audio/file.flac", target_sr=sr)

        assert result.dtype == np.float32
        assert dur == pytest.approx(4.0, abs=0.01)

    @patch("whalu.data.noaa.librosa.load")
    @patch("whalu.data.noaa._gcs")
    def test_limit_s_truncates_audio(self, mock_gcs, mock_load):
        sr = 5000
        audio = np.zeros(sr * 10, dtype=np.float32)
        mock_client, lr = self._make_mocks(audio, sr)
        mock_gcs.return_value = mock_client
        mock_load.return_value = lr

        result, dur = download_audio(
            "nrs/audio/01/dep/audio/file.flac", target_sr=sr, limit_s=3.0
        )

        assert dur == pytest.approx(3.0, abs=0.01)
        assert len(result) == sr * 3

    @patch("whalu.data.noaa.librosa.load")
    @patch("whalu.data.noaa._gcs")
    def test_tempfile_cleaned_up(self, mock_gcs, mock_load):
        sr = 5000
        audio = np.zeros(sr, dtype=np.float32)
        mock_client, lr = self._make_mocks(audio, sr)
        mock_gcs.return_value = mock_client
        mock_load.return_value = lr

        tmp_before = set(os.listdir(tempfile.gettempdir()))
        download_audio("blob.flac", target_sr=sr)
        tmp_after = set(os.listdir(tempfile.gettempdir()))

        new_files = tmp_after - tmp_before
        assert not any(f.endswith(".flac") for f in new_files)


# ---------------------------------------------------------------------------
# stream_chunks
# ---------------------------------------------------------------------------


class TestStreamChunks:
    @patch("whalu.data.noaa.download_audio")
    def test_yields_correct_number_of_chunks(self, mock_dl):
        sr = 5000
        total_s = 7.0
        mock_dl.return_value = (np.zeros(int(total_s * sr), dtype=np.float32), total_s)

        chunks = list(stream_chunks("blob.flac", target_sr=sr, chunk_s=3.0))

        assert len(chunks) == 3  # 3s, 3s, 1s

    @patch("whalu.data.noaa.download_audio")
    def test_chunk_start_offsets_are_cumulative(self, mock_dl):
        sr = 5000
        mock_dl.return_value = (np.zeros(sr * 9, dtype=np.float32), 9.0)

        chunks = list(stream_chunks("blob.flac", target_sr=sr, chunk_s=3.0))
        starts = [c[1] for c in chunks]

        assert starts == pytest.approx([0.0, 3.0, 6.0])

    @patch("whalu.data.noaa.download_audio")
    def test_chunks_reconstruct_full_audio(self, mock_dl):
        sr = 5000
        original = np.arange(sr * 6, dtype=np.float32)
        mock_dl.return_value = (original.copy(), 6.0)

        pieces = [c[0] for c in stream_chunks("blob.flac", target_sr=sr, chunk_s=2.0)]
        reconstructed = np.concatenate(pieces)

        np.testing.assert_array_equal(reconstructed, original)

    @patch("whalu.data.noaa.download_audio")
    def test_last_chunk_is_shorter(self, mock_dl):
        sr = 5000
        mock_dl.return_value = (np.zeros(sr * 5, dtype=np.float32), 5.0)

        chunks = list(stream_chunks("blob.flac", target_sr=sr, chunk_s=3.0))

        assert len(chunks) == 2
        _, _, last_dur = chunks[-1]
        assert last_dur == pytest.approx(2.0, abs=0.01)

    @patch("whalu.data.noaa.download_audio")
    def test_single_chunk_when_audio_shorter_than_chunk_s(self, mock_dl):
        sr = 5000
        mock_dl.return_value = (np.zeros(sr * 2, dtype=np.float32), 2.0)

        chunks = list(stream_chunks("blob.flac", target_sr=sr, chunk_s=3600.0))

        assert len(chunks) == 1
