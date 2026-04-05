"""Unit tests for pure functions in whalu.data.mbari."""

import struct

import pytest

from whalu.data.mbari import _build_wav, _find_data_chunk


def _make_wav_header(chunks: list[tuple[bytes, bytes]]) -> bytes:
    """Build a minimal RIFF/WAVE byte string with the given sub-chunks.

    Parameters
    ----------
    chunks : list of (chunk_id, chunk_data) pairs
        Each tuple becomes one RIFF sub-chunk.

    Returns
    -------
    bytes
        Valid RIFF/WAVE header bytes.
    """
    body = b""
    for chunk_id, data in chunks:
        body += chunk_id + struct.pack("<I", len(data)) + data

    riff_size = 4 + len(body)  # "WAVE" + body
    return b"RIFF" + struct.pack("<I", riff_size) + b"WAVE" + body


class TestFindDataChunk:
    """Tests for _find_data_chunk."""

    def test_finds_data_chunk_immediately_after_fmt(self):
        fmt_data = b"\x01\x00\x01\x00\x80\x3e\x00\x00\x80\x3e\x00\x00\x03\x00\x18\x00"
        audio_data = b"\x00" * 64
        header = _make_wav_header(
            [
                (b"fmt ", fmt_data),
                (b"data", audio_data),
            ]
        )
        offset, size = _find_data_chunk(header)
        assert size == 64

    def test_finds_data_chunk_after_list_chunk(self):
        fmt_data = b"\x01\x00" + b"\x00" * 14
        list_data = b"INFO" + b"\x00" * 8
        audio_data = b"\x00" * 32
        header = _make_wav_header(
            [
                (b"fmt ", fmt_data),
                (b"LIST", list_data),
                (b"data", audio_data),
            ]
        )
        offset, size = _find_data_chunk(header)
        assert size == 32

    def test_returns_correct_offset(self):
        fmt_data = b"\x00" * 16
        audio_data = b"\x00" * 16
        header = _make_wav_header(
            [
                (b"fmt ", fmt_data),
                (b"data", audio_data),
            ]
        )
        offset, _ = _find_data_chunk(header)
        # Verify the chunk ID at that offset really is 'data'
        assert header[offset : offset + 4] == b"data"

    def test_raises_if_no_data_chunk(self):
        fmt_data = b"\x00" * 16
        header = _make_wav_header([(b"fmt ", fmt_data)])
        with pytest.raises(ValueError, match="No 'data' chunk"):
            _find_data_chunk(header)

    def test_data_chunk_size_matches_audio(self):
        audio = b"\xab\xcd\xef" * 100
        header = _make_wav_header(
            [
                (b"fmt ", b"\x00" * 16),
                (b"data", audio),
            ]
        )
        _, size = _find_data_chunk(header)
        assert size == len(audio)


class TestBuildWav:
    """Tests for _build_wav."""

    def _header_and_offset(
        self, audio_placeholder: bytes = b"\x00" * 8
    ) -> tuple[bytes, int]:
        """Return (header_bytes, data_offset) for a simple WAV."""
        header = _make_wav_header(
            [
                (b"fmt ", b"\x00" * 16),
                (b"data", audio_placeholder),
            ]
        )
        offset, _ = _find_data_chunk(header)
        return header, offset

    def test_returns_bytes(self):
        header, offset = self._header_and_offset()
        result = _build_wav(header, offset, b"\x00" * 16)
        assert isinstance(result, bytes)

    def test_starts_with_riff(self):
        header, offset = self._header_and_offset()
        result = _build_wav(header, offset, b"\x00" * 16)
        assert result[:4] == b"RIFF"

    def test_data_chunk_size_patched(self):
        header, offset = self._header_and_offset()
        audio = b"\x01\x02\x03" * 20
        result = _build_wav(header, offset, audio)
        patched_size = struct.unpack_from("<I", result, offset + 4)[0]
        assert patched_size == len(audio)

    def test_riff_size_patched(self):
        header, offset = self._header_and_offset()
        audio = b"\x00" * 30
        result = _build_wav(header, offset, audio)
        riff_size = struct.unpack_from("<I", result, 4)[0]
        assert riff_size == len(result) - 8

    def test_audio_data_embedded(self):
        header, offset = self._header_and_offset()
        audio = bytes(range(16))
        result = _build_wav(header, offset, audio)
        # Audio starts right after the data chunk header (8 bytes)
        assert result[offset + 8 : offset + 8 + len(audio)] == audio

    def test_different_audio_lengths(self):
        header, offset = self._header_and_offset()
        for n in [0, 1, 100, 1000]:
            audio = b"\xff" * n
            result = _build_wav(header, offset, audio)
            size = struct.unpack_from("<I", result, offset + 4)[0]
            assert size == n
