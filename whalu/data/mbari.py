"""MBARI Pacific Sound — public S3 data source.

Bucket: s3://pacific-sound-16khz  (no credentials needed)
Path:   YYYY/MM/MARS-YYYYMMDDTHHMMSSZ-16kHz.wav
Format: 16kHz mono 24-bit PCM, one file per day (24h = 4.1GB each)
WAV structure: RIFF/fmt/LIST(metadata)/data — data chunk starts at byte 332
"""

import logging
import os
import struct
import tempfile
from collections.abc import Iterator

import boto3
import librosa
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config

log = logging.getLogger(__name__)

BUCKET = "pacific-sound-16khz"
_NATIVE_SR = 16_000
_BYTES_PER_SAMPLE = 3  # 24-bit mono


def _s3():
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def list_files(year: int, month: int) -> list[str]:
    """Return sorted S3 keys for all WAV files in a given year/month."""
    prefix = f"{year}/{month:02d}/"
    paginator = _s3().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".wav"):
                keys.append(obj["Key"])
    log.debug("Listed %d files under s3://%s/%s", len(keys), BUCKET, prefix)
    return sorted(keys)


def _find_data_chunk(header: bytes) -> tuple[int, int]:
    """Walk WAV chunks to find the 'data' chunk. Returns (offset, size)."""
    offset = 12  # skip RIFF/WAVE preamble
    while offset < len(header) - 8:
        chunk_id = header[offset : offset + 4]
        chunk_size = struct.unpack_from("<I", header, offset + 4)[0]
        if chunk_id == b"data":
            return offset, chunk_size
        offset += 8 + chunk_size
    raise ValueError("No 'data' chunk found in WAV header")


def _build_wav(header_bytes: bytes, data_offset: int, audio_data: bytes) -> bytes:
    """
    Assemble a valid WAV from the original header (up through the data chunk
    header) with the data size fields patched to match the actual audio_data.
    """
    audio_len = len(audio_data)
    wav = bytearray(header_bytes[: data_offset + 8] + audio_data)
    struct.pack_into("<I", wav, data_offset + 4, audio_len)  # data chunk size
    struct.pack_into("<I", wav, 4, len(wav) - 8)  # RIFF size
    return bytes(wav)


def download_audio(
    key: str,
    target_sr: int,
    limit_s: float | None = None,
) -> tuple[np.ndarray, float]:
    """
    Download a MBARI WAV, optionally truncated to the first `limit_s` seconds.
    Resamples to target_sr. Returns (audio_float32, duration_s).

    With limit_s the download is a small S3 range request instead of 4GB.
    """
    s3 = _s3()

    header_bytes = s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-511")[
        "Body"
    ].read()
    data_offset, full_data_size = _find_data_chunk(header_bytes)
    audio_start = data_offset + 8

    if limit_s is not None:
        audio_bytes_wanted = min(
            int(limit_s * _NATIVE_SR * _BYTES_PER_SAMPLE), full_data_size
        )
    else:
        audio_bytes_wanted = full_data_size

    mb = audio_bytes_wanted / 1e6
    log.debug("Downloading %.0fMB from s3://%s/%s", mb, BUCKET, key)

    end_byte = audio_start + audio_bytes_wanted - 1
    audio_data = s3.get_object(
        Bucket=BUCKET, Key=key, Range=f"bytes={audio_start}-{end_byte}"
    )["Body"].read()

    wav_bytes = _build_wav(header_bytes, data_offset, audio_data)

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        f.write(wav_bytes)
        tmp = f.name

    try:
        audio, _ = librosa.load(tmp, sr=target_sr, mono=True)
        audio = audio.astype(np.float32)
    finally:
        os.unlink(tmp)

    duration = len(audio) / target_sr
    log.debug("Loaded %.1fs of audio at %dHz", duration, target_sr)
    return audio, duration


def stream_chunks(
    key: str,
    target_sr: int,
    chunk_s: float = 3600.0,
) -> Iterator[tuple[np.ndarray, float, float]]:
    """
    Stream a full MBARI file in equal-sized chunks via S3 range requests.

    Yields (audio_chunk, chunk_start_s, chunk_duration_s) for each chunk.
    RAM usage is bounded to one chunk at a time (~1.4 GB for 1-hour chunks).
    Detection windows are stateless so chunking produces identical results.
    """
    s3 = _s3()
    header_bytes = s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-511")[
        "Body"
    ].read()
    data_offset, full_data_size = _find_data_chunk(header_bytes)
    audio_start = data_offset + 8
    total_s = full_data_size / (_NATIVE_SR * _BYTES_PER_SAMPLE)

    chunk_bytes = int(chunk_s * _NATIVE_SR * _BYTES_PER_SAMPLE)
    byte_offset = 0
    chunk_start_s = 0.0

    while byte_offset < full_data_size:
        n_bytes = min(chunk_bytes, full_data_size - byte_offset)
        start_byte = audio_start + byte_offset
        end_byte = start_byte + n_bytes - 1

        log.debug(
            "Chunk %.1f–%.1fh  (%.0f MB)",
            chunk_start_s / 3600,
            min(chunk_start_s + chunk_s, total_s) / 3600,
            n_bytes / 1e6,
        )

        audio_data = s3.get_object(
            Bucket=BUCKET, Key=key, Range=f"bytes={start_byte}-{end_byte}"
        )["Body"].read()

        wav_bytes = _build_wav(header_bytes, data_offset, audio_data)

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            f.write(wav_bytes)
            tmp = f.name

        try:
            audio, _ = librosa.load(tmp, sr=target_sr, mono=True)
            audio = audio.astype(np.float32)
        finally:
            os.unlink(tmp)

        chunk_dur = len(audio) / target_sr
        yield audio, chunk_start_s, chunk_dur

        byte_offset += n_bytes
        chunk_start_s += chunk_s
