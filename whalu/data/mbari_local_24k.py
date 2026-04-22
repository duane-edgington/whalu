"""MBARI Pacific Sound — local 24 kHz resampled data source.

Root: /mnt/PAM_Analysis/GoogleMultiSpeciesWhaleModel2/resampled_24kHz/

Path layout: YYYY/MM/<any>.wav

Format: 24 kHz mono WAV files, pre-resampled for the Google Multi-Species
Whale Model (GMWM).

Background
----------
The MBARI S3 bucket (s3://pacific-sound-16khz) stores audio at 16 kHz.
The Google Multi-Species Whale Model requires 24 kHz input.  Passing 16 kHz
audio directly causes the model to process compressed spectrograms and
degrades classification accuracy.  This module reads from a directory of
files that have already been resampled to 24 kHz offline, avoiding any
on-the-fly resampling cost during inference.

This module mirrors the public API of whalu/data/mbari.py so that callers
can switch data sources with a one-line import change, e.g.:

    # S3 16 kHz source (original — wrong rate for GMWM)
    from whalu.data import mbari as source

    # Local 24 kHz source (this module — correct rate for GMWM)
    from whalu.data import mbari_local_24k as source

    files = source.list_files(2018, 4)
    audio, dur = source.download_audio(files[0], target_sr=24_000)

The ``target_sr`` parameter is accepted for API compatibility but is a no-op
when it matches the native 24 kHz rate.  Resampling via librosa is performed
only when a caller explicitly passes a different rate.

Model reference
---------------
Google Multi-Species Whale Model:
  https://www.kaggle.com/models/google/multispecies-whale
  Input: 24 000 Hz, mono, float32, 5-second context windows.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from pathlib import Path

import librosa
import numpy as np

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Root directory containing the pre-resampled 24 kHz WAV files.
#: Override at runtime via the environment variable WHALU_LOCAL_24K_ROOT
#: or by reassigning this module-level variable before calling any function.
DEFAULT_ROOT = Path(
    os.environ.get(
        "WHALU_LOCAL_24K_ROOT",
        "/mnt/PAM_Analysis/GoogleMultiSpeciesWhaleModel2/resampled_24kHz",
    )
)

#: Native sample rate of the files in this data source.
#: The Google Multi-Species Whale Model requires 24 000 Hz input.
_NATIVE_SR = 24_000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _month_dir(year: int, month: int, root: Path | None = None) -> Path:
    """Return the YYYY/MM directory path for a given year and month."""
    base = root if root is not None else DEFAULT_ROOT
    return base / f"{year}" / f"{month:02d}"


# ---------------------------------------------------------------------------
# Public API  (mirrors whalu/data/mbari.py)
# ---------------------------------------------------------------------------


def list_files(
    year: int,
    month: int,
    *,
    root: Path | None = None,
) -> list[str]:
    """Return sorted absolute paths for all WAV files in a given year/month.

    Parameters
    ----------
    year:   Four-digit year, e.g. 2018.
    month:  One- or two-digit month, e.g. 4 or 04.
    root:   Override the default root directory for this call only.
            Falls back to ``DEFAULT_ROOT`` (or ``$WHALU_LOCAL_24K_ROOT``).

    Returns
    -------
    Sorted list of absolute path strings, compatible with the S3 key strings
    returned by ``mbari.list_files``.
    """
    directory = _month_dir(year, month, root)
    if not directory.exists():
        log.warning("Directory does not exist: %s", directory)
        return []

    paths = sorted(
        str(p) for p in directory.iterdir() if p.suffix.lower() == ".wav"
    )
    log.debug("Listed %d WAV files under %s", len(paths), directory)
    return paths


def download_audio(
    path: str,
    target_sr: int,
    limit_s: float | None = None,
    *,
    root: Path | None = None,  # unused; present for API symmetry
) -> tuple[np.ndarray, float]:
    """Load a local 24 kHz WAV file, optionally truncated to ``limit_s`` seconds.

    Pass ``target_sr=24_000`` for the Google Multi-Species Whale Model.
    Resampling via librosa is performed automatically when ``target_sr``
    differs from the file's native 24 kHz rate.

    Returns ``(audio_float32, duration_s)``.

    Parameters
    ----------
    path:       Absolute path string as returned by :func:`list_files`.
    target_sr:  Desired output sample rate.  Use 24 000 for GMWM inference.
    limit_s:    If set, load only the first this many seconds (fast seek via
                librosa's ``duration`` argument — no full-file read needed).
    root:       Ignored; kept for API compatibility with ``mbari.download_audio``.
    """
    log.debug("Loading %s (limit_s=%s, target_sr=%d)", path, limit_s, target_sr)

    audio, _ = librosa.load(
        path,
        sr=target_sr,
        mono=True,
        duration=limit_s,  # None → load entire file
    )
    audio = audio.astype(np.float32)
    duration = len(audio) / target_sr
    log.debug("Loaded %.1fs of audio at %dHz", duration, target_sr)
    return audio, duration


def stream_chunks(
    path: str,
    target_sr: int,
    chunk_s: float = 3600.0,
    *,
    root: Path | None = None,  # unused; present for API symmetry
) -> Iterator[tuple[np.ndarray, float, float]]:
    """Stream a local 24 kHz WAV in equal-sized time chunks.

    Yields ``(audio_chunk, chunk_start_s, chunk_duration_s)`` for each chunk.
    RAM usage is bounded to approximately one chunk at a time, matching the
    behaviour of the S3 range-request path in ``mbari.stream_chunks``.

    librosa's ``offset`` / ``duration`` arguments are used so that only the
    required portion of the file is decoded on each iteration — no full-file
    load occurs.

    Parameters
    ----------
    path:       Absolute path string as returned by :func:`list_files`.
    target_sr:  Desired output sample rate.  Use 24 000 for GMWM inference.
    chunk_s:    Target chunk length in seconds (default 3600 = 1 hour).
    root:       Ignored; kept for API compatibility with ``mbari.stream_chunks``.
    """
    import soundfile as sf  # lazy import — only needed for the metadata probe

    # Probe total length without decoding audio.
    with sf.SoundFile(path) as f:
        native_sr: int = f.samplerate
        total_native_frames: int = len(f)

    total_s = total_native_frames / native_sr
    chunk_native_frames = int(chunk_s * native_sr)

    log.debug(
        "Streaming %s: %.1f h total, chunk=%.0f s, native_sr=%d",
        path,
        total_s / 3600,
        chunk_s,
        native_sr,
    )

    frame_offset = 0
    chunk_start_s = 0.0

    while frame_offset < total_native_frames:
        n_frames = min(chunk_native_frames, total_native_frames - frame_offset)
        offset_s = frame_offset / native_sr

        log.debug(
            "Chunk %.1f–%.1f h",
            chunk_start_s / 3600,
            min(chunk_start_s + chunk_s, total_s) / 3600,
        )

        audio, _ = librosa.load(
            path,
            sr=target_sr,
            mono=True,
            offset=offset_s,
            duration=n_frames / native_sr,
        )
        audio = audio.astype(np.float32)
        chunk_dur = len(audio) / target_sr

        yield audio, chunk_start_s, chunk_dur

        frame_offset += n_frames
        chunk_start_s += chunk_s
