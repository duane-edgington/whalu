"""MBARI Pacific Sound — local 32 kHz resampled data source.

Root: /mnt/PAM_Analysis/GoogleMultiSpeciesWhaleModel2/resampled_32kHz/

Path layout: YYYY/MM/<any>.wav

Format: 32 kHz mono WAV files, pre-resampled for Perch 2.0.

This module mirrors the public API of whalu/data/mbari.py so that callers
can switch data sources by changing one import, e.g.:

    # S3 16 kHz source (original)
    from whalu.data import mbari as source

    # Local 32 kHz source (this module)
    from whalu.data import mbari_local_32k as source

    files = source.list_files(2018, 4)
    audio, dur = source.download_audio(files[0], target_sr=32_000)

The ``target_sr`` parameter is accepted for API compatibility but is
essentially a no-op when it matches the native 32 kHz sample rate.
Resampling via librosa is performed only when a caller explicitly requests
a different rate.
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

#: Root directory containing the pre-resampled 32 kHz WAV files.
#: Override at runtime via the environment variable WHALU_LOCAL_32K_ROOT
#: or by reassigning this module-level variable before calling any function.
DEFAULT_ROOT = Path(
    os.environ.get(
        "WHALU_LOCAL_32K_ROOT",
        "/mnt/PAM_Analysis/GoogleMultiSpeciesWhaleModel2/resampled_32kHz",
    )
)

_NATIVE_SR = 32_000


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def _month_dir(year: int, month: int, root: Path | None = None) -> Path:
    """Return the directory path for a given year/month."""
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

    Returns
    -------
    Sorted list of absolute path strings (compatible with the S3 key strings
    returned by the original ``mbari.list_files``).
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
    """Load a local 32 kHz WAV file, optionally truncated to ``limit_s`` seconds.

    Resamples to ``target_sr`` when it differs from the native 32 kHz rate.
    Returns ``(audio_float32, duration_s)``.

    Parameters
    ----------
    path:       Absolute path string as returned by :func:`list_files`.
    target_sr:  Desired output sample rate (32 000 for Perch 2.0).
    limit_s:    If set, load only the first this many seconds (fast seek).
    root:       Ignored; kept for API compatibility.
    """
    duration_arg = limit_s  # librosa accepts ``duration`` in seconds
    log.debug("Loading %s (limit_s=%s, target_sr=%d)", path, limit_s, target_sr)

    audio, _ = librosa.load(
        path,
        sr=target_sr,
        mono=True,
        duration=duration_arg,
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
    """Stream a local 32 kHz WAV in equal-sized chunks.

    Yields ``(audio_chunk, chunk_start_s, chunk_duration_s)`` for each chunk,
    keeping RAM bounded to roughly one chunk at a time — matching the
    behaviour of the S3 streaming path in ``mbari.stream_chunks``.

    Parameters
    ----------
    path:       Absolute path string as returned by :func:`list_files`.
    target_sr:  Desired output sample rate (32 000 for Perch 2.0).
    chunk_s:    Target chunk length in seconds (default 3600 = 1 hour).
    root:       Ignored; kept for API compatibility.
    """
    import soundfile as sf  # lazy import — only needed for metadata probe

    with sf.SoundFile(path) as f:
        native_sr = f.samplerate
        total_native_frames = len(f)

    total_s = total_native_frames / native_sr
    chunk_native_frames = int(chunk_s * native_sr)
    log.debug(
        "Streaming %s: %.1f h total, chunk=%.0f s", path, total_s / 3600, chunk_s
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

        # librosa.load with offset/duration avoids loading the entire file.
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
