"""NOAA passive acoustic data via Google Cloud Storage.

Bucket: gs://noaa-passive-bioacoustic  (public, no credentials required)

Supported programs
------------------
nrs         Ocean Noise Reference Station Network (12 stations, 2014-present)
            Path:  nrs/audio/{site}/{deployment}/audio/
            Files: NRS{site}_{YYYYMMDD}_{HHMMSS}.flac  (~4 h each, 5 kHz)

sanctsound   National Marine Sanctuary Soundscape Monitoring (2018-2021)
            Path:  sanctsound/audio/{site}/{deployment}/audio/
            Files: SanctSound_{SITE}_{deploy}_{serial}_{ISO8601Z}.flac  (15-30 min, 48-96 kHz)

The Perch multispecies_whale model expects 24 kHz input; librosa resamples
automatically. NRS files top out at 2.5 kHz (Nyquist of 5 kHz) so
high-frequency calls are absent, but low-frequency whales (blue, fin,
humpback) are unaffected.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from collections.abc import Iterator

import librosa
import numpy as np
from google.cloud import storage
from google.cloud.storage import Client

log = logging.getLogger(__name__)

BUCKET = "noaa-passive-bioacoustic"

# NRS station codes -> human-readable site name
NRS_SITES: dict[str, str] = {
    "01": "Bering Sea / Alaskan Arctic",
    "02": "Gulf of Alaska",
    "03": "Olympic Coast NMS",
    "04": "Hawaii",
    "05": "Channel Islands NMS",
    "06": "Gulf of Mexico",
    "07": "SE Continental US",
    "08": "NE Continental US",
    "09": "Stellwagen Bank NMS",
    "10": "American Samoa",
    "11": "Cordell Bank NMS",
    "12": "Buck Island Reef, USVI",
}

# SanctSound site codes -> sanctuary name
SANCTSOUND_SITES: dict[str, str] = {
    "ci": "Channel Islands",
    "fk": "Florida Keys",
    "gr": "Gray's Reef",
    "hi": "Hawaiian Islands",
    "mb": "Monterey Bay",
    "oc": "Olympic Coast",
    "pm": "Papahanaumokuakea",
    "sb": "Stellwagen Bank",
}


def _gcs() -> Client:
    """Return an anonymous GCS client."""
    return storage.Client.create_anonymous_client()


# ---------------------------------------------------------------------------
# Listing helpers
# ---------------------------------------------------------------------------

def list_deployments(program: str, site: str) -> list[str]:
    """Return sorted deployment folder names for a given program and site.

    Parameters
    ----------
    program:
        Dataset program, e.g. ``"nrs"`` or ``"sanctsound"``.
    site:
        Station/site identifier, e.g. ``"01"`` (NRS) or ``"mb01"`` (SanctSound).

    Returns
    -------
    list[str]
        Sorted deployment names (the immediate sub-folder under ``audio/{site}/``).
    """
    prefix = f"{program}/audio/{site}/"
    client = _gcs()
    bucket = client.bucket(BUCKET)
    blobs = client.list_blobs(bucket, prefix=prefix, delimiter="/")
    # Consume the iterator to populate blobs.prefixes
    list(blobs)
    deployments = sorted(
        p.rstrip("/").split("/")[-1] for p in (blobs.prefixes or [])
    )
    log.debug(
        "Found %d deployments under gs://%s/%s",
        len(deployments), BUCKET, prefix,
    )
    return deployments


def list_files(program: str, site: str, deployment: str) -> list[str]:
    """Return sorted GCS blob names for all FLAC files in a deployment.

    Parameters
    ----------
    program:
        Dataset program, e.g. ``"nrs"`` or ``"sanctsound"``.
    site:
        Station/site identifier.
    deployment:
        Deployment folder name as returned by :func:`list_deployments`.

    Returns
    -------
    list[str]
        Sorted blob names (full GCS paths relative to bucket root).
    """
    prefix = f"{program}/audio/{site}/{deployment}/audio/"
    client = _gcs()
    bucket = client.bucket(BUCKET)
    blobs = [
        b.name
        for b in client.list_blobs(bucket, prefix=prefix)
        if b.name.endswith(".flac")
    ]
    log.debug(
        "Listed %d FLAC files under gs://%s/%s",
        len(blobs), BUCKET, prefix,
    )
    return sorted(blobs)


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def parse_timestamp(blob_name: str) -> str | None:
    """Extract an ISO-8601 UTC timestamp string from a NOAA blob name.

    Handles both NRS and SanctSound naming conventions:

    - NRS:        ``NRS01_20141014_234015.flac``  -> ``"20141014T234015"``
    - SanctSound: ``SanctSound_MB01_01_...20181115T000002Z.flac`` -> ``"20181115T000002"``

    Parameters
    ----------
    blob_name:
        Full GCS blob name or just the filename.

    Returns
    -------
    str or None
        Compact UTC timestamp string ``"YYYYMMDDTHHmmSS"``, or ``None`` if
        the filename does not match a known pattern.
    """
    fname = os.path.basename(blob_name)

    # NRS: NRS01_20141014_234015.flac
    m = re.search(r"NRS\d+_(\d{8})_(\d{6})", fname)
    if m:
        return f"{m.group(1)}T{m.group(2)}"

    # SanctSound: ...20181115T000002Z.flac
    m = re.search(r"(\d{8}T\d{6})Z?\.flac$", fname, re.IGNORECASE)
    if m:
        return m.group(1)

    return None


# ---------------------------------------------------------------------------
# Audio download
# ---------------------------------------------------------------------------

def download_audio(
    blob_name: str,
    target_sr: int,
    limit_s: float | None = None,
) -> tuple[np.ndarray, float]:
    """Download a NOAA FLAC file and return resampled float32 audio.

    Parameters
    ----------
    blob_name:
        GCS blob name within ``gs://noaa-passive-bioacoustic``.
    target_sr:
        Target sample rate in Hz (model sample rate).
    limit_s:
        If given, truncate to the first ``limit_s`` seconds after loading.
        Unlike MBARI, FLAC files cannot be range-requested, so the full file
        is always downloaded.

    Returns
    -------
    tuple[np.ndarray, float]
        ``(audio_float32, duration_s)``
    """
    client = _gcs()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(blob_name)

    mb = blob.size / 1e6 if blob.size else 0
    log.debug("Downloading %.0f MB  gs://%s/%s", mb, BUCKET, blob_name)

    with tempfile.NamedTemporaryFile(suffix=".flac", delete=False) as f:
        tmp = f.name

    try:
        blob.download_to_filename(tmp)
        audio, _ = librosa.load(tmp, sr=target_sr, mono=True)
        audio = audio.astype(np.float32)
    finally:
        os.unlink(tmp)

    if limit_s is not None:
        max_samples = int(limit_s * target_sr)
        audio = audio[:max_samples]

    duration = len(audio) / target_sr
    log.debug("Loaded %.1f s at %d Hz", duration, target_sr)
    return audio, duration


# ---------------------------------------------------------------------------
# Chunked streaming
# ---------------------------------------------------------------------------

def stream_chunks(
    blob_name: str,
    target_sr: int,
    chunk_s: float = 3600.0,
) -> Iterator[tuple[np.ndarray, float, float]]:
    """Stream a NOAA FLAC file in equal-duration chunks.

    Because FLAC does not support byte-range requests, the file is downloaded
    once in full, then split in memory. Use ``limit_s`` in
    :func:`download_audio` for quick tests instead.

    Parameters
    ----------
    blob_name:
        GCS blob name within ``gs://noaa-passive-bioacoustic``.
    target_sr:
        Target sample rate in Hz.
    chunk_s:
        Duration of each chunk in seconds. Default is 3600 (1 hour).

    Yields
    ------
    tuple[np.ndarray, float, float]
        ``(audio_chunk, chunk_start_s, chunk_duration_s)``
    """
    audio, total_dur = download_audio(blob_name, target_sr)
    chunk_samples = int(chunk_s * target_sr)
    offset = 0
    chunk_start_s = 0.0

    while offset < len(audio):
        chunk = audio[offset : offset + chunk_samples]
        chunk_dur = len(chunk) / target_sr
        log.debug(
            "Chunk %.1f-%.1f h  (%d samples)",
            chunk_start_s / 3600,
            (chunk_start_s + chunk_dur) / 3600,
            len(chunk),
        )
        yield chunk, chunk_start_s, chunk_dur
        offset += chunk_samples
        chunk_start_s += chunk_s
