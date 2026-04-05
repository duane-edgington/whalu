"""Orcasound — public S3 data source (labeled killer whale recordings).

Bucket: s3://acoustic-sandbox  (no credentials needed)
"""

import io
import os
import tempfile

import boto3
import librosa
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config

BUCKET = "acoustic-sandbox"

# Known labeled sample for quick validation
SAMPLE_KEY = (
    "labeled-data/classification/killer-whales/southern-residents/"
    "20190705/orcasound-lab/test-only/OS_7_05_2019_08_24_00_.wav"
)


def _s3():
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def download_audio(key: str, target_sr: int) -> tuple[np.ndarray, float]:
    """Download a WAV from Orcasound S3, resample to target_sr, return (audio, duration_s)."""
    buf = io.BytesIO()
    _s3().download_fileobj(BUCKET, key, buf)
    buf.seek(0)

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        f.write(buf.read())
        tmp = f.name

    try:
        audio, _ = librosa.load(tmp, sr=target_sr, mono=True)
        audio = audio.astype(np.float32)
    finally:
        os.unlink(tmp)

    return audio, len(audio) / target_sr
