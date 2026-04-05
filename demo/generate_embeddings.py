"""Generate Perch v2 acoustic novelty curve for the demo.

Slides a 5-second window (2.5s hop) over the audio clip, embeds each window
with Perch v2, and computes cosine distance between adjacent windows. High
novelty = large acoustic change = likely call boundary or detection event.

Usage
-----
# From the repo root (requires the raw PCM segment - see note below):
python demo/generate_embeddings.py --input /tmp/segment_correct.bin --output demo/data/

# The input file is raw 24-bit signed little-endian PCM at 16kHz.
# It was downloaded from MBARI Pacific Sound S3 using the byte range:
#   bytes=96960340-106560340  (MARS-20260301T000000Z-16kHz.wav, t=2020-2220s)
# To re-download it:
#   curl -s -o /tmp/segment_correct.bin \\
#     -H "Range: bytes=96960340-106560340" \\
#     "https://pacific-sound-16khz.s3.amazonaws.com/2026/03/MARS-20260301T000000Z-16kHz.wav"

Parameters
----------
input : str
    Path to raw 24-bit signed little-endian PCM at 16kHz.
output : str
    Output directory for embeddings.json.
--hop : float
    Hop size in seconds (default 2.5, matching multispecies_whale).
"""

import argparse
import json
import struct
from pathlib import Path

import librosa
import numpy as np
from perch_hoplite.zoo import model_configs


def load_raw_s24le(path: Path) -> np.ndarray:
    """Load a raw 24-bit signed little-endian PCM file as float32."""
    data = path.read_bytes()
    n_samples = len(data) // 3
    samples = np.empty(n_samples, dtype=np.float32)
    for i in range(n_samples):
        b = data[i * 3 : i * 3 + 3]
        val = struct.unpack("<i", b + (b"\xff" if b[2] & 0x80 else b"\x00"))[0] >> 8
        samples[i] = val / 8_388_607.0
    return samples


def cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance in [0, 1]: 0 = identical, 1 = orthogonal."""
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    return float(1.0 - np.dot(a, b) / denom)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Perch v2 novelty curve.")
    parser.add_argument("--input", required=True, help="Raw s24le PCM file at 16kHz")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--hop", type=float, default=2.5, help="Hop size in seconds")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading Perch v2 (CPU)...")
    model = model_configs.load_model_by_name("perch_v2_cpu")
    target_sr: int = model.sample_rate  # 32000
    window_s: float = getattr(model, "window_size_s", 5.0)

    print(f"Perch v2: {target_sr}Hz, {window_s}s window")

    print(f"Loading audio from {input_path}...")
    audio_16k = load_raw_s24le(input_path)
    audio = librosa.resample(audio_16k, orig_sr=16000, target_sr=target_sr)
    duration = len(audio) / target_sr
    print(f"Audio: {duration:.1f}s at {target_sr}Hz")

    window_samples = int(window_s * target_sr)
    hop_samples = int(args.hop * target_sr)
    n_frames = max(1, (len(audio) - window_samples) // hop_samples + 1)

    print(f"Embedding {n_frames} windows ({window_s}s window, {args.hop}s hop)...")
    embeddings = []
    timestamps = []

    for i in range(n_frames):
        start = i * hop_samples
        chunk = audio[start : start + window_samples]
        if len(chunk) < window_samples:
            break
        outputs = model.embed(chunk)
        # embeddings shape: [frames, channels, features] - pool to 1-D
        raw_emb = outputs.embeddings
        assert raw_emb is not None, "Perch v2 returned no embeddings"
        emb = raw_emb.mean(axis=tuple(range(raw_emb.ndim - 1)))
        embeddings.append(emb)
        timestamps.append(round(i * args.hop, 1))
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{n_frames}")

    embeddings_arr = np.array(embeddings)  # (n_frames, 1536)

    # Cosine distance between adjacent windows
    windows = []
    for i, (t, emb) in enumerate(zip(timestamps, embeddings)):
        novelty = 0.0 if i == 0 else cosine_distance(embeddings_arr[i - 1], emb)
        windows.append({"t": t, "novelty": round(novelty, 4)})

    out = output_dir / "embeddings.json"
    out.write_text(json.dumps({"windows": windows}, separators=(",", ":")) + "\n")

    max_novelty = max(w["novelty"] for w in windows)
    print(f"Wrote {len(windows)} windows to {out}")
    print(f"Novelty range: 0 - {max_novelty:.4f}")


if __name__ == "__main__":
    main()
