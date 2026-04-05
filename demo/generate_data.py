"""Convert whalu detection Parquet output into the demo JSON format.

Usage
-----
# From the repo root:
python demo/generate_data.py data/detections/mbari demo/data/

# This writes:
#   demo/data/detections.json   -- detection data for the demo
#
# Then copy or symlink the 1-hour audio file:
#   cp /path/to/MARS-20260301T000000Z-16kHz_hour0.wav demo/data/audio.wav
#
# Open demo/index.html in a browser (served via HTTP, e.g. python -m http.server)
# and load demo/data/audio.wav when prompted.

Parameters
----------
input_dir : str
    Directory containing .parquet detection files written by whalu.
output_dir : str
    Where to write detections.json.
--source : str, optional
    Override the source label shown in the demo.
--duration : float, optional
    Recording duration in seconds (default: derived from detections).
"""

import argparse
import json
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(description="Export whalu detections to demo JSON.")
    parser.add_argument("input_dir", help="Directory of .parquet detection files")
    parser.add_argument("output_dir", help="Output directory for detections.json")
    parser.add_argument("--source", default=None, help="Override source label")
    parser.add_argument("--duration", type=float, default=None, help="Recording duration (s)")
    args = parser.parse_args()

    input_dir  = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(input_dir.glob("*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No .parquet files found in {input_dir}")

    df = pl.concat([pl.read_parquet(f) for f in parquet_files])

    # Keep rank-1 detections above threshold
    df = df.filter((pl.col("rank") == 1) & (pl.col("confidence") >= 0.5))

    if df.is_empty():
        raise SystemExit("No detections above threshold found.")

    # Derive duration from data if not provided
    duration = args.duration or float(df["time_end_s"].max() or 3600)  # type: ignore[arg-type]

    # Derive source from first file name
    source = args.source or parquet_files[0].stem

    detections = [
        {
            "t":  round(float(row["time_start_s"]), 1),
            "sp": row["species"],
            "c":  round(float(row["confidence"]), 3),
        }
        for row in df.sort("time_start_s").iter_rows(named=True)
    ]

    payload = {
        "source":   source,
        "duration": duration,
        "isSample": False,
        "dets":     detections,
    }

    out = output_dir / "detections.json"
    out.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"Wrote {len(detections)} detections to {out}")
    print(f"Duration: {duration:.0f}s  |  Source: {source}")


if __name__ == "__main__":
    main()
