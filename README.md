# whalu

Marine bioacoustics detection pipeline. Slides a 5-second window over continuous hydrophone recordings, runs the Google [Perch multispecies whale model](https://huggingface.co/google/multispecies-whale), and stores detections as Parquet files for analysis.

![whalu logo](assets/logo.svg)

## Supported data sources

| Source | Coverage | Format |
|--------|----------|--------|
| [MBARI Pacific Sound](https://registry.opendata.aws/pacific-sound/) | 2015-present, Monterey Canyon | 16 kHz 24-bit WAV, 1 file/day (4.1 GB) |
| [Orcasound](https://www.orcasound.net/) | 2017-present, Puget Sound | 20 kHz 16-bit WAV |

## Install

```bash
git clone https://github.com/amrit110/whalu
cd whalu
uv sync
```

## Usage

```bash
# Show help and available commands
uv run whalu

# Run detection on one MBARI day (full 24h, streamed in 1h chunks)
uv run whalu scan mbari --start 2026-03 --max-files 1 --output-dir data/detections/mbari

# Quick test: first hour only
uv run whalu scan mbari --start 2026-03 --max-files 1 --limit-hours 1 --output-dir data/detections/mbari

# Scan a date range
uv run whalu scan mbari --start 2023-07 --end 2023-10 --output-dir data/detections/mbari

# Analyze detections
uv run whalu analyze --input-dir data/detections/mbari

# Show source info
uv run whalu info mbari
```

## How it works

- Streams 4 GB daily files via S3 range requests in 1-hour chunks (~172 MB each) — bounded RAM
- Applies sigmoid activation (correct for the multi-label whale model, not softmax)
- Emits detections only where confidence >= 0.5
- Stores one Parquet file per audio source — runs are resumable

## Detection model

Google [`multispecies_whale`](https://huggingface.co/google/multispecies-whale) via [perch-hoplite](https://github.com/google-research/perch). 12 classes: blue whale (Bm), fin whale (Bp), humpback (Mn), minke (Ba), Bryde's (Be), sei (Bs), right whale (Eg), orca (Oo), and call types (Upcall, Gunshot, Call, Echolocation, Whistle).

## Built for

[Kaggle Gemma 4 Good Hackathon](https://www.kaggle.com/competitions/gemma-4-good-hackathon) — ocean health track.
