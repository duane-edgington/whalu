![whalu logo](assets/logo.svg)

[![PyPI](https://img.shields.io/pypi/v/whalu)](https://pypi.org/project/whalu/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://pypi.org/project/whalu/)
[![code checks](https://github.com/amrit110/whalu/actions/workflows/code_checks.yml/badge.svg)](https://github.com/amrit110/whalu/actions/workflows/code_checks.yml)
[![unit tests](https://github.com/amrit110/whalu/actions/workflows/unit_tests.yml/badge.svg)](https://github.com/amrit110/whalu/actions/workflows/unit_tests.yml)
[![codecov](https://codecov.io/gh/amrit110/whalu/branch/main/graph/badge.svg)](https://codecov.io/gh/amrit110/whalu)
[![License](https://img.shields.io/github/license/amrit110/whalu)](LICENSE.md)

Marine bioacoustics detection pipeline. Slides a 5-second window over continuous hydrophone recordings, runs the Google Perch multispecies whale model, and stores detections as Parquet files for analysis.

## Install

```bash
pip install whalu
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add whalu
```

## CLI

```
whalu [-v] <command> ...
```

| Command | Description |
|---------|-------------|
| `whalu scan mbari` | Run detection over MBARI Pacific Sound (S3, no auth required) |
| `whalu scan orcasound` | Run detection over Orcasound labeled samples (S3, no auth required) |
| `whalu analyze` | Summarize and visualize stored detections |
| `whalu info [source]` | Show sensor/dataset metadata for a source |

### `whalu scan mbari`

| Flag | Default | Description |
|------|---------|-------------|
| `--start YYYY-MM` | required | First year-month to process |
| `--end YYYY-MM` | same as `--start` | Last year-month (inclusive) |
| `--max-files N` | all | Stop after N files |
| `--limit-hours N` | full file | Only process first N hours per file |
| `--output-dir PATH` | `data/detections/mbari` | Where to write Parquet files |

### `whalu scan orcasound`

| Flag | Default | Description |
|------|---------|-------------|
| `--key S3_KEY` | labeled killer whale sample | Specific S3 key to process |
| `--output-dir PATH` | `data/detections/orcasound` | Where to write Parquet files |

### `whalu analyze`

| Flag | Default | Description |
|------|---------|-------------|
| `--input-dir PATH` | required | Directory of detection Parquet files |
| `--top-n N` | `5` | Number of top species shown in heatmap |

### `whalu info`

| Flag | Default | Description |
|------|---------|-------------|
| `source` | (all) | Source ID to inspect (`mbari`, `orcasound`) |

### Examples

```bash
# Single file, first hour only (quick test)
whalu scan mbari --start 2026-03 --max-files 1 --limit-hours 1

# Full month
whalu scan mbari --start 2026-03 --output-dir data/detections/mbari

# Multi-month date range (blue whale season)
whalu scan mbari --start 2023-07 --end 2023-10

# Orcasound validation sample
whalu scan orcasound

# Analyze stored detections
whalu analyze --input-dir data/detections/mbari

# Show source metadata
whalu info mbari
whalu info
```

## Supported data sources

| Source | Location | Coverage | Format |
|--------|----------|----------|--------|
| [MBARI Pacific Sound](https://registry.opendata.aws/pacific-sound/) | Monterey Canyon, CA | 2015-present | 16 kHz 24-bit WAV, 1 file/day (4.1 GB) |
| [Orcasound](https://www.orcasound.net/) | Puget Sound, WA | 2017-present | 20 kHz 16-bit WAV |

## How it works

- Streams 4 GB daily files via S3 range requests in 1-hour chunks (~172 MB each), bounded RAM
- Applies sigmoid activation (correct for the multi-label whale model, not softmax)
- Emits detections only where confidence >= 0.5
- Stores one Parquet file per audio source, runs are resumable

## Detection model

Google `multispecies_whale` via [perch-hoplite](https://github.com/google-research/perch). 12 classes: blue whale (Bm), fin whale (Bp), humpback (Mn), minke (Ba), Bryde's (Be), sei (Bs), right whale (Eg), orca (Oo), and call types (Upcall, Gunshot, Call, Echolocation, Whistle).

