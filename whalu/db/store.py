"""Parquet-based detection store with per-file checkpointing."""

from pathlib import Path

import polars as pl


class DetectionStore:
    """
    Writes one Parquet file per audio file processed.
    Skips files already on disk → safe to resume interrupted runs.
    """

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, stem: str) -> Path:
        return self.output_dir / f"{stem}.parquet"

    def is_done(self, stem: str) -> bool:
        return self._path(stem).exists()

    def write(self, df: pl.DataFrame, stem: str) -> Path:
        p = self._path(stem)
        df.write_parquet(p)
        return p

    def merge(self) -> pl.DataFrame:
        """Concatenate all stored Parquet files into one DataFrame."""
        files = sorted(self.output_dir.glob("*.parquet"))
        if not files:
            return pl.DataFrame()
        return pl.concat([pl.read_parquet(f) for f in files])

    def summary(self, hop_size_s: float = 2.5) -> pl.DataFrame:
        """
        Top-species summary across all stored detections.

        Includes `minutes_detected` = windows × hop_size_s / 60,
        representing unique non-overlapping time with that species dominant.
        """
        df = self.merge()
        if df.is_empty():
            return df
        return (
            df.filter((pl.col("rank") == 1) & (pl.col("confidence") > 0.05))
            .group_by("species")
            .agg(
                pl.len().alias("windows"),
                (pl.len() * hop_size_s / 60).alias("minutes_detected"),
                pl.col("confidence").max().alias("max_conf"),
                pl.col("confidence").mean().alias("mean_conf"),
            )
            .sort("windows", descending=True)
        )
