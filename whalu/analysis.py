"""Detection analysis: temporal patterns, species distributions."""

import re
from datetime import datetime, timezone

import polars as pl


def add_timestamps(df: pl.DataFrame) -> pl.DataFrame:
    """
    Parse absolute UTC timestamps from the source column + time_start_s.

    Source format: mbari/MARS-20260301T000000Z-16kHz
    Adds columns: timestamp (datetime), hour (0-23), date (date)
    """
    # Extract YYYYMMDD and HHMMSS from source string
    dates = []
    for source in df["source"].to_list():
        m = re.search(r"(\d{8})T(\d{6})", source)
        if m:
            base = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc
            )
        else:
            base = datetime(2000, 1, 1, tzinfo=timezone.utc)
        dates.append(base)

    base_ts = [int(d.timestamp()) for d in dates]

    return df.with_columns(
        (
            (pl.Series("_base_ts", base_ts) + pl.col("time_start_s").cast(pl.Int64))
            * 1_000
        )
        .cast(pl.Datetime("ms", "UTC"))
        .alias("timestamp")
    ).with_columns(
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.date().alias("date"),
    )


def species_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Per-species totals: windows, time detected, mean/max confidence."""
    rank1 = df.filter((pl.col("rank") == 1) & (pl.col("confidence") >= 0.5))
    # Estimate total windows from time span per source (non-detection windows
    # produce no rows, so we can't count them directly from the dataframe)
    total_windows = int(
        df.group_by("source")
        .agg((pl.col("time_end_s").max() / 2.5).alias("n"))["n"]
        .sum()
    )
    return (
        rank1.group_by("species")
        .agg(
            pl.len().alias("windows"),
            (pl.len() * 2.5 / 60).alias("minutes"),
            pl.col("confidence").mean().alias("mean_conf"),
            pl.col("confidence").max().alias("max_conf"),
        )
        .with_columns(
            (pl.col("windows") / total_windows * 100).round(1).alias("pct_of_time")
        )
        .sort("windows", descending=True)
    )


def hourly_activity(df: pl.DataFrame, top_n: int = 5) -> tuple[pl.DataFrame, list[str]]:
    """
    Detection rate (%) per species per hour of day.
    Returns a wide DataFrame: rows = hours (0-23), cols = top species.
    """
    rank1 = df.filter((pl.col("rank") == 1) & (pl.col("confidence") >= 0.5))

    top_species = (
        rank1.group_by("species")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(top_n)["species"]
        .to_list()
    )

    # Fixed denominator: 3600s / 2.5s hop = 1440 windows per hour
    WINDOWS_PER_HOUR = 3600 / 2.5

    # Detections per species per hour
    counts = (
        rank1.filter(pl.col("species").is_in(top_species))
        .group_by(["hour", "species"])
        .agg(pl.len().alias("n"))
        .with_columns((pl.col("n") / WINDOWS_PER_HOUR * 100).round(1).alias("rate"))
        .pivot(on="species", index="hour", values="rate", aggregate_function="mean")
        .sort("hour")
    )

    # Fill missing hours/species with 0
    all_hours = pl.DataFrame({"hour": list(range(24))})
    counts = all_hours.join(counts, on="hour", how="left").fill_null(0.0)

    return counts, top_species


def daily_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Detections per day per species."""
    return (
        df.filter((pl.col("rank") == 1) & (pl.col("confidence") >= 0.5))
        .group_by(["date", "species"])
        .agg(pl.len().alias("windows"))
        .sort(["date", "species"])
    )
