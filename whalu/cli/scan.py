"""
whalu-scan: Run whale detection over MBARI / Orcasound audio archives.

Examples
--------
# Single day test (1 file, first hour)
uv run whalu-scan mbari --start 2026-03 --max-files 1 --limit-hours 1

# Full month
uv run whalu-scan mbari --start 2026-03 --output-dir data/detections/mbari

# Date range (blue whale season)
uv run whalu-scan mbari --start 2023-07 --end 2023-10

# Orcasound validation sample
uv run whalu-scan orcasound
"""

import argparse
import logging
import sys
from pathlib import Path

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from whalu._logging import setup_logging
from whalu.analysis import (
    add_timestamps,
    species_summary,
    hourly_activity,
    daily_counts,
)
from whalu.models.loader import get_whale_model
import whalu.data.mbari as mbari
import whalu.data.orcasound as orcasound  # type: ignore[reportMissingImports]
from whalu.detection.runner import run_detections
from whalu.db.store import DetectionStore
from whalu.sources import REGISTRY as SOURCE_REGISTRY, SourceInfo
from whalu.species import display_name, scientific_name

log = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Progress bar
# ---------------------------------------------------------------------------


def _progress() -> Progress:
    return Progress(
        SpinnerColumn(spinner_name="dots"),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


# ---------------------------------------------------------------------------
# MBARI
# ---------------------------------------------------------------------------


def _parse_ym(s: str) -> tuple[int, int]:
    try:
        year, month = s.split("-")
        return int(year), int(month)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM, got: {s!r}")


def _month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    sy, sm = start
    ey, em = end
    result = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def cmd_mbari(args: argparse.Namespace) -> None:
    start = _parse_ym(args.start)
    end = _parse_ym(args.end) if args.end else start
    periods = _month_range(start, end)
    limit_s = args.limit_hours * 3600 if args.limit_hours else None

    model = get_whale_model()
    store = DetectionStore(args.output_dir)

    total = skipped = processed = errors = 0

    with _progress() as progress:
        for year, month in periods:
            console.print(
                Rule(
                    f"[bold cyan]MBARI Pacific Sound  ·  {year}-{month:02d}",
                    style="cyan",
                )
            )

            keys = mbari.list_files(year, month)
            if not keys:
                log.warning("No WAV files found for %d-%02d", year, month)
                continue

            if args.max_files:
                keys = keys[: args.max_files]

            suffix = f"  [dim](limit {args.limit_hours}h/file)[/dim]" if limit_s else ""
            log.info(
                "Processing [bold cyan]%d[/bold cyan] files%s → [dim]%s[/dim]",
                len(keys),
                suffix,
                args.output_dir,
            )

            task = progress.add_task(f"{year}-{month:02d}", total=len(keys))

            for key in keys:
                fname = Path(key).name
                stem = f"mbari_{year}_{month:02d}_{Path(key).stem}"
                if limit_s:
                    stem += f"_lim{args.limit_hours}h"
                total += 1

                progress.update(task, description=f"[cyan]{fname[:40]}[/cyan]")

                if store.is_done(stem):
                    log.debug("Skip (already done): %s", stem)
                    skipped += 1
                    progress.advance(task)
                    continue

                try:
                    import polars as pl

                    source_name = f"mbari/{Path(key).stem}"

                    if limit_s is not None:
                        # Small test download — single range request
                        audio, dur = mbari.download_audio(
                            key, model.sample_rate, limit_s=limit_s
                        )
                        log.info(
                            "[green]✓[/green] %-42s  [cyan]%.2fh[/cyan]",
                            fname,
                            dur / 3600,
                        )
                        df = run_detections(model, audio, source_name=source_name)
                    else:
                        # Full 24h file — stream 1-hour chunks (~172 MB each, ~2 GB RAM max)
                        chunk_dfs: list[pl.DataFrame] = []
                        total_dur = 0.0
                        n_chunks = 24  # 24h / 1h chunks
                        chunk_task = progress.add_task(
                            f"[dim]{fname[:32]}  chunk 0/{n_chunks}[/dim]",
                            total=n_chunks,
                        )
                        for (
                            chunk_audio,
                            chunk_start_s,
                            chunk_dur_s,
                        ) in mbari.stream_chunks(key, model.sample_rate):
                            chunk_idx = int(chunk_start_s / 3600) + 1
                            progress.update(
                                chunk_task,
                                description=f"[dim]{fname[:28]}  {chunk_idx:02d}/{n_chunks}h[/dim]",
                            )
                            chunk_df = run_detections(
                                model,
                                chunk_audio,
                                source_name=source_name,
                                offset_s=chunk_start_s,
                            )
                            chunk_dfs.append(chunk_df)
                            total_dur += chunk_dur_s
                            progress.advance(chunk_task)
                        progress.remove_task(chunk_task)
                        df = (
                            pl.concat(chunk_dfs)
                            if chunk_dfs
                            else pl.DataFrame(
                                {
                                    "source": [],
                                    "time_start_s": [],
                                    "time_end_s": [],
                                    "species": [],
                                    "confidence": [],
                                    "rank": [],
                                }
                            )
                        )
                        log.info(
                            "[green]✓[/green] %-42s  [cyan]%.1fh[/cyan]  %d detections",
                            fname,
                            total_dur / 3600,
                            len(df),
                        )

                    store.write(df, stem)
                    processed += 1
                except Exception as exc:
                    log.error("[red]✗[/red] %s: %s", fname, exc, exc_info=True)
                    errors += 1

                progress.advance(task)

    console.print()
    stats = Table.grid(padding=(0, 2))
    stats.add_row(
        Text(f"✓ {processed} processed", style="bold green"),
        Text(f"↷ {skipped} skipped", style="dim"),
        Text(f"✗ {errors} errors", style="bold red" if errors else "dim"),
        Text(f"∑ {total} total", style="bold"),
    )
    console.print(
        Panel(stats, title="Run complete", border_style="green", padding=(0, 2))
    )
    _render_summary(store)


# ---------------------------------------------------------------------------
# Orcasound
# ---------------------------------------------------------------------------


def cmd_orcasound(args: argparse.Namespace) -> None:
    model = get_whale_model()
    store = DetectionStore(args.output_dir)

    key = args.key or orcasound.SAMPLE_KEY
    stem = f"orcasound_{Path(key).stem}"

    console.print(Rule("[bold cyan]Orcasound · acoustic-sandbox", style="cyan"))

    if store.is_done(stem):
        log.info("Already processed: [dim]%s[/dim]", stem)
        _render_summary(store)
        return

    with _progress() as progress:
        task = progress.add_task(Path(key).name, total=None)
        audio, dur = orcasound.download_audio(key, model.sample_rate)
        progress.update(task, total=1, completed=0, description="Running inference...")
        log.info(
            "Loaded [cyan]%.1fs[/cyan] at [cyan]%dHz[/cyan]", dur, model.sample_rate
        )
        df = run_detections(model, audio, source_name=f"orcasound/{Path(key).stem}")
        store.write(df, stem)
        progress.update(task, completed=1)

    log.info(
        "[green]✓[/green] %d rows → [dim]%s/%s.parquet[/dim]",
        len(df),
        args.output_dir,
        stem,
    )
    _render_summary(store)


# ---------------------------------------------------------------------------
# Info command
# ---------------------------------------------------------------------------


def cmd_info(args: argparse.Namespace) -> None:
    source_id = args.info_source
    if source_id and source_id not in SOURCE_REGISTRY:
        log.error(
            "Unknown source '%s'. Available: %s", source_id, ", ".join(SOURCE_REGISTRY)
        )
        sys.exit(1)

    sources = (
        [SOURCE_REGISTRY[source_id]] if source_id else list(SOURCE_REGISTRY.values())
    )
    for src in sources:
        _render_source_info(src)
        if len(sources) > 1:
            console.print()


def _render_source_info(src: SourceInfo) -> None:
    console.print()
    console.print(Rule(f"[bold {_O}]{src.name}[/bold {_O}]", style=_S))
    console.print()

    # ── Description ─────────────────────────────────────────────────────────
    console.print(
        Panel(
            Text(src.description, style="white"),
            border_style=_D,
            padding=(0, 2),
            width=74,
        )
    )
    console.print()

    # ── Sensor facts ────────────────────────────────────────────────────────
    facts = Table.grid(padding=(0, 3))
    facts.add_column(style=_D, min_width=18, no_wrap=True)
    facts.add_column(style="white", max_width=32)
    for label, value in [
        ("Operator", src.operator),
        ("Location", src.location),
        ("Coordinates", src.coordinates),
        ("Sensor depth", f"{src.depth_m} m"),
        ("Range", f"{src.detection_range_km} km"),
        ("Coverage", src.coverage),
        ("Volume", src.volume),
        ("Format", src.file_format),
    ]:
        facts.add_row(label, value)

    # ── Species list ─────────────────────────────────────────────────────────
    sp_table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0))
    sp_table.add_column(style=_T, no_wrap=True)
    for sp in src.species:
        sp_table.add_row(f"  • {sp}")

    from rich.columns import Columns as RichColumns

    console.print(
        RichColumns(
            [
                Panel(
                    facts,
                    title=f"[bold {_O}]Sensor & Data[/bold {_O}]",
                    border_style=_S,
                    padding=(1, 2),
                    expand=False,
                ),
                Panel(
                    sp_table,
                    title=f"[bold {_O}]Species[/bold {_O}]",
                    border_style=_S,
                    padding=(1, 1),
                    expand=False,
                ),
            ],
            equal=False,
            expand=False,
        )
    )

    # ── Notes ────────────────────────────────────────────────────────────────
    if src.notes:
        notes = Text()
        for note in src.notes:
            notes.append("  • ", style=_T)
            notes.append(note + "\n", style=_D)
        console.print(
            Panel(
                notes,
                title=f"[bold {_O}]Notes[/bold {_O}]",
                border_style=_D,
                padding=(0, 1),
                width=74,
            )
        )

    console.print()


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------


def _render_summary(store: DetectionStore) -> None:
    summary = store.summary()
    if summary.is_empty():
        log.warning("No confident detections (>5%%) found.")
        return

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style=f"bold white on {_S}",
        border_style=_S,
        padding=(0, 1),
    )
    table.add_column("Species", style=f"bold {_O}", min_width=20)
    table.add_column("Scientific name", style=f"italic {_D}", min_width=26)
    table.add_column("Code", style=_D, justify="center")
    table.add_column("Windows", style=_T, justify="right")
    table.add_column("Time detected", style=_T, justify="right")
    table.add_column("Max conf", style="green", justify="right")
    table.add_column("Mean conf", style="green", justify="right")

    for row in summary.iter_rows(named=True):
        code = row["species"]
        table.add_row(
            display_name(code),
            scientific_name(code),
            code,
            f"{row['windows']:,}",
            f"{row['minutes_detected']:.1f} min",
            f"{row['max_conf']:.1%}",
            f"{row['mean_conf']:.1%}",
        )

    console.print()
    console.print(
        Panel(
            table,
            title=f"[bold {_O}]Detection Summary[/bold {_O}]  [{_D}]rank-1 · confidence > 50%[/{_D}]",
            border_style=_S,
            padding=(1, 2),
            expand=False,
        )
    )


# ---------------------------------------------------------------------------
# Analyze command
# ---------------------------------------------------------------------------

_BARS = " ▁▂▃▄▅▆▇█"


def _bar(rate: float, max_rate: float) -> str:
    if max_rate == 0:
        return " "
    return _BARS[int(rate / max_rate * (len(_BARS) - 1))]


def cmd_analyze(args: argparse.Namespace) -> None:
    import polars as pl

    files = sorted(Path(args.input_dir).glob("*.parquet"))
    if not files:
        log.error("No Parquet files found in %s", args.input_dir)
        sys.exit(1)

    log.info(
        "Loading %d Parquet files from [dim]%s[/dim]...", len(files), args.input_dir
    )
    df = pl.concat([pl.read_parquet(f) for f in files])
    df = add_timestamps(df)

    n_windows = df.filter(pl.col("rank") == 1).shape[0]
    n_detections = df.filter(
        (pl.col("rank") == 1) & (pl.col("confidence") >= 0.5)
    ).shape[0]
    dates = df["date"].unique().sort().to_list()

    console.print()
    console.print(Rule(f"[bold {_O}]Detection Analysis[/bold {_O}]", style=_S))

    # ── Overview ────────────────────────────────────────────────────────────
    meta = Table.grid(padding=(0, 3))
    meta.add_column(style=_D, no_wrap=True)
    meta.add_column(style="white", no_wrap=True)
    meta.add_row("Files", str(len(files)))
    meta.add_row("Date range", f"{dates[0]}  →  {dates[-1]}")
    meta.add_row("Total windows", f"{n_windows:,}")
    meta.add_row("Detections", f"{n_detections:,}  (sigmoid ≥ 0.5, rank-1)")
    console.print(Panel(meta, border_style=_D, padding=(0, 2), width=60))
    console.print()

    # ── Species summary ──────────────────────────────────────────────────────
    summary = species_summary(df)
    sp_table = Table(
        box=box.ROUNDED,
        border_style=_S,
        header_style=f"bold white on {_S}",
        padding=(0, 1),
    )
    sp_table.add_column("Species", style=f"bold {_O}")
    sp_table.add_column("Scientific", style=f"italic {_D}", min_width=24)
    sp_table.add_column("Windows", style=_T, justify="right")
    sp_table.add_column("% of time", style=_T, justify="right")
    sp_table.add_column("Min detected", style=_T, justify="right")
    sp_table.add_column("Mean conf", style="green", justify="right")
    sp_table.add_column("Max conf", style="green", justify="right")

    for row in summary.iter_rows(named=True):
        code = row["species"]
        sp_table.add_row(
            display_name(code),
            scientific_name(code),
            f"{row['windows']:,}",
            f"{row['pct_of_time']:.1f}%",
            f"{row['minutes']:.0f}",
            f"{row['mean_conf']:.1%}",
            f"{row['max_conf']:.1%}",
        )
    console.print(
        Panel(
            sp_table,
            title=f"[bold {_O}]Species Summary[/bold {_O}]",
            border_style=_S,
            padding=(1, 2),
            expand=False,
        )
    )
    console.print()

    # ── Hourly activity heatmap ──────────────────────────────────────────────
    hourly, top_species = hourly_activity(df, top_n=args.top_n)
    _raw_max = (
        max((hourly[sp].max() or 0.0) for sp in top_species if sp in hourly.columns)
        or 1.0
    )
    global_max = float(_raw_max)  # type: ignore[arg-type]

    heat = Table(
        box=box.SIMPLE_HEAD, border_style=_D, padding=(0, 1), header_style=f"bold {_O}"
    )
    heat.add_column("Hour", style=_D, justify="right", no_wrap=True)
    for sp in top_species:
        heat.add_column(display_name(sp), justify="center", min_width=12)

    for row in hourly.iter_rows(named=True):
        h = int(row["hour"])
        cells = []
        for sp in top_species:
            rate = float(row.get(sp) or 0.0)
            bar = _bar(rate, global_max)
            cells.append(f"[{_T}]{bar}[/{_T}] [{_D}]{rate:.0f}%[/{_D}]")
        heat.add_row(f"{h:02d}:00", *cells)

    console.print(
        Panel(
            heat,
            title=f"[bold {_O}]Hourly Activity[/bold {_O}]  [{_D}]detection rate · rank-1 · conf ≥ 50%[/{_D}]",
            border_style=_S,
            padding=(1, 2),
            expand=False,
        )
    )
    console.print()

    # ── Daily counts ─────────────────────────────────────────────────────────
    top_sp = summary.head(args.top_n)["species"].to_list()
    daily = daily_counts(df)

    day_table = Table(
        box=box.SIMPLE_HEAD, border_style=_D, padding=(0, 1), header_style=f"bold {_O}"
    )
    day_table.add_column("Date", style=_D, no_wrap=True)
    for sp in top_sp:
        day_table.add_column(display_name(sp), justify="right", style=_T)

    for date in dates:
        row_data = daily.filter(pl.col("date") == date)
        cells = []
        for sp in top_sp:
            match = row_data.filter(pl.col("species") == sp)
            n = int(match["windows"][0]) if len(match) else 0
            cells.append(str(n) if n else "[dim]·[/dim]")
        day_table.add_row(str(date), *cells)

    console.print(
        Panel(
            day_table,
            title=f"[bold {_O}]Daily Detections[/bold {_O}]",
            border_style=_S,
            padding=(1, 2),
            expand=False,
        )
    )
    console.print()


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

_O = "orange3"  # warm amber          — commands / primary
_S = "sky_blue3"  # muted sky blue      — borders / whale body
_T = "light_cyan3"  # soft cyan           — whale water / accents
_A = "pale_green1"  # bright pale green   — arguments / flags (contrast)
_D = "grey66"  # medium grey         — dim / descriptions


def _whale_text() -> Text:
    t = Text()
    t.append("      ________\n", style=f"bold {_O}")
    t.append("  .~~          ~~.\n", style=_S)
    t.append(" /   ", style=_S)
    t.append("(o)", style="bold white")
    t.append("           \\\n", style=_S)
    t.append("|                `----._\n", style=_S)
    t.append(" \\___________________", style=_S)
    t.append(".--~\n", style=f"bold {_O}")
    t.append("   ~~ ~~ ~~  ~~ ~~ ~~", style=_T)
    return t


def _title_text() -> Text:
    t = Text()
    t.append("\n")
    t.append("  whalu\n", style=f"bold {_O}")
    t.append("\n")
    t.append("  marine bioacoustics\n", style="white")
    t.append("  Perch v2  ·  Gemma 4\n", style=_D)
    return t


def _print_banner() -> None:
    from rich.columns import Columns as RichColumns

    console.print()
    console.print(
        Panel(
            RichColumns([_whale_text(), _title_text()], equal=False, expand=False),
            border_style=_S,
            padding=(1, 3),
            width=64,
        )
    )
    _CMDS = [
        (
            True,
            "whalu scan mbari",
            "--start 2026-03",
            "detect whales · MBARI Pacific Sound",
        ),
        (
            False,
            "",
            "  --max-files 1 --limit-hours 1",
            "↳ single-day test (~172 MB/file)",
        ),
        (
            True,
            "whalu scan mbari",
            "--start 2023-07 --end 2023-10",
            "scan a date range",
        ),
        (True, "whalu scan orcasound", "", "detect whales · Orcasound / Puget Sound"),
        (True, "whalu info mbari", "", "sensor, species, S3 coverage"),
        (True, "whalu info orcasound", "", "Orcasound network info"),
    ]
    lines: list[Text] = []
    for is_cmd, cmd, flags, desc in _CMDS:
        t = Text()
        if is_cmd:
            t.append("  $ ", style="dim")
            t.append(cmd, style=f"bold {_O}")
            if flags:
                t.append(" " + flags, style=_A)
        else:
            t.append("    " + flags, style=_A)
        t.append("  " + desc, style=_D)
        lines.append(t)

    block = Text("\n").join(lines)
    console.print()
    console.print(block)
    console.print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="whalu",
        description="Marine bioacoustics detection  Perch v2 + Gemma 4.",
        add_help=True,
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Enable DEBUG logging")
    sub = p.add_subparsers(dest="command")

    # ── scan ──────────────────────────────────────────────────────────────────
    scan_p = sub.add_parser("scan", help="Run detection over an audio archive")
    scan_sub = scan_p.add_subparsers(dest="source", required=True)

    m = scan_sub.add_parser(
        "mbari", help="MBARI Pacific Sound (s3://pacific-sound-16khz)"
    )
    m.add_argument("--start", required=True, metavar="YYYY-MM")
    m.add_argument(
        "--end",
        metavar="YYYY-MM",
        help="End year-month inclusive (default: same as --start)",
    )
    m.add_argument(
        "--max-files",
        type=int,
        default=None,
        metavar="N",
        help="Only process the first N files (default: all)",
    )
    m.add_argument(
        "--limit-hours",
        type=float,
        default=None,
        metavar="N",
        help="Only process the first N hours of each file (default: full file)",
    )
    m.add_argument("--output-dir", default="data/detections/mbari")

    o = scan_sub.add_parser("orcasound", help="Orcasound (s3://acoustic-sandbox)")
    o.add_argument(
        "--key",
        metavar="S3_KEY",
        help="Specific S3 key (default: labeled killer whale sample)",
    )
    o.add_argument("--output-dir", default="data/detections/orcasound")

    # ── analyze ───────────────────────────────────────────────────────────────
    az = sub.add_parser("analyze", help="Analyze detection Parquet files")
    az.add_argument(
        "--input-dir",
        default="data/detections/mbari",
        help="Directory of detection Parquet files",
    )
    az.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Top N species to show in heatmap (default: 5)",
    )

    # ── info ──────────────────────────────────────────────────────────────────
    info_p = sub.add_parser("info", help="Show sensor/dataset info for a source")
    info_p.add_argument(
        "info_source",
        nargs="?",
        choices=list(SOURCE_REGISTRY.keys()),
        metavar="SOURCE",
        help=f"One of: {', '.join(SOURCE_REGISTRY)}  (omit to show all)",
    )

    return p


def main() -> None:
    args = build_parser().parse_args()
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    if not args.command:
        _print_banner()
        return

    if args.command == "analyze":
        cmd_analyze(args)

    elif args.command == "scan":
        if args.source == "mbari":
            cmd_mbari(args)
        elif args.source == "orcasound":
            cmd_orcasound(args)
        else:
            log.error("Unknown source: %s", args.source)
            sys.exit(1)

    elif args.command == "info":
        cmd_info(args)


if __name__ == "__main__":
    main()
