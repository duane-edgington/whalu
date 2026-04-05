"""Metadata about each supported hydrophone data source."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceInfo:
    id: str
    name: str
    operator: str
    location: str
    coordinates: str
    depth_m: int
    detection_range_km: str
    sample_rate_hz: int
    bit_depth: int
    coverage: str          # date range
    volume: str            # approximate total size
    file_format: str
    s3_bucket: str
    s3_path_format: str
    description: str
    habitat: str
    species: list[str]
    notes: list[str]


MBARI = SourceInfo(
    id="mbari",
    name="MBARI Pacific Sound",
    operator="Monterey Bay Aquarium Research Institute (MBARI)",
    location="Monterey Canyon, California, ~32 km offshore from Moss Landing",
    coordinates="36.713°N, 122.187°W",
    depth_m=891,
    detection_range_km="tens to hundreds",
    sample_rate_hz=16_000,
    bit_depth=24,
    coverage="2015 – present (continuous)",
    volume="~140 TB",
    file_format="16kHz 24-bit mono PCM WAV, one file per day (4.1 GB each)",
    s3_bucket="pacific-sound-16khz",
    s3_path_format="s3://pacific-sound-16khz/YYYY/MM/MARS-YYYYMMDDTHHMMSSZ-16kHz.wav",
    description=(
        "MBARI's MARS (Monterey Accelerated Research System) hydrophone sits on the "
        "seafloor at 891 m depth in the heart of Monterey Canyon, one of the richest "
        "whale feeding areas on the US West Coast. It is a fixed deep-water observatory "
        "that has been recording continuously since 2015. Sound propagates efficiently "
        "through deep water (the SOFAR channel), so detections may include animals tens "
        "to hundreds of kilometres away, not only those passing directly overhead."
    ),
    habitat="Deep submarine canyon, open Pacific Ocean",
    species=[
        "Blue whale (Bm)",
        "Fin whale (Bp)",
        "Humpback whale (Mn)",
        "Minke whale (Ba)",
        "Bryde's whale (Be)",
        "Sei whale (Bs)",
        "North Atlantic right whale (Eg)",
        "Orca (Oo)",
    ],
    notes=[
        "No authentication required. Public S3 bucket.",
        "Files are 4.1 GB each (24 h). Use --limit-hours for testing.",
        "Google ran multispecies_whale over 200 k+ h of NOAA data internally "
        "but never published a queryable database. This project fills that gap.",
    ],
)

ORCASOUND = SourceInfo(
    id="orcasound",
    name="Orcasound",
    operator="Orcasound open-source network",
    location="Puget Sound, Washington State (multiple hydrophone nodes)",
    coordinates="~47.8°N, 122.4°W (Orcasound Lab node)",
    depth_m=30,
    detection_range_km="1–10",
    sample_rate_hz=20_000,
    bit_depth=16,
    coverage="2017 – present (with gaps)",
    volume="~5 TB (archived segments)",
    file_format="20kHz 16-bit mono WAV",
    s3_bucket="acoustic-sandbox",
    s3_path_format="s3://acoustic-sandbox/labeled-data/...",
    description=(
        "A citizen-science hydrophone network in Puget Sound designed to monitor "
        "Southern Resident killer whales (SRKW), one of the most endangered orca "
        "populations on Earth (~73 individuals). Orcasound nodes are shallow-water "
        "near-shore sensors with a much shorter detection range than MBARI, but with "
        "human-labeled ground-truth data ideal for model validation."
    ),
    habitat="Inland sea, shallow near-shore coastal waters",
    species=[
        "Orca / killer whale (Oo) - primary target",
        "Humpback whale (Mn) - occasional",
    ],
    notes=[
        "No authentication required. Public S3 bucket.",
        "Labeled ground-truth data available for model validation.",
        "Southern Resident killer whales are critically endangered (~73 individuals).",
        "Live stream available at orcasound.net.",
    ],
)

REGISTRY: dict[str, SourceInfo] = {
    "mbari": MBARI,
    "orcasound": ORCASOUND,
}
