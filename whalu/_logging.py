"""Central logging configuration for whalu.

Call setup_logging() once at the CLI entry point. All other modules just do:

    import logging
    log = logging.getLogger(__name__)
"""

import logging

from rich.logging import RichHandler


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                tracebacks_show_locals=True,
                show_path=True,
                markup=True,
            )
        ],
    )
    # Suppress noisy third-party loggers
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("tensorflow").setLevel(logging.ERROR)
    logging.getLogger("absl").setLevel(logging.ERROR)
