from __future__ import annotations

import logging

from rich.logging import RichHandler


def setup_logging(level: str = "INFO", *, rich_tracebacks: bool = True) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=rich_tracebacks)],
        force=True,
    )
