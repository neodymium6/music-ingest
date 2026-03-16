from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

from rich.logging import RichHandler


def setup_logging(
    level: str = "INFO",
    *,
    rich_tracebacks: bool = True,
    logs_root: Path | None = None,
) -> None:
    handlers: list[logging.Handler] = [RichHandler(rich_tracebacks=rich_tracebacks)]

    if logs_root is not None:
        logs_root.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            logs_root / "app.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        handlers=handlers,
        force=True,
    )
