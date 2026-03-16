from __future__ import annotations

import datetime
import logging
import logging.handlers
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def setup_logging(
    level: str = "INFO",
    *,
    rich_tracebacks: bool = True,
    logs_root: Path | None = None,
    timezone: str = "UTC",
) -> None:
    from rich.logging import RichHandler

    handlers: list[logging.Handler] = [RichHandler(rich_tracebacks=rich_tracebacks)]

    if logs_root is not None:
        logs_root.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            logs_root / "app.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(_make_formatter(timezone))
        handlers.append(file_handler)

    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        handlers=handlers,
        force=True,
    )


def _make_formatter(timezone: str) -> logging.Formatter:
    try:
        tz = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {timezone!r}") from exc

    class _TZFormatter(logging.Formatter):
        def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
            dt = datetime.datetime.fromtimestamp(record.created, tz=tz)
            return dt.strftime(datefmt or "%Y-%m-%dT%H:%M:%S%z")

    return _TZFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
