from __future__ import annotations

import logging
from dataclasses import dataclass

from music_ingest.config.loader import load_settings
from music_ingest.config.schema import Settings
from music_ingest.infra.logging import setup_logging


@dataclass(slots=True)
class BootstrapContext:
    settings: Settings


def bootstrap() -> BootstrapContext:
    settings = load_settings()
    setup_logging(
        level=settings.logging.level,
        rich_tracebacks=settings.logging.rich_tracebacks,
    )
    logging.getLogger(__name__).info(
        "Loaded settings for %s on %s:%s",
        settings.app.title,
        settings.app.host,
        settings.app.port,
    )
    return BootstrapContext(settings=settings)
