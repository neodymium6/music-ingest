from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

from music_ingest.config.loader import load_settings
from music_ingest.config.schema import Settings
from music_ingest.infra.beets_runner import BeetsRunner
from music_ingest.infra.db import open_db
from music_ingest.infra.logging import setup_logging
from music_ingest.services import ImportService
from music_ingest.worker import ImportWorker, start_worker


@dataclass(slots=True)
class BootstrapContext:
    settings: Settings
    connection: sqlite3.Connection
    beets_runner: BeetsRunner
    import_service: ImportService
    worker: ImportWorker


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

    connection = open_db(settings.db.path, wal=settings.db.wal)
    beets_runner = BeetsRunner(
        executable=settings.beets.executable,
        beetsdir=settings.beets.beetsdir,
        config_file=settings.beets.config_file,
        timeout_seconds=settings.beets.timeout_seconds,
    )
    import_service = ImportService(connection)
    worker = start_worker(connection, beets_runner)

    return BootstrapContext(
        settings=settings,
        connection=connection,
        beets_runner=beets_runner,
        import_service=import_service,
        worker=worker,
    )
