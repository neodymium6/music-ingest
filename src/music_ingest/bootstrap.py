from __future__ import annotations

import logging
import shutil
import sqlite3
from dataclasses import dataclass

from music_ingest.config.loader import load_settings
from music_ingest.config.schema import Settings
from music_ingest.infra.beets_runner import BeetsRunner
from music_ingest.infra.db import open_db
from music_ingest.infra.logging import setup_logging
from music_ingest.services import ImportService
from music_ingest.worker import reconcile_stale_jobs


@dataclass(slots=True)
class BootstrapContext:
    settings: Settings
    connection: sqlite3.Connection
    beets_runner: BeetsRunner
    import_service: ImportService


def bootstrap() -> BootstrapContext:
    settings = load_settings()
    setup_logging(
        level=settings.logging.level,
        rich_tracebacks=settings.logging.rich_tracebacks,
        logs_root=settings.paths.logs_root,
    )
    logging.getLogger(__name__).info(
        "Loaded settings for %s on %s:%s",
        settings.app.title,
        settings.app.host,
        settings.app.port,
    )

    _validate_environment(settings)

    connection = open_db(settings.db.path, wal=settings.db.wal)
    beets_runner = BeetsRunner(
        executable=settings.beets.executable,
        beetsdir=settings.beets.beetsdir,
        config_file=settings.beets.config_file,
        timeout_seconds=settings.beets.timeout_seconds,
    )
    import_service = ImportService(connection)
    reconcile_stale_jobs(connection)

    return BootstrapContext(
        settings=settings,
        connection=connection,
        beets_runner=beets_runner,
        import_service=import_service,
    )


def _validate_environment(settings: Settings) -> None:
    log = logging.getLogger(__name__)

    if not settings.paths.incoming_root.is_dir():
        raise RuntimeError(
            f"incoming_root does not exist or is not a directory: {settings.paths.incoming_root}"
        )
    log.debug("incoming_root OK: %s", settings.paths.incoming_root)

    if not settings.beets.beetsdir.is_dir():
        raise RuntimeError(
            f"beets beetsdir does not exist or is not a directory: {settings.beets.beetsdir}"
        )
    log.debug("beets beetsdir OK: %s", settings.beets.beetsdir)

    if not settings.beets.config_file.is_file():
        raise RuntimeError(f"beets config_file does not exist: {settings.beets.config_file}")
    log.debug("beets config_file OK: %s", settings.beets.config_file)

    if shutil.which(settings.beets.executable) is None:
        raise RuntimeError(f"beets executable not found on PATH: {settings.beets.executable!r}")
    log.debug("beets executable OK: %s", settings.beets.executable)
