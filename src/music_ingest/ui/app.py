from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Protocol

from nicegui import ui

from music_ingest.config.schema import Settings
from music_ingest.domain import IncomingAlbum, Job
from music_ingest.infra.scanner import scan_incoming_albums
from music_ingest.services import ImportService
from music_ingest.ui.pages import register_incoming_page, register_jobs_page

logger = logging.getLogger(__name__)


class WorkerProtocol(Protocol):
    def run_next_pending(self) -> Job | None: ...


@dataclass(slots=True)
class MusicIngestApp:
    settings: Settings
    import_service: ImportService
    worker: WorkerProtocol
    _worker_lock: Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._worker_lock = Lock()

    @property
    def incoming_root(self) -> Path:
        return self.settings.paths.incoming_root

    def list_incoming_albums(self) -> list[IncomingAlbum]:
        return scan_incoming_albums(self.incoming_root)

    def list_jobs(self, *, limit: int = 100) -> list[Job]:
        return self.import_service.list_jobs(limit=limit)

    def enqueue_as_is(self, album_dir: Path) -> Job:
        return self.import_service.enqueue_as_is(album_dir)

    def enqueue_release(self, album_dir: Path, release_ref: str) -> Job:
        return self.import_service.enqueue_release(album_dir, release_ref)

    async def run_pending_jobs(self) -> Job | None:
        if not self._worker_lock.acquire(blocking=False):
            return None
        try:
            return await asyncio.to_thread(self.worker.run_next_pending)
        except Exception:
            logger.exception("Failed while processing a queued import job")
            return None
        finally:
            self._worker_lock.release()


def register_ui(app: MusicIngestApp) -> None:
    register_incoming_page(app)
    register_jobs_page(app)


def run_ui(settings: Settings) -> None:
    ui.run(
        host=settings.app.host,
        port=settings.app.port,
        title=settings.app.title,
        reload=False,
    )
