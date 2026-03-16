from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
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

    def close(self) -> None: ...


@dataclass(slots=True)
class MusicIngestApp:
    settings: Settings
    import_service: ImportService
    worker: WorkerProtocol
    _worker_lock: Lock = field(init=False, repr=False)
    _worker_executor: ThreadPoolExecutor = field(init=False, repr=False)
    _job_snapshot: list[Job] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._worker_lock = Lock()
        self._worker_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ingest-ui")
        self._job_snapshot = self.import_service.list_jobs(limit=200)

    @property
    def incoming_root(self) -> Path:
        return self.settings.paths.incoming_root

    def list_incoming_albums(self) -> list[IncomingAlbum]:
        return scan_incoming_albums(self.incoming_root)

    def list_jobs(self, *, limit: int = 100) -> list[Job]:
        return self.import_service.list_jobs(limit=limit)

    def current_job_snapshot(self) -> list[Job]:
        return list(self._job_snapshot)

    def refresh_job_snapshot(self, *, limit: int = 200) -> list[Job]:
        self._job_snapshot = self.import_service.list_jobs(limit=limit)
        return list(self._job_snapshot)

    def enqueue_as_is(self, album_dir: Path) -> Job:
        job = self.import_service.enqueue_as_is(album_dir)
        self.refresh_job_snapshot()
        return job

    def enqueue_release(self, album_dir: Path, release_ref: str) -> Job:
        job = self.import_service.enqueue_release(album_dir, release_ref)
        self.refresh_job_snapshot()
        return job

    async def run_pending_jobs(self) -> Job | None:
        if not self._worker_lock.acquire(blocking=False):
            return None
        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self._worker_executor, self.worker.run_next_pending)
            if result is not None:
                self.refresh_job_snapshot()
            return result
        except Exception:
            logger.exception("Failed while processing a queued import job")
            return None
        finally:
            self._worker_lock.release()

    def shutdown(self) -> None:
        future = self._worker_executor.submit(self.worker.close)
        future.result()
        self._worker_executor.shutdown(wait=True)


def register_ui(app: MusicIngestApp) -> None:
    ui.timer(1.0, app.run_pending_jobs)
    ui.timer(2.0, app.refresh_job_snapshot)
    register_incoming_page(app)
    register_jobs_page(app)


def run_ui(settings: Settings) -> None:
    ui.run(
        host=settings.app.host,
        port=settings.app.port,
        title=settings.app.title,
        reload=False,
    )
