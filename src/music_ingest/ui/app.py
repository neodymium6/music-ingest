from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Protocol

from nicegui import app as nicegui_app
from nicegui import ui

from music_ingest.config.schema import Settings
from music_ingest.domain import DuplicateAction, IncomingAlbum, Job
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
    _polling_task: asyncio.Task[None] | None = field(init=False, repr=False)
    _is_shutdown: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._worker_lock = Lock()
        self._worker_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ingest-ui")
        self._job_snapshot = self.import_service.list_jobs(limit=200)
        self._polling_task = None
        self._is_shutdown = False

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

    def enqueue_as_is(
        self, album_dir: Path, duplicate_action: DuplicateAction = DuplicateAction.ABORT
    ) -> Job:
        job = self.import_service.enqueue_as_is(album_dir, duplicate_action)
        self.refresh_job_snapshot()
        return job

    def enqueue_release(
        self,
        album_dir: Path,
        release_ref: str,
        duplicate_action: DuplicateAction = DuplicateAction.ABORT,
    ) -> Job:
        job = self.import_service.enqueue_release(album_dir, release_ref, duplicate_action)
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
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed while processing a queued import job")
            return None
        finally:
            self._worker_lock.release()

    async def start_background_tasks(self) -> None:
        if self._polling_task is None:
            self._polling_task = asyncio.create_task(self._poll_worker_loop())

    async def stop_background_tasks(self) -> None:
        await self._stop_polling_task()
        await self.shutdown()

    async def shutdown(self) -> None:
        if self._is_shutdown:
            return
        await self._stop_polling_task()
        try:
            close_future = self._worker_executor.submit(self.worker.close)
            await asyncio.wrap_future(close_future)
        finally:
            try:
                await asyncio.to_thread(self._worker_executor.shutdown, wait=True)
            finally:
                self._is_shutdown = True

    async def _poll_worker_loop(self) -> None:
        while True:
            try:
                await self.run_pending_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unhandled exception in background worker loop")
            await asyncio.sleep(1.0)

    async def _stop_polling_task(self) -> None:
        polling_task = self._polling_task
        self._polling_task = None
        if polling_task is None:
            return
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Background polling task failed before shutdown")


def register_ui(app: MusicIngestApp) -> None:
    nicegui_app.on_startup(app.start_background_tasks)
    nicegui_app.on_shutdown(app.stop_background_tasks)
    register_incoming_page(app)
    register_jobs_page(app)


def run_ui(settings: Settings) -> None:
    ui.run(
        host=settings.app.host,
        port=settings.app.port,
        title=settings.app.title,
        reload=False,
        storage_secret="music-ingest",
    )
