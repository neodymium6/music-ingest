from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from threading import Event

import pytest

from music_ingest.config.schema import Settings
from music_ingest.domain import Job
from music_ingest.infra.db import open_db
from music_ingest.services import ImportService
from music_ingest.ui import MusicIngestApp


class FakeWorker:
    def __init__(self) -> None:
        self.calls = 0

    def run_next_pending(self) -> None:
        self.calls += 1
        return None

    def close(self) -> None:
        return None


class BlockingWorker(FakeWorker):
    def __init__(self) -> None:
        super().__init__()
        self.started = Event()
        self.release = Event()

    def run_next_pending(self) -> None:
        self.calls += 1
        self.started.set()
        self.release.wait(timeout=1.0)
        return None


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = open_db(tmp_path / "app.db")
    try:
        yield db
    finally:
        db.close()


def test_music_ingest_app_lists_incoming_albums_and_jobs(
    connection: sqlite3.Connection, tmp_path: Path
) -> None:
    incoming_root = tmp_path / "incoming"
    album_dir = incoming_root / "Artist" / "Album"
    album_dir.mkdir(parents=True)
    (album_dir / "01 - Track.flac").write_text("", encoding="utf-8")

    settings = Settings()
    settings.paths.incoming_root = incoming_root
    service = ImportService(connection)
    created_job = service.enqueue_as_is(album_dir)
    app = MusicIngestApp(settings=settings, import_service=service, worker=FakeWorker())

    albums = app.list_incoming_albums()
    jobs = app.list_jobs()

    assert [album.album_name for album in albums] == ["Album"]
    assert jobs[0].id == created_job.id


def test_music_ingest_app_runs_pending_jobs_without_reentry(connection: sqlite3.Connection) -> None:
    worker = BlockingWorker()
    app = MusicIngestApp(
        settings=Settings(),
        import_service=ImportService(connection),
        worker=worker,
    )

    async def run_concurrently() -> tuple[Job | None, Job | None]:
        first = asyncio.create_task(app.run_pending_jobs())
        await asyncio.to_thread(worker.started.wait, 1.0)
        second = await app.run_pending_jobs()
        worker.release.set()
        return await first, second

    first_result, second_result = asyncio.run(run_concurrently())

    assert first_result is None
    assert second_result is None
    assert worker.calls == 1


def test_music_ingest_app_enqueue_release_uses_service_normalization(
    connection: sqlite3.Connection,
) -> None:
    app = MusicIngestApp(
        settings=Settings(),
        import_service=ImportService(connection),
        worker=FakeWorker(),
    )

    job = app.enqueue_release(
        Path("/music/incoming/Artist/Album"),
        "https://musicbrainz.org/release/12345678-1234-1234-1234-123456789ABC",
    )

    assert job.release_ref == "12345678-1234-1234-1234-123456789abc"
