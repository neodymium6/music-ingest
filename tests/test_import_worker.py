from __future__ import annotations

import sqlite3
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from music_ingest.domain import JobMode, JobStatus
from music_ingest.infra.db import open_db, set_job_running
from music_ingest.services.imports import (
    DuplicateActiveJobError,
    ImportService,
    normalize_release_ref,
)
from music_ingest.worker.executor import ImportWorker, start_worker


class FakeBeetsRunner:
    def __init__(
        self,
        *,
        preview_returncode: int = 0,
        preview_stdout: str = "",
        preview_stderr: str = "",
        run_returncode: int = 0,
        run_stdout: str = "",
        run_stderr: str = "",
        preview_exception: Exception | None = None,
        run_exception: Exception | None = None,
    ) -> None:
        self.preview_result = subprocess.CompletedProcess(
            ("beet", "import", "--pretend"), preview_returncode, preview_stdout, preview_stderr
        )
        self.run_result = subprocess.CompletedProcess(
            ("beet", "import"), run_returncode, run_stdout, run_stderr
        )
        self.preview_exception = preview_exception
        self.run_exception = run_exception
        self.calls: list[tuple[str, Path, str | None]] = []

    def preview_as_is(self, album_dir: Path) -> subprocess.CompletedProcess[str]:
        self.calls.append(("preview_as_is", album_dir, None))
        if self.preview_exception is not None:
            raise self.preview_exception
        return self.preview_result

    def run_as_is(self, album_dir: Path) -> subprocess.CompletedProcess[str]:
        self.calls.append(("run_as_is", album_dir, None))
        if self.run_exception is not None:
            raise self.run_exception
        return self.run_result

    def preview_release(
        self, album_dir: Path, release_ref: str
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(("preview_release", album_dir, release_ref))
        if self.preview_exception is not None:
            raise self.preview_exception
        return self.preview_result

    def run_release(self, album_dir: Path, release_ref: str) -> subprocess.CompletedProcess[str]:
        self.calls.append(("run_release", album_dir, release_ref))
        if self.run_exception is not None:
            raise self.run_exception
        return self.run_result


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = open_db(tmp_path / "app.db")
    try:
        yield db
    finally:
        db.close()


def test_import_service_normalizes_release_ref_and_blocks_duplicate_active_jobs(
    connection: sqlite3.Connection,
) -> None:
    service = ImportService(connection)
    album_dir = Path("/music/incoming/Unknown Artist/Unknown Album")

    job = service.enqueue_release(
        album_dir,
        "https://musicbrainz.org/release/12345678-1234-1234-1234-123456789ABC",
    )

    assert job.mode is JobMode.RELEASE
    assert job.release_ref == "12345678-1234-1234-1234-123456789abc"

    with pytest.raises(DuplicateActiveJobError):
        service.enqueue_as_is(album_dir)


def test_import_service_reraises_unrelated_integrity_errors(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = ImportService(connection)
    album_dir = Path("/music/incoming/Artist/Album")
    service.enqueue_as_is(album_dir)

    def raise_unrelated_integrity_error(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.IntegrityError("CHECK constraint failed: jobs")

    monkeypatch.setattr("music_ingest.services.imports.create_job", raise_unrelated_integrity_error)

    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        service.enqueue_as_is(album_dir)


def test_import_service_normalizes_release_url_host_case_and_port() -> None:
    normalized = normalize_release_ref(
        "https://MusicBrainz.org:443/release/12345678-1234-1234-1234-123456789ABC"
    )

    assert normalized == "12345678-1234-1234-1234-123456789abc"


def test_worker_marks_preview_failures_without_running_import(
    connection: sqlite3.Connection,
) -> None:
    service = ImportService(connection)
    job = service.enqueue_as_is(Path("/music/incoming/Artist/Album"))
    runner = FakeBeetsRunner(
        preview_returncode=1, preview_stdout="preview", preview_stderr="failed"
    )
    worker = ImportWorker(connection, runner)

    result = worker.run_next_pending()

    assert result is not None
    assert result.id == job.id
    assert result.status is JobStatus.FAILED
    assert result.preview_exit_code == 1
    assert result.preview_stdout == "preview"
    assert result.preview_stderr == "failed"
    assert result.run_stdout is None
    assert result.run_stderr is None
    assert runner.calls == [("preview_as_is", Path("/music/incoming/Artist/Album"), None)]


def test_worker_fails_job_when_preview_raises(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    job = service.enqueue_as_is(Path("/music/incoming/Artist/Album"))
    worker = ImportWorker(connection, FakeBeetsRunner(preview_exception=TimeoutError("timed out")))

    result = worker.run_next_pending()

    assert result is not None
    assert result.id == job.id
    assert result.status is JobStatus.FAILED
    assert result.run_stderr == "beets preview failed: TimeoutError('timed out')"


def test_worker_fails_job_when_import_raises(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    job = service.enqueue_as_is(Path("/music/incoming/Artist/Album"))
    worker = ImportWorker(connection, FakeBeetsRunner(run_exception=FileNotFoundError("beet")))

    result = worker.run_next_pending()

    assert result is not None
    assert result.id == job.id
    assert result.status is JobStatus.FAILED
    assert result.preview_exit_code == 0
    assert result.run_stderr == "beets import failed: FileNotFoundError('beet')"


def test_start_worker_reconciles_stale_running_jobs(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    job = service.enqueue_as_is(Path("/music/incoming/Artist/Album"))
    set_job_running(connection, job.id)

    worker = start_worker(connection, FakeBeetsRunner())
    reconciled = service.get_job(job.id)

    assert isinstance(worker, ImportWorker)
    assert reconciled is not None
    assert reconciled.status is JobStatus.FAILED
    assert reconciled.run_stderr == "Application restarted before job completion"


def test_worker_runs_release_job_after_successful_preview(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    job = service.enqueue_release(
        Path("/music/incoming/Unknown Artist/Unknown Album"),
        "https://musicbrainz.org/release/12345678-1234-1234-1234-123456789ABC",
    )
    runner = FakeBeetsRunner(run_stdout="imported", run_stderr="")
    worker = ImportWorker(connection, runner)

    result = worker.run_next_pending()

    assert result is not None
    assert result.id == job.id
    assert result.status is JobStatus.SUCCEEDED
    assert result.run_exit_code == 0
    assert result.run_stdout == "imported"
    assert runner.calls == [
        (
            "preview_release",
            Path("/music/incoming/Unknown Artist/Unknown Album"),
            "12345678-1234-1234-1234-123456789abc",
        ),
        (
            "run_release",
            Path("/music/incoming/Unknown Artist/Unknown Album"),
            "12345678-1234-1234-1234-123456789abc",
        ),
    ]


def test_worker_run_job_claims_pending_job_by_id(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    job = service.enqueue_as_is(Path("/music/incoming/Artist/Album"))
    runner = FakeBeetsRunner(run_stdout="imported")
    worker = ImportWorker(connection, runner)

    result = worker.run_job(job.id)

    assert result.id == job.id
    assert result.status is JobStatus.SUCCEEDED
    assert runner.calls == [
        ("preview_as_is", Path("/music/incoming/Artist/Album"), None),
        ("run_as_is", Path("/music/incoming/Artist/Album"), None),
    ]


def test_run_next_pending_claims_oldest_job_first(connection: sqlite3.Connection) -> None:
    service = ImportService(connection)
    oldest = service.enqueue_as_is(Path("/music/incoming/Artist/Old"))
    newest = service.enqueue_as_is(Path("/music/incoming/Artist/New"))
    runner = FakeBeetsRunner()
    worker = ImportWorker(connection, runner)

    result = worker.run_next_pending()
    remaining = service.get_job(newest.id)

    assert result is not None
    assert result.id == oldest.id
    assert remaining is not None
    assert remaining.status is JobStatus.PENDING
