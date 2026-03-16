from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from music_ingest.domain import JobMode, JobStatus
from music_ingest.infra.db import (
    SCHEMA_VERSION,
    create_job,
    fail_running_jobs,
    get_job,
    list_jobs,
    open_db,
    record_job_preview,
    set_job_failed,
    set_job_running,
)


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = open_db(tmp_path / "app.db")
    try:
        yield db
    finally:
        db.close()


def test_open_db_bootstraps_schema_and_version(connection: sqlite3.Connection) -> None:
    user_version = connection.execute("PRAGMA user_version;").fetchone()[0]
    journal_mode = connection.execute("PRAGMA journal_mode;").fetchone()[0]
    object_names = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index');"
        ).fetchall()
    }

    assert user_version == SCHEMA_VERSION
    assert journal_mode.lower() == "wal"
    assert "jobs" in object_names
    assert "idx_jobs_status_created_at" in object_names
    assert "idx_jobs_album_dir_pending_running" in object_names


def test_jobs_enforce_release_ref_constraints(connection: sqlite3.Connection) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        create_job(
            connection,
            job_id="job-as-is-invalid",
            album_dir=Path("/music/incoming/Artist/Album"),
            mode=JobMode.AS_IS,
            release_ref="should-not-exist",
        )

    with pytest.raises(sqlite3.IntegrityError):
        create_job(
            connection,
            job_id="job-release-invalid",
            album_dir=Path("/music/incoming/Artist/Album"),
            mode=JobMode.RELEASE,
            release_ref=None,
        )


def test_jobs_block_duplicate_pending_or_running_album_dirs(connection: sqlite3.Connection) -> None:
    album_dir = Path("/music/incoming/Artist/Album")

    create_job(connection, job_id="job-1", album_dir=album_dir, mode=JobMode.AS_IS)

    with pytest.raises(sqlite3.IntegrityError):
        create_job(connection, job_id="job-2", album_dir=album_dir, mode=JobMode.AS_IS)

    set_job_failed(connection, "job-1", run_stderr="preview failed")
    created = create_job(connection, job_id="job-3", album_dir=album_dir, mode=JobMode.AS_IS)

    assert created.id == "job-3"
    assert created.status is JobStatus.PENDING


def test_job_repository_helpers_round_trip(connection: sqlite3.Connection) -> None:
    created = create_job(
        connection,
        job_id="job-1",
        album_dir=Path("/music/incoming/Unknown_Artist/Unknown_Album"),
        mode=JobMode.RELEASE,
        release_ref="https://musicbrainz.org/release/test-release",
    )

    assert created.status is JobStatus.PENDING

    running = set_job_running(connection, "job-1")
    assert running.status is JobStatus.RUNNING

    previewed = record_job_preview(connection, "job-1", exit_code=0, stdout="ok", stderr="")
    assert previewed.status is JobStatus.RUNNING
    assert previewed.preview_exit_code == 0
    assert previewed.preview_stdout == "ok"

    reconciled = fail_running_jobs(connection, message="Application restarted")
    assert reconciled == 1

    reloaded = get_job(connection, "job-1")
    assert reloaded is not None
    assert reloaded.status is JobStatus.FAILED
    assert reloaded.run_stderr == "Application restarted"

    listed = list_jobs(connection)
    assert [job.id for job in listed] == ["job-1"]


def test_record_job_preview_allows_idempotent_updates(connection: sqlite3.Connection) -> None:
    create_job(
        connection,
        job_id="job-1",
        album_dir=Path("/music/incoming/Artist/Album"),
        mode=JobMode.AS_IS,
    )
    set_job_running(connection, "job-1")

    first = record_job_preview(connection, "job-1", exit_code=0, stdout="ok", stderr="")
    second = record_job_preview(connection, "job-1", exit_code=0, stdout="ok", stderr="")

    assert first.preview_exit_code == 0
    assert second.preview_exit_code == 0
    assert second.preview_stdout == "ok"
