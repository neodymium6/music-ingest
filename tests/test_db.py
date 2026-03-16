from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import pytest

from music_ingest.domain import DuplicateAction, JobMode, JobStatus
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
    set_job_skipped,
    set_job_succeeded,
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

    set_job_running(connection, "job-1")

    with pytest.raises(sqlite3.IntegrityError):
        create_job(connection, job_id="job-2-running", album_dir=album_dir, mode=JobMode.AS_IS)

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


def test_set_job_succeeded_records_terminal_run_details(connection: sqlite3.Connection) -> None:
    create_job(
        connection,
        job_id="job-success",
        album_dir=Path("/music/incoming/Artist/Album"),
        mode=JobMode.AS_IS,
        created_at=datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc),
    )
    set_job_running(connection, "job-success")

    finished = set_job_succeeded(
        connection,
        "job-success",
        finished_at=datetime(2026, 3, 16, 12, 5, 0, tzinfo=timezone.utc),
        run_exit_code=0,
        run_stdout="imported",
        run_stderr="",
    )

    assert finished.status is JobStatus.SUCCEEDED
    assert finished.run_exit_code == 0
    assert finished.run_stdout == "imported"
    assert finished.run_stderr == ""
    assert finished.finished_at == datetime(2026, 3, 16, 12, 5, 0, tzinfo=timezone.utc)


def test_create_job_rejects_naive_datetimes(connection: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="Naive datetimes"):
        create_job(
            connection,
            job_id="job-naive",
            album_dir=Path("/music/incoming/Artist/Album"),
            mode=JobMode.AS_IS,
            created_at=datetime(2026, 3, 16, 12, 0, 0),
        )


def test_set_job_failed_keeps_run_output_null_by_default(connection: sqlite3.Connection) -> None:
    create_job(
        connection,
        job_id="job-1",
        album_dir=Path("/music/incoming/Artist/Album"),
        mode=JobMode.AS_IS,
        created_at=datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc),
    )

    failed = set_job_failed(connection, "job-1")

    assert failed.run_stdout is None
    assert failed.run_stderr is None


def test_open_db_migrates_v1_pending_running_index(tmp_path: Path) -> None:
    db_path = tmp_path / "migrate.db"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.executescript(
            """
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              album_dir TEXT NOT NULL,
              mode TEXT NOT NULL
                CHECK (mode IN ('as_is', 'release')),
              release_ref TEXT,
              status TEXT NOT NULL
                CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
              created_at TEXT NOT NULL,
              started_at TEXT,
              finished_at TEXT,
              preview_stdout TEXT,
              preview_stderr TEXT,
              preview_exit_code INTEGER,
              run_stdout TEXT,
              run_stderr TEXT,
              run_exit_code INTEGER,
              CHECK (
                (mode = 'as_is' AND release_ref IS NULL) OR
                (mode = 'release' AND release_ref IS NOT NULL)
              )
            );

            CREATE INDEX idx_jobs_status_created_at
            ON jobs(status, created_at);

            CREATE UNIQUE INDEX idx_jobs_album_dir_pending_running
            ON jobs(album_dir, status)
            WHERE status IN ('pending', 'running');

            PRAGMA user_version = 1;
            """
        )
        raw.execute(
            """
            INSERT INTO jobs (id, album_dir, mode, release_ref, status, created_at, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "job-1",
                "/music/incoming/Artist/Album",
                "as_is",
                None,
                "running",
                "2026-03-16T00:00:00+00:00",
                "2026-03-16T00:01:00+00:00",
            ),
        )
        raw.commit()
    finally:
        raw.close()

    migrated = open_db(db_path)
    try:
        user_version = migrated.execute("PRAGMA user_version;").fetchone()[0]
        assert user_version == SCHEMA_VERSION

        with pytest.raises(sqlite3.IntegrityError):
            create_job(
                migrated,
                job_id="job-2",
                album_dir=Path("/music/incoming/Artist/Album"),
                mode=JobMode.AS_IS,
            )
    finally:
        migrated.close()


def test_open_db_rejects_v1_migration_with_duplicate_active_album_dirs(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate-migrate.db"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.executescript(
            """
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              album_dir TEXT NOT NULL,
              mode TEXT NOT NULL
                CHECK (mode IN ('as_is', 'release')),
              release_ref TEXT,
              status TEXT NOT NULL
                CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
              created_at TEXT NOT NULL,
              started_at TEXT,
              finished_at TEXT,
              preview_stdout TEXT,
              preview_stderr TEXT,
              preview_exit_code INTEGER,
              run_stdout TEXT,
              run_stderr TEXT,
              run_exit_code INTEGER,
              CHECK (
                (mode = 'as_is' AND release_ref IS NULL) OR
                (mode = 'release' AND release_ref IS NOT NULL)
              )
            );

            CREATE INDEX idx_jobs_status_created_at
            ON jobs(status, created_at);

            CREATE UNIQUE INDEX idx_jobs_album_dir_pending_running
            ON jobs(album_dir, status)
            WHERE status IN ('pending', 'running');

            PRAGMA user_version = 1;
            """
        )
        raw.executemany(
            """
            INSERT INTO jobs (id, album_dir, mode, release_ref, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "job-1",
                    "/music/incoming/Artist/Album",
                    "as_is",
                    None,
                    "pending",
                    "2026-03-16T00:00:00+00:00",
                ),
                (
                    "job-2",
                    "/music/incoming/Artist/Album",
                    "as_is",
                    None,
                    "running",
                    "2026-03-16T00:01:00+00:00",
                ),
            ],
        )
        raw.commit()
    finally:
        raw.close()

    with pytest.raises(RuntimeError, match="multiple pending/running jobs"):
        open_db(db_path)


def test_open_db_rejects_existing_unversioned_jobs_table(tmp_path: Path) -> None:
    db_path = tmp_path / "unversioned.db"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY)")
        raw.execute("PRAGMA user_version = 0;")
        raw.commit()
    finally:
        raw.close()

    with pytest.raises(RuntimeError, match="schema version 0"):
        open_db(db_path)


def test_create_job_defaults_duplicate_action_to_abort(connection: sqlite3.Connection) -> None:
    job = create_job(
        connection,
        job_id="job-1",
        album_dir=Path("/music/incoming/Artist/Album"),
        mode=JobMode.AS_IS,
    )

    assert job.duplicate_action is DuplicateAction.ABORT


def test_create_job_stores_explicit_duplicate_action(connection: sqlite3.Connection) -> None:
    skip_job = create_job(
        connection,
        job_id="job-skip",
        album_dir=Path("/music/incoming/Artist/Album One"),
        mode=JobMode.AS_IS,
        duplicate_action=DuplicateAction.SKIP,
    )
    remove_job = create_job(
        connection,
        job_id="job-remove",
        album_dir=Path("/music/incoming/Artist/Album Two"),
        mode=JobMode.AS_IS,
        duplicate_action=DuplicateAction.REMOVE,
    )

    assert skip_job.duplicate_action is DuplicateAction.SKIP
    assert remove_job.duplicate_action is DuplicateAction.REMOVE


def test_set_job_skipped_records_terminal_run_details(connection: sqlite3.Connection) -> None:
    create_job(
        connection,
        job_id="job-1",
        album_dir=Path("/music/incoming/Artist/Album"),
        mode=JobMode.AS_IS,
        duplicate_action=DuplicateAction.SKIP,
        created_at=datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc),
    )
    set_job_running(connection, "job-1")

    skipped = set_job_skipped(
        connection,
        "job-1",
        finished_at=datetime(2026, 3, 16, 12, 5, 0, tzinfo=timezone.utc),
        run_exit_code=0,
        run_stdout="This album is already in the library!\nSkipping.",
        run_stderr="",
    )

    assert skipped.status is JobStatus.SKIPPED
    assert skipped.run_exit_code == 0
    assert skipped.finished_at == datetime(2026, 3, 16, 12, 5, 0, tzinfo=timezone.utc)


def test_open_db_migrates_v2_adds_duplicate_action_column(tmp_path: Path) -> None:
    db_path = tmp_path / "v2.db"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.executescript(
            """
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              album_dir TEXT NOT NULL,
              mode TEXT NOT NULL CHECK (mode IN ('as_is', 'release')),
              release_ref TEXT,
              status TEXT NOT NULL
                CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
              created_at TEXT NOT NULL,
              started_at TEXT,
              finished_at TEXT,
              preview_stdout TEXT,
              preview_stderr TEXT,
              preview_exit_code INTEGER,
              run_stdout TEXT,
              run_stderr TEXT,
              run_exit_code INTEGER,
              CHECK (
                (mode = 'as_is' AND release_ref IS NULL) OR
                (mode = 'release' AND release_ref IS NOT NULL)
              )
            );

            CREATE INDEX idx_jobs_status_created_at ON jobs(status, created_at);

            CREATE UNIQUE INDEX idx_jobs_album_dir_pending_running
            ON jobs(album_dir)
            WHERE status IN ('pending', 'running');

            PRAGMA user_version = 2;
            """
        )
        raw.execute(
            "INSERT INTO jobs (id, album_dir, mode, release_ref, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "job-1",
                "/music/incoming/Artist/Album",
                "as_is",
                None,
                "succeeded",
                "2026-03-16T00:00:00+00:00",
            ),
        )
        raw.commit()
    finally:
        raw.close()

    migrated = open_db(db_path)
    try:
        assert migrated.execute("PRAGMA user_version;").fetchone()[0] == SCHEMA_VERSION
        job = get_job(migrated, "job-1")
        assert job is not None
        assert job.duplicate_action is DuplicateAction.ABORT
    finally:
        migrated.close()


def test_list_jobs_rejects_non_positive_limit(connection: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        list_jobs(connection, limit=0)

    with pytest.raises(ValueError, match="positive integer"):
        list_jobs(connection, limit=-1)
