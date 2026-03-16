from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from music_ingest.domain import Job, JobMode, JobStatus

SCHEMA_VERSION = 1


def open_db(path: str | Path, *, wal: bool = True) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute(f"PRAGMA journal_mode = {'WAL' if wal else 'DELETE'};")

    apply_schema(connection)
    return connection


def apply_schema(connection: sqlite3.Connection) -> None:
    current_version = int(connection.execute("PRAGMA user_version;").fetchone()[0])
    if current_version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {current_version} is newer than supported {SCHEMA_VERSION}"
        )

    if current_version == SCHEMA_VERSION:
        return

    if current_version == 0:
        with connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
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

                CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at
                ON jobs(status, created_at);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_album_dir_pending_running
                ON jobs(album_dir, status)
                WHERE status IN ('pending', 'running');
                """
            )
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION};")
        return

    raise RuntimeError(f"Unsupported database schema version {current_version}")


def create_job(
    connection: sqlite3.Connection,
    *,
    job_id: str,
    album_dir: Path,
    mode: JobMode,
    release_ref: str | None = None,
    created_at: datetime | None = None,
) -> Job:
    created = created_at or datetime.now(timezone.utc)
    with connection:
        connection.execute(
            """
            INSERT INTO jobs (
              id, album_dir, mode, release_ref, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                str(album_dir),
                mode.value,
                release_ref,
                JobStatus.PENDING.value,
                _to_db_timestamp(created),
            ),
        )

    job = get_job(connection, job_id)
    if job is None:
        raise LookupError(f"Job was inserted but could not be loaded: {job_id}")
    return job


def get_job(connection: sqlite3.Connection, job_id: str) -> Job | None:
    row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_job(row) if row is not None else None


def list_jobs(connection: sqlite3.Connection, *, limit: int = 100) -> list[Job]:
    rows = connection.execute(
        """
        SELECT * FROM jobs
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_row_to_job(row) for row in rows]


def set_job_running(
    connection: sqlite3.Connection, job_id: str, *, started_at: datetime | None = None
) -> Job:
    started = started_at or datetime.now(timezone.utc)
    _update_job(
        connection,
        """
        UPDATE jobs
        SET status = ?, started_at = ?
        WHERE id = ?
        """,
        (JobStatus.RUNNING.value, _to_db_timestamp(started), job_id),
        job_id,
    )
    return _require_job(connection, job_id)


def record_job_preview(
    connection: sqlite3.Connection,
    job_id: str,
    *,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> Job:
    _update_job(
        connection,
        """
        UPDATE jobs
        SET preview_exit_code = ?, preview_stdout = ?, preview_stderr = ?
        WHERE id = ?
        """,
        (exit_code, stdout, stderr, job_id),
        job_id,
    )
    return _require_job(connection, job_id)


def set_job_succeeded(
    connection: sqlite3.Connection,
    job_id: str,
    *,
    finished_at: datetime | None = None,
    run_exit_code: int = 0,
    run_stdout: str = "",
    run_stderr: str = "",
) -> Job:
    finished = finished_at or datetime.now(timezone.utc)
    _update_job(
        connection,
        """
        UPDATE jobs
        SET status = ?, finished_at = ?, run_exit_code = ?, run_stdout = ?, run_stderr = ?
        WHERE id = ?
        """,
        (
            JobStatus.SUCCEEDED.value,
            _to_db_timestamp(finished),
            run_exit_code,
            run_stdout,
            run_stderr,
            job_id,
        ),
        job_id,
    )
    return _require_job(connection, job_id)


def set_job_failed(
    connection: sqlite3.Connection,
    job_id: str,
    *,
    finished_at: datetime | None = None,
    run_exit_code: int | None = None,
    run_stdout: str = "",
    run_stderr: str = "",
) -> Job:
    finished = finished_at or datetime.now(timezone.utc)
    _update_job(
        connection,
        """
        UPDATE jobs
        SET status = ?, finished_at = ?, run_exit_code = ?, run_stdout = ?, run_stderr = ?
        WHERE id = ?
        """,
        (
            JobStatus.FAILED.value,
            _to_db_timestamp(finished),
            run_exit_code,
            run_stdout,
            run_stderr,
            job_id,
        ),
        job_id,
    )
    return _require_job(connection, job_id)


def fail_running_jobs(
    connection: sqlite3.Connection,
    *,
    finished_at: datetime | None = None,
    message: str = "Application restarted before job completion",
) -> int:
    finished = finished_at or datetime.now(timezone.utc)
    with connection:
        cursor = connection.execute(
            """
            UPDATE jobs
            SET status = ?, finished_at = ?, run_stderr = CASE
              WHEN run_stderr IS NULL OR run_stderr = '' THEN ?
              ELSE run_stderr
            END
            WHERE status = ?
            """,
            (
                JobStatus.FAILED.value,
                _to_db_timestamp(finished),
                message,
                JobStatus.RUNNING.value,
            ),
        )
    return int(cursor.rowcount)


def _require_job(connection: sqlite3.Connection, job_id: str) -> Job:
    job = get_job(connection, job_id)
    if job is None:
        raise LookupError(f"Job does not exist: {job_id}")
    return job


def _update_job(
    connection: sqlite3.Connection, statement: str, params: tuple[object, ...], job_id: str
) -> None:
    with connection:
        cursor = connection.execute(statement, params)
    if cursor.rowcount == 1:
        return
    if cursor.rowcount == 0 and get_job(connection, job_id) is not None:
        return
    raise LookupError(f"Job does not exist: {job_id}")


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"],
        album_dir=Path(row["album_dir"]),
        mode=JobMode(row["mode"]),
        release_ref=row["release_ref"],
        status=JobStatus(row["status"]),
        created_at=_require_timestamp(row["created_at"], field_name="created_at"),
        started_at=_from_db_timestamp(row["started_at"]),
        finished_at=_from_db_timestamp(row["finished_at"]),
        preview_stdout=row["preview_stdout"],
        preview_stderr=row["preview_stderr"],
        preview_exit_code=row["preview_exit_code"],
        run_stdout=row["run_stdout"],
        run_stderr=row["run_stderr"],
        run_exit_code=row["run_exit_code"],
    )


def _to_db_timestamp(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat()


def _from_db_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _require_timestamp(value: str | None, *, field_name: str) -> datetime:
    if value is None:
        raise ValueError(f"Expected non-null timestamp for {field_name}")
    return datetime.fromisoformat(value)
