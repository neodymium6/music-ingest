from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from music_ingest.domain import Job, JobMode, JobStatus

SCHEMA_VERSION = 2


def open_db(path: str | Path, *, wal: bool = True) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute(f"PRAGMA journal_mode = {'WAL' if wal else 'DELETE'};")

    try:
        apply_schema(connection)
    except Exception:
        connection.close()
        raise
    return connection


def apply_schema(connection: sqlite3.Connection) -> None:
    connection.row_factory = sqlite3.Row
    current_version = int(connection.execute("PRAGMA user_version;").fetchone()[0])
    if current_version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {current_version} is newer than supported {SCHEMA_VERSION}"
        )

    while current_version < SCHEMA_VERSION:
        if current_version == 0:
            _create_schema_v1(connection)
            current_version = 1
            continue

        if current_version == 1:
            _migrate_v1_to_v2(connection)
            current_version = 2
            continue

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
    if limit <= 0:
        raise ValueError(f"limit must be a positive integer, got {limit!r}")
    rows = connection.execute(
        """
        SELECT * FROM jobs
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_row_to_job(row) for row in rows]


def get_active_job_for_album_dir(connection: sqlite3.Connection, album_dir: Path) -> Job | None:
    row = connection.execute(
        """
        SELECT * FROM jobs
        WHERE album_dir = ? AND status IN (?, ?)
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (str(album_dir), JobStatus.PENDING.value, JobStatus.RUNNING.value),
    ).fetchone()
    return _row_to_job(row) if row is not None else None


def claim_next_pending_job(
    connection: sqlite3.Connection, *, started_at: datetime | None = None
) -> Job | None:
    started = started_at or datetime.now(timezone.utc)
    row: sqlite3.Row | None = None
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        pending_row = cursor.execute(
            """
            SELECT id FROM jobs
            WHERE status = ?
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (JobStatus.PENDING.value,),
        ).fetchone()
        if pending_row is None:
            connection.rollback()
            return None

        job_id = pending_row["id"]
        cursor.execute(
            """
            UPDATE jobs
            SET status = ?, started_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                JobStatus.RUNNING.value,
                _to_db_timestamp(started),
                job_id,
                JobStatus.PENDING.value,
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return None

        row = cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return _row_to_job(row) if row is not None else None


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
    run_stdout: str | None = None,
    run_stderr: str | None = None,
) -> Job:
    finished = finished_at or datetime.now(timezone.utc)
    query, params = _build_terminal_update(
        status=JobStatus.SUCCEEDED,
        finished_at=finished,
        run_exit_code=run_exit_code,
        run_stdout=run_stdout,
        run_stderr=run_stderr,
        job_id=job_id,
    )
    _update_job(connection, query, params, job_id)
    return _require_job(connection, job_id)


def set_job_failed(
    connection: sqlite3.Connection,
    job_id: str,
    *,
    finished_at: datetime | None = None,
    run_exit_code: int | None = None,
    run_stdout: str | None = None,
    run_stderr: str | None = None,
) -> Job:
    finished = finished_at or datetime.now(timezone.utc)
    query, params = _build_terminal_update(
        status=JobStatus.FAILED,
        finished_at=finished,
        run_exit_code=run_exit_code,
        run_stdout=run_stdout,
        run_stderr=run_stderr,
        job_id=job_id,
    )
    _update_job(connection, query, params, job_id)
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
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Naive datetimes are not supported; pass a timezone-aware datetime")
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat()


def _from_db_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _require_timestamp(value: str | None, *, field_name: str) -> datetime:
    if value is None:
        raise ValueError(f"Expected non-null timestamp for {field_name}")
    return datetime.fromisoformat(value)


def _build_terminal_update(
    *,
    status: JobStatus,
    finished_at: datetime,
    run_exit_code: int | None,
    run_stdout: str | None,
    run_stderr: str | None,
    job_id: str,
) -> tuple[str, tuple[object, ...]]:
    set_clauses = [
        "status = ?",
        "finished_at = ?",
        "run_exit_code = ?",
    ]
    params: list[object] = [
        status.value,
        _to_db_timestamp(finished_at),
        run_exit_code,
    ]
    if run_stdout is not None:
        set_clauses.append("run_stdout = ?")
        params.append(run_stdout)
    if run_stderr is not None:
        set_clauses.append("run_stderr = ?")
        params.append(run_stderr)
    params.append(job_id)
    query = f"""
        UPDATE jobs
        SET {", ".join(set_clauses)}
        WHERE id = ?
    """
    return query, tuple(params)


def _create_schema_v1(connection: sqlite3.Connection) -> None:
    existing_jobs_table = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs';"
    ).fetchone()
    if existing_jobs_table is not None:
        raise RuntimeError(
            "Existing 'jobs' table found in database with schema version 0; "
            "refusing to auto-migrate an unknown schema."
        )

    with connection:
        connection.executescript(
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
            """
        )
        connection.execute("PRAGMA user_version = 1;")


def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
    duplicate_rows = connection.execute(
        """
        SELECT album_dir, COUNT(*) AS duplicate_count
        FROM jobs
        WHERE status IN ('pending', 'running')
        GROUP BY album_dir
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if duplicate_rows:
        conflicting_dirs = ", ".join(repr(row["album_dir"]) for row in duplicate_rows)
        raise RuntimeError(
            "Cannot migrate database schema from v1 to v2 because multiple pending/running "
            "jobs exist for the same album_dir. Resolve the duplicates and retry. "
            f"Conflicting album_dir values: {conflicting_dirs}"
        )

    with connection:
        connection.executescript(
            """
            DROP INDEX IF EXISTS idx_jobs_album_dir_pending_running;

            CREATE UNIQUE INDEX idx_jobs_album_dir_pending_running
            ON jobs(album_dir)
            WHERE status IN ('pending', 'running');
            """
        )
        connection.execute("PRAGMA user_version = 2;")
