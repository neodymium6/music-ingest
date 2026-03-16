from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1


def open_db(path: str | Path, *, wal: bool = True) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path), check_same_thread=False)
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
