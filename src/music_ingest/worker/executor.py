from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from subprocess import CompletedProcess
from typing import Protocol

from music_ingest.config.schema import Settings
from music_ingest.domain import DuplicateAction, Job, JobMode
from music_ingest.infra.db import (
    claim_next_pending_job,
    claim_pending_job,
    fail_running_jobs,
    open_db,
    record_job_preview,
    set_job_failed,
    set_job_skipped,
    set_job_succeeded,
)

logger = logging.getLogger(__name__)

_BEETS_DUPLICATE_MARKER = "already in the library"


class BeetsRunnerProtocol(Protocol):
    def preview_as_is(self, album_dir: Path) -> CompletedProcess[str]: ...

    def run_as_is(
        self, album_dir: Path, duplicate_action: DuplicateAction
    ) -> CompletedProcess[str]: ...

    def preview_release(self, album_dir: Path, release_ref: str) -> CompletedProcess[str]: ...

    def run_release(
        self, album_dir: Path, release_ref: str, duplicate_action: DuplicateAction
    ) -> CompletedProcess[str]: ...


class ImportWorker:
    def __init__(
        self,
        connection: sqlite3.Connection,
        beets_runner: BeetsRunnerProtocol,
        incoming_root: Path | None = None,
    ) -> None:
        self._connection = connection
        self._beets_runner = beets_runner
        self._incoming_root = incoming_root

    def reconcile_stale_jobs(self) -> int:
        return fail_running_jobs(self._connection)

    def run_next_pending(self) -> Job | None:
        claimed_job = claim_next_pending_job(self._connection)
        if claimed_job is None:
            return None
        return self._run_claimed_job(claimed_job)

    def run_job(self, job_id: str) -> Job:
        running_job = claim_pending_job(self._connection, job_id)
        return self._run_claimed_job(running_job)

    def _run_claimed_job(self, running_job: Job) -> Job:
        logger.info("Job started: %s | %s", running_job.id, running_job.album_dir)

        try:
            preview = self._run_preview(running_job)
        except Exception as exc:
            logger.exception("Job %s preview raised", running_job.id)
            return set_job_failed(
                self._connection,
                running_job.id,
                run_stderr=f"beets preview failed: {exc!r}",
            )
        record_job_preview(
            self._connection,
            running_job.id,
            exit_code=preview.returncode,
            stdout=_completed_output(preview.stdout),
            stderr=_completed_output(preview.stderr),
        )
        if preview.returncode != 0:
            logger.warning("Job %s preview failed (exit %d)", running_job.id, preview.returncode)
            return set_job_failed(self._connection, running_job.id)

        try:
            run = self._run_import(running_job)
        except Exception as exc:
            logger.exception("Job %s import raised", running_job.id)
            return set_job_failed(
                self._connection,
                running_job.id,
                run_stderr=f"beets import failed: {exc!r}",
            )
        if run.returncode == 0:
            stdout = _completed_output(run.stdout)
            stderr = _completed_output(run.stderr)
            if (
                running_job.duplicate_action is DuplicateAction.SKIP
                and _BEETS_DUPLICATE_MARKER in stdout + stderr
            ):
                logger.info("Job %s skipped (duplicate)", running_job.id)
                if self._incoming_root is not None and running_job.album_dir.is_relative_to(
                    self._incoming_root
                ):
                    _delete_flac_files(running_job.album_dir)
                    _cleanup_empty_dirs(running_job.album_dir)
                return set_job_skipped(
                    self._connection,
                    running_job.id,
                    run_exit_code=run.returncode,
                    run_stdout=stdout,
                    run_stderr=stderr,
                )
            logger.info("Job %s succeeded", running_job.id)
            if self._incoming_root is not None and running_job.album_dir.is_relative_to(
                self._incoming_root
            ):
                _cleanup_empty_dirs(running_job.album_dir)
            return set_job_succeeded(
                self._connection,
                running_job.id,
                run_exit_code=run.returncode,
                run_stdout=stdout,
                run_stderr=stderr,
            )
        logger.warning("Job %s failed (exit %d)", running_job.id, run.returncode)
        return set_job_failed(
            self._connection,
            running_job.id,
            run_exit_code=run.returncode,
            run_stdout=_completed_output(run.stdout),
            run_stderr=_completed_output(run.stderr),
        )

    def _run_preview(self, job: Job) -> CompletedProcess[str]:
        if job.mode is JobMode.AS_IS:
            return self._beets_runner.preview_as_is(job.album_dir)
        return self._beets_runner.preview_release(job.album_dir, _require_release_ref(job))

    def _run_import(self, job: Job) -> CompletedProcess[str]:
        if job.mode is JobMode.AS_IS:
            return self._beets_runner.run_as_is(job.album_dir, job.duplicate_action)
        return self._beets_runner.run_release(
            job.album_dir, _require_release_ref(job), job.duplicate_action
        )


def start_worker(connection: sqlite3.Connection, beets_runner: BeetsRunnerProtocol) -> ImportWorker:
    worker = ImportWorker(connection, beets_runner)
    worker.reconcile_stale_jobs()
    return worker


def reconcile_stale_jobs(connection: sqlite3.Connection) -> int:
    return fail_running_jobs(connection)


class ThreadedImportWorker:
    def __init__(self, settings: Settings, beets_runner: BeetsRunnerProtocol) -> None:
        self._settings = settings
        self._beets_runner = beets_runner
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = open_db(self._settings.db.path, wal=self._settings.db.wal)
        return self._connection

    def run_next_pending(self) -> Job | None:
        worker = ImportWorker(
            self._get_connection(),
            self._beets_runner,
            incoming_root=self._settings.paths.incoming_root,
        )
        return worker.run_next_pending()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def _delete_flac_files(album_dir: Path) -> None:
    for flac_file in album_dir.glob("*.flac"):
        flac_file.unlink()
        logger.info("Deleted duplicate FLAC: %s", flac_file)


def _cleanup_empty_dirs(album_dir: Path) -> None:
    for dir_path in (album_dir, album_dir.parent):
        try:
            dir_path.rmdir()
            logger.info("Removed empty directory: %s", dir_path)
        except FileNotFoundError:
            continue
        except OSError:
            break


def _require_release_ref(job: Job) -> str:
    if job.release_ref is None:
        raise ValueError(f"Release job {job.id} is missing release_ref")
    return job.release_ref


def _completed_output(value: str | None) -> str:
    return value or ""
