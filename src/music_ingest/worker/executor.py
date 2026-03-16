from __future__ import annotations

import sqlite3
from typing import Protocol

from music_ingest.domain import Job, JobMode, JobStatus
from music_ingest.infra.db import (
    fail_running_jobs,
    get_job,
    get_next_pending_job,
    record_job_preview,
    set_job_failed,
    set_job_running,
    set_job_succeeded,
)


class BeetsRunnerProtocol(Protocol):
    def preview_as_is(self, album_dir): ...
    def run_as_is(self, album_dir): ...
    def preview_release(self, album_dir, release_ref): ...
    def run_release(self, album_dir, release_ref): ...


class ImportWorker:
    def __init__(self, connection: sqlite3.Connection, beets_runner: BeetsRunnerProtocol) -> None:
        self._connection = connection
        self._beets_runner = beets_runner

    def reconcile_stale_jobs(self) -> int:
        return fail_running_jobs(self._connection)

    def run_next_pending(self) -> Job | None:
        next_job = get_next_pending_job(self._connection)
        if next_job is None:
            return None
        return self.run_job(next_job.id)

    def run_job(self, job_id: str) -> Job:
        job = get_job(self._connection, job_id)
        if job is None:
            raise LookupError(f"Job does not exist: {job_id}")
        if job.status is not JobStatus.PENDING:
            raise ValueError(f"Job {job_id} is not pending: {job.status.value}")

        running_job = set_job_running(self._connection, job_id)
        preview = self._run_preview(running_job)
        record_job_preview(
            self._connection,
            job_id,
            exit_code=preview.returncode,
            stdout=preview.stdout,
            stderr=preview.stderr,
        )
        if preview.returncode != 0:
            return set_job_failed(self._connection, job_id)

        run = self._run_import(running_job)
        if run.returncode == 0:
            return set_job_succeeded(
                self._connection,
                job_id,
                run_exit_code=run.returncode,
                run_stdout=run.stdout,
                run_stderr=run.stderr,
            )
        return set_job_failed(
            self._connection,
            job_id,
            run_exit_code=run.returncode,
            run_stdout=run.stdout,
            run_stderr=run.stderr,
        )

    def _run_preview(self, job: Job):
        if job.mode is JobMode.AS_IS:
            return self._beets_runner.preview_as_is(job.album_dir)
        return self._beets_runner.preview_release(job.album_dir, _require_release_ref(job))

    def _run_import(self, job: Job):
        if job.mode is JobMode.AS_IS:
            return self._beets_runner.run_as_is(job.album_dir)
        return self._beets_runner.run_release(job.album_dir, _require_release_ref(job))


def start_worker(connection: sqlite3.Connection, beets_runner: BeetsRunnerProtocol) -> ImportWorker:
    worker = ImportWorker(connection, beets_runner)
    worker.reconcile_stale_jobs()
    return worker


def _require_release_ref(job: Job) -> str:
    if job.release_ref is None:
        raise ValueError(f"Release job {job.id} is missing release_ref")
    return job.release_ref
