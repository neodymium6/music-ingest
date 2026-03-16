from __future__ import annotations

import sqlite3
from pathlib import Path
from subprocess import CompletedProcess
from typing import Protocol

from music_ingest.domain import Job, JobMode
from music_ingest.infra.db import (
    claim_next_pending_job,
    claim_pending_job,
    fail_running_jobs,
    record_job_preview,
    set_job_failed,
    set_job_succeeded,
)


class BeetsRunnerProtocol(Protocol):
    def preview_as_is(self, album_dir: Path) -> CompletedProcess[str]: ...

    def run_as_is(self, album_dir: Path) -> CompletedProcess[str]: ...

    def preview_release(self, album_dir: Path, release_ref: str) -> CompletedProcess[str]: ...

    def run_release(self, album_dir: Path, release_ref: str) -> CompletedProcess[str]: ...


class ImportWorker:
    def __init__(self, connection: sqlite3.Connection, beets_runner: BeetsRunnerProtocol) -> None:
        self._connection = connection
        self._beets_runner = beets_runner

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
        try:
            preview = self._run_preview(running_job)
        except Exception as exc:
            return set_job_failed(
                self._connection,
                running_job.id,
                run_stderr=f"beets preview failed: {exc!r}",
            )
        record_job_preview(
            self._connection,
            running_job.id,
            exit_code=preview.returncode,
            stdout=preview.stdout,
            stderr=preview.stderr,
        )
        if preview.returncode != 0:
            return set_job_failed(self._connection, running_job.id)

        try:
            run = self._run_import(running_job)
        except Exception as exc:
            return set_job_failed(
                self._connection,
                running_job.id,
                run_stderr=f"beets import failed: {exc!r}",
            )
        if run.returncode == 0:
            return set_job_succeeded(
                self._connection,
                running_job.id,
                run_exit_code=run.returncode,
                run_stdout=run.stdout,
                run_stderr=run.stderr,
            )
        return set_job_failed(
            self._connection,
            running_job.id,
            run_exit_code=run.returncode,
            run_stdout=run.stdout,
            run_stderr=run.stderr,
        )

    def _run_preview(self, job: Job) -> CompletedProcess[str]:
        if job.mode is JobMode.AS_IS:
            return self._beets_runner.preview_as_is(job.album_dir)
        return self._beets_runner.preview_release(job.album_dir, _require_release_ref(job))

    def _run_import(self, job: Job) -> CompletedProcess[str]:
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
