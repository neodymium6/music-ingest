from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

from music_ingest.domain import Job, JobMode
from music_ingest.infra.db import create_job, get_active_job_for_album_dir, get_job, list_jobs

_MBID_PATTERN = re.compile(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_MBID_SEARCH_PATTERN = re.compile(
    r"(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
_SUPPORTED_MB_HOSTS = {"musicbrainz.org", "beta.musicbrainz.org"}
_ACTIVE_JOB_CONSTRAINT_MARKERS = (
    "UNIQUE constraint failed: jobs.album_dir",
    "idx_jobs_album_dir_pending_running",
)


class DuplicateActiveJobError(ValueError):
    def __init__(self, album_dir: Path) -> None:
        super().__init__(f"An active import job already exists for {album_dir}")
        self.album_dir = album_dir


class InvalidReleaseRefError(ValueError):
    pass


class ImportService:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def enqueue_as_is(self, album_dir: Path) -> Job:
        return self._enqueue(album_dir=album_dir, mode=JobMode.AS_IS)

    def enqueue_release(self, album_dir: Path, release_ref: str) -> Job:
        return self._enqueue(
            album_dir=album_dir,
            mode=JobMode.RELEASE,
            release_ref=normalize_release_ref(release_ref),
        )

    def get_job(self, job_id: str) -> Job | None:
        return get_job(self._connection, job_id)

    def list_jobs(self, *, limit: int = 100) -> list[Job]:
        return list_jobs(self._connection, limit=limit)

    def _enqueue(self, *, album_dir: Path, mode: JobMode, release_ref: str | None = None) -> Job:
        try:
            return create_job(
                self._connection,
                job_id=str(uuid4()),
                album_dir=album_dir,
                mode=mode,
                release_ref=release_ref,
            )
        except sqlite3.IntegrityError as exc:
            if _is_active_job_constraint_error(exc) and (
                get_active_job_for_album_dir(self._connection, album_dir) is not None
            ):
                raise DuplicateActiveJobError(album_dir) from exc
            raise


def normalize_release_ref(release_ref: str) -> str:
    candidate = release_ref.strip()
    if not candidate:
        raise InvalidReleaseRefError("release_ref must not be empty")

    if _MBID_PATTERN.fullmatch(candidate):
        return candidate.lower()

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        raise InvalidReleaseRefError(
            "release_ref must be a MusicBrainz release URL or raw release MBID"
        )
    if parsed.hostname is None or parsed.hostname not in _SUPPORTED_MB_HOSTS:
        raise InvalidReleaseRefError(
            "release_ref URL must point to musicbrainz.org or beta.musicbrainz.org"
        )

    path_parts = [part for part in parsed.path.split("/") if part]
    try:
        release_index = path_parts.index("release")
        release_id = path_parts[release_index + 1]
    except (ValueError, IndexError) as exc:
        raise InvalidReleaseRefError("release_ref URL must contain a /release/<mbid> path") from exc

    match = _MBID_SEARCH_PATTERN.search(release_id)
    if match is None:
        raise InvalidReleaseRefError(
            "release_ref URL must contain a valid MusicBrainz release MBID"
        )
    return match.group(0).lower()


def _is_active_job_constraint_error(exc: sqlite3.IntegrityError) -> bool:
    message = str(exc)
    return any(marker in message for marker in _ACTIVE_JOB_CONSTRAINT_MARKERS)
