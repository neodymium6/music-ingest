from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from music_ingest.domain.enums import DuplicateAction, JobMode, JobStatus


@dataclass(slots=True, frozen=True)
class Job:
    id: str
    album_dir: Path
    mode: JobMode
    release_ref: str | None
    duplicate_action: DuplicateAction
    status: JobStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    preview_stdout: str | None = None
    preview_stderr: str | None = None
    preview_exit_code: int | None = None
    run_stdout: str | None = None
    run_stderr: str | None = None
    run_exit_code: int | None = None


@dataclass(slots=True, frozen=True)
class IncomingAlbum:
    album_dir: Path
    relative_path: Path
    artist_name: str
    album_name: str
    tracks: tuple[str, ...]

    @property
    def track_count(self) -> int:
        return len(self.tracks)
