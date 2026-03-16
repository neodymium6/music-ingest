from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from music_ingest.domain import Job, JobMode, JobStatus
from music_ingest.ui.pages.jobs import _prune_expansion_state


def test_prune_expansion_state_removes_entries_for_absent_jobs() -> None:
    jobs = [
        Job(
            id="job-1",
            album_dir=Path("/music/incoming/A/One"),
            mode=JobMode.AS_IS,
            release_ref=None,
            status=JobStatus.FAILED,
            created_at=datetime.now(timezone.utc),
        ),
        Job(
            id="job-2",
            album_dir=Path("/music/incoming/B/Two"),
            mode=JobMode.RELEASE,
            release_ref="release-id",
            status=JobStatus.SUCCEEDED,
            created_at=datetime.now(timezone.utc),
        ),
    ]
    expansion_state = {
        ("job-1", "preview"): True,
        ("job-1", "run"): False,
        ("job-2", "run"): True,
        ("job-old", "preview"): True,
    }

    _prune_expansion_state(expansion_state, jobs)

    assert expansion_state == {
        ("job-1", "preview"): True,
        ("job-1", "run"): False,
        ("job-2", "run"): True,
    }
