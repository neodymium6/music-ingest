from __future__ import annotations

from pathlib import Path

import pytest

from music_ingest.config.loader import load_settings
from music_ingest.config.schema import Settings


def test_load_settings_returns_typed_settings() -> None:
    settings = load_settings()

    assert isinstance(settings, Settings)
    assert settings.app.host == "0.0.0.0"
    assert settings.app.port == 8080
    assert settings.app.title == "music-ingest"
    assert settings.paths.incoming_root == Path("/music/incoming")
    assert settings.db.path == Path("/app/data/app.db")
    assert settings.beets.timeout_seconds == 300


def test_load_settings_raises_for_missing_directory(tmp_path: Path) -> None:
    missing_dir = tmp_path / "conf"

    with pytest.raises(FileNotFoundError):
        load_settings(conf_dir=missing_dir)
