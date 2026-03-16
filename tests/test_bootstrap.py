from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from music_ingest.bootstrap import _validate_environment
from music_ingest.config.schema import BeetsConfig, PathsConfig, Settings


def _make_settings(tmp_path: Path, *, beets_executable: str = "beet") -> Settings:
    incoming_root = tmp_path / "incoming"
    incoming_root.mkdir()
    config_file = tmp_path / "beets" / "config.yaml"
    config_file.parent.mkdir()
    config_file.write_text("", encoding="utf-8")

    settings = Settings()
    settings.paths = PathsConfig(
        incoming_root=incoming_root,
        logs_root=tmp_path / "logs",
    )
    settings.beets = BeetsConfig(
        executable=beets_executable,
        beetsdir=config_file.parent,
        config_file=config_file,
        timeout_seconds=30,
    )
    return settings


def test_validate_environment_passes_when_all_ok(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    with patch("shutil.which", return_value="/usr/bin/beet"):
        _validate_environment(settings)


def test_validate_environment_raises_when_incoming_root_missing(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    settings.paths = PathsConfig(
        incoming_root=tmp_path / "nonexistent",
        logs_root=tmp_path / "logs",
    )
    with (
        patch("shutil.which", return_value="/usr/bin/beet"),
        pytest.raises(RuntimeError, match="incoming_root does not exist"),
    ):
        _validate_environment(settings)


def test_validate_environment_raises_when_beets_config_missing(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    settings.beets = BeetsConfig(
        executable="beet",
        beetsdir=tmp_path,
        config_file=tmp_path / "missing.yaml",
        timeout_seconds=30,
    )
    with (
        patch("shutil.which", return_value="/usr/bin/beet"),
        pytest.raises(RuntimeError, match="beets config_file does not exist"),
    ):
        _validate_environment(settings)


def test_validate_environment_raises_when_beets_executable_not_found(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    with (
        patch("shutil.which", return_value=None),
        pytest.raises(RuntimeError, match="beets executable not found on PATH"),
    ):
        _validate_environment(settings)
