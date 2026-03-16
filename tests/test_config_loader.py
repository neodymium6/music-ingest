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


def test_load_settings_uses_env_conf_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    conf_dir = tmp_path / "conf"
    (conf_dir / "app").mkdir(parents=True)
    (conf_dir / "paths").mkdir()
    (conf_dir / "db").mkdir()
    (conf_dir / "beets").mkdir()
    (conf_dir / "logging").mkdir()

    (conf_dir / "config.yaml").write_text(
        "defaults:\n"
        "  - _self_\n"
        "  - app: base\n"
        "  - paths: default\n"
        "  - db: sqlite\n"
        "  - beets: default\n"
        "  - logging: default\n",
        encoding="utf-8",
    )
    (conf_dir / "app" / "base.yaml").write_text(
        "host: 127.0.0.1\nport: 9090\ntitle: env-config\nworkers: 2\n",
        encoding="utf-8",
    )
    (conf_dir / "paths" / "default.yaml").write_text(
        "incoming_root: /tmp/incoming\n"
        "library_root: /tmp/library\n"
        "data_root: /tmp/data\n"
        "logs_root: /tmp/logs\n",
        encoding="utf-8",
    )
    (conf_dir / "db" / "sqlite.yaml").write_text("path: /tmp/app.db\nwal: true\n", encoding="utf-8")
    (conf_dir / "beets" / "default.yaml").write_text(
        "executable: beet\n"
        "beetsdir: /tmp/beets\n"
        "config_file: /tmp/beets/config.yaml\n"
        "timeout_seconds: 30\n",
        encoding="utf-8",
    )
    (conf_dir / "logging" / "default.yaml").write_text(
        "level: DEBUG\nrich_tracebacks: true\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("MUSIC_INGEST_CONF_DIR", str(conf_dir))

    settings = load_settings()

    assert settings.app.title == "env-config"
    assert settings.app.port == 9090
    assert settings.paths.incoming_root == Path("/tmp/incoming")
    assert settings.beets.timeout_seconds == 30


def test_load_settings_raises_for_missing_directory(tmp_path: Path) -> None:
    missing_dir = tmp_path / "conf"

    with pytest.raises(FileNotFoundError):
        load_settings(conf_dir=missing_dir)


def test_load_settings_raises_for_missing_env_conf_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing_dir = tmp_path / "missing-conf"
    monkeypatch.setenv("MUSIC_INGEST_CONF_DIR", str(missing_dir))

    with pytest.raises(FileNotFoundError, match="MUSIC_INGEST_CONF_DIR"):
        load_settings()


def test_load_settings_raises_when_no_default_conf_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_loader_path = tmp_path / "installed" / "music_ingest" / "config" / "loader.py"
    fake_loader_path.parent.mkdir(parents=True)
    fake_loader_path.write_text("", encoding="utf-8")

    monkeypatch.delenv("MUSIC_INGEST_CONF_DIR", raising=False)
    monkeypatch.setattr("music_ingest.config.loader.__file__", str(fake_loader_path))

    with pytest.raises(FileNotFoundError, match="No default config directory is available"):
        load_settings()
