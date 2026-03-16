from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    title: str = "music-ingest"


@dataclass(slots=True)
class PathsConfig:
    incoming_root: Path = Path("/music/incoming")
    logs_root: Path = Path("/app/data/logs")


@dataclass(slots=True)
class DbConfig:
    path: Path = Path("/app/data/app.db")
    wal: bool = True


@dataclass(slots=True)
class BeetsConfig:
    executable: str = "beet"
    beetsdir: Path = Path("/app/beets")
    config_file: Path = Path("/app/beets/config.yaml")
    timeout_seconds: int = 300


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    rich_tracebacks: bool = True


@dataclass(slots=True)
class Settings:
    app: AppConfig = field(default_factory=AppConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    db: DbConfig = field(default_factory=DbConfig)
    beets: BeetsConfig = field(default_factory=BeetsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
