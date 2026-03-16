from __future__ import annotations

import os
from pathlib import Path

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from music_ingest.config.schema import Settings

_CONF_DIR_ENV_VAR = "MUSIC_INGEST_CONF_DIR"


def load_settings(conf_dir: Path | None = None) -> Settings:
    resolved_conf_dir = conf_dir or _default_conf_dir()
    if not resolved_conf_dir.is_dir():
        raise FileNotFoundError(f"Config directory does not exist: {resolved_conf_dir}")

    with initialize_config_dir(version_base=None, config_dir=str(resolved_conf_dir)):
        config = compose(config_name="config")

    merged = OmegaConf.merge(OmegaConf.structured(Settings), config)
    settings = OmegaConf.to_object(merged)
    if not isinstance(settings, Settings):
        raise TypeError("Expected composed settings to resolve to a Settings instance")
    return settings


def _default_conf_dir() -> Path:
    env_conf_dir = os.environ.get(_CONF_DIR_ENV_VAR)
    if env_conf_dir:
        env_path = Path(env_conf_dir).expanduser()
        if not env_path.is_dir():
            raise FileNotFoundError(
                f"{_CONF_DIR_ENV_VAR} points to a missing config directory: {env_path}"
            )
        return env_path

    repo_conf_dir = Path(__file__).resolve().parents[3] / "conf"
    if repo_conf_dir.is_dir():
        return repo_conf_dir

    raise FileNotFoundError(
        "No default config directory is available. "
        "Pass 'conf_dir' to load_settings() or set "
        f"{_CONF_DIR_ENV_VAR} to a valid config directory."
    )
