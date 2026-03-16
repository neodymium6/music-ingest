from __future__ import annotations

from pathlib import Path

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from music_ingest.config.schema import Settings


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
    return Path(__file__).resolve().parents[3] / "conf"
