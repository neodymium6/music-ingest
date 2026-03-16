from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path

import pytest

from music_ingest.infra.logging import setup_logging


@pytest.fixture(autouse=True)
def _cleanup_logging() -> Generator[None, None, None]:
    try:
        yield
    finally:
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            handler.close()
            root_logger.removeHandler(handler)


def test_setup_logging_without_logs_root_attaches_no_file_handler() -> None:
    setup_logging(logs_root=None)
    for handler in logging.getLogger().handlers:
        assert not isinstance(handler, logging.FileHandler)


def test_setup_logging_creates_log_file(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    setup_logging(logs_root=logs_root)

    assert logs_root.is_dir()
    assert (logs_root / "app.log").is_file()


def test_setup_logging_writes_to_log_file(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    setup_logging(level="DEBUG", logs_root=logs_root)

    logging.getLogger("test_file_logging").warning("hello from test")

    content = (logs_root / "app.log").read_text(encoding="utf-8")
    assert "hello from test" in content


def test_setup_logging_timestamp_includes_timezone_offset(tmp_path: Path) -> None:
    logs_root = tmp_path / "logs"
    setup_logging(logs_root=logs_root, timezone="Asia/Tokyo")

    logging.getLogger("test_tz").warning("tz check")

    content = (logs_root / "app.log").read_text(encoding="utf-8")
    assert "+0900" in content


def test_setup_logging_raises_for_unknown_timezone(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown timezone"):
        setup_logging(logs_root=tmp_path / "logs", timezone="Invalid/Zone")


def test_setup_logging_creates_logs_root_if_missing(tmp_path: Path) -> None:
    logs_root = tmp_path / "deep" / "nested" / "logs"
    setup_logging(logs_root=logs_root)

    assert logs_root.is_dir()
    assert (logs_root / "app.log").is_file()
