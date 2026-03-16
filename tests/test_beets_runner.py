from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

import pytest

from music_ingest.domain import DuplicateAction
from music_ingest.infra import beets_runner as beets_runner_module
from music_ingest.infra.beets_runner import BeetsRunner


def test_build_preview_as_is_command_uses_expected_args_and_environment() -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=300,
        working_directory=Path("/workspace"),
        base_env={"PATH": "/usr/bin"},
    )

    command = runner.build_preview_as_is(Path("/music/incoming/Artist/Album"))

    assert command.argv == (
        "beet",
        "-c",
        "/app/beets/config.yaml",
        "import",
        "--pretend",
        "-A",
        "/music/incoming/Artist/Album",
    )
    assert command.cwd == Path("/workspace")
    assert command.timeout_seconds == 300
    assert command.env["BEETSDIR"] == "/app/beets"
    assert command.env["PATH"] == "/usr/bin"

    assert isinstance(command.env, MappingProxyType)
    with pytest.raises(TypeError):
        command.env["PATH"] = "/bin"  # type: ignore[index]


def test_build_run_as_is_command_uses_expected_args() -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=300,
    )

    command = runner.build_run_as_is(Path("/music/incoming/Artist/Album"))

    assert command.argv == (
        "beet",
        "-c",
        "/app/beets/config.yaml",
        "import",
        "-A",
        "/music/incoming/Artist/Album",
    )


def test_build_release_commands_preserve_release_ref_values() -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=120,
    )

    preview = runner.build_preview_release(
        Path("/music/incoming/Unknown Artist/Unknown Album"),
        "https://musicbrainz.org/release/test-release",
    )
    run = runner.build_run_release(
        Path("/music/incoming/Unknown Artist/Unknown Album"),
        "12345678-1234-1234-1234-123456789abc",
    )

    assert preview.argv == (
        "beet",
        "-c",
        "/app/beets/config.yaml",
        "import",
        "--pretend",
        "--search-id",
        "https://musicbrainz.org/release/test-release",
        "/music/incoming/Unknown Artist/Unknown Album",
    )
    assert run.argv == (
        "beet",
        "-c",
        "/app/beets/config.yaml",
        "import",
        "--search-id",
        "12345678-1234-1234-1234-123456789abc",
        "/music/incoming/Unknown Artist/Unknown Album",
    )
    assert preview.input_text is None
    assert run.input_text == "A\n"


def test_runner_inherits_process_environment_by_default(monkeypatch) -> None:
    monkeypatch.setenv("PATH", "/usr/local/bin")
    monkeypatch.setenv("HOME", "/home/tester")

    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=120,
    )

    command = runner.build_preview_as_is(Path("/music/incoming/Artist/Album"))

    assert command.env["PATH"] == "/usr/local/bin"
    assert command.env["HOME"] == "/home/tester"
    assert command.env["BEETSDIR"] == "/app/beets"


def test_preview_as_is_executes_with_fixed_subprocess_policy(monkeypatch) -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=45,
        working_directory=Path("/workspace"),
        base_env={"PATH": "/usr/bin"},
    )
    captured: dict[str, object] = {}

    def fake_run(
        argv: tuple[str, ...],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: Mapping[str, str],
        input: str | None,
        text: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        captured["argv"] = argv
        captured["capture_output"] = capture_output
        captured["check"] = check
        captured["cwd"] = cwd
        captured["env"] = env
        captured["input"] = input
        captured["text"] = text
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(argv, 0, stdout="preview ok", stderr="")

    monkeypatch.setattr(beets_runner_module.subprocess, "run", fake_run)

    result = runner.preview_as_is(Path("/music/incoming/Artist/Album"))

    assert result.returncode == 0
    assert result.stdout == "preview ok"
    command_env = captured["env"]
    assert captured == {
        "argv": (
            "beet",
            "-c",
            "/app/beets/config.yaml",
            "import",
            "--pretend",
            "-A",
            "/music/incoming/Artist/Album",
        ),
        "capture_output": True,
        "check": False,
        "cwd": Path("/workspace"),
        "env": command_env,
        "input": None,
        "text": True,
        "timeout": 45,
    }
    assert isinstance(command_env, Mapping)
    assert command_env["PATH"] == "/usr/bin"
    assert command_env["BEETSDIR"] == "/app/beets"


@pytest.mark.parametrize(
    ("duplicate_action", "expected_input"),
    [
        (DuplicateAction.ABORT, ""),
        (DuplicateAction.SKIP, "S\n"),
        (DuplicateAction.REMOVE, "R\n"),
    ],
)
def test_build_run_as_is_input_reflects_duplicate_action(
    duplicate_action: DuplicateAction, expected_input: str
) -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=300,
    )

    command = runner.build_run_as_is(Path("/music/incoming/Artist/Album"), duplicate_action)

    assert command.input_text == expected_input


@pytest.mark.parametrize(
    ("duplicate_action", "expected_input"),
    [
        (DuplicateAction.ABORT, "A\n"),
        (DuplicateAction.SKIP, "A\nS\n"),
        (DuplicateAction.REMOVE, "A\nR\n"),
    ],
)
def test_build_run_release_input_reflects_duplicate_action(
    duplicate_action: DuplicateAction, expected_input: str
) -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=300,
    )

    command = runner.build_run_release(
        Path("/music/incoming/Artist/Album"),
        "12345678-1234-1234-1234-123456789abc",
        duplicate_action,
    )

    assert command.input_text == expected_input


def test_run_release_executes_with_apply_input(monkeypatch) -> None:
    runner = BeetsRunner(
        executable="beet",
        beetsdir=Path("/app/beets"),
        config_file=Path("/app/beets/config.yaml"),
        timeout_seconds=45,
    )
    captured: dict[str, object] = {}

    def fake_run(
        argv: tuple[str, ...],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: Mapping[str, str],
        input: str | None,
        text: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        captured["argv"] = argv
        captured["input"] = input
        captured["capture_output"] = capture_output
        captured["check"] = check
        captured["cwd"] = cwd
        captured["env"] = env
        captured["text"] = text
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(argv, 0, stdout="applied", stderr="")

    monkeypatch.setattr(beets_runner_module.subprocess, "run", fake_run)

    result = runner.run_release(
        Path("/music/incoming/Unknown Artist/Unknown Album"),
        "12345678-1234-1234-1234-123456789abc",
    )

    assert result.returncode == 0
    assert result.stdout == "applied"
    assert captured["input"] == "A\n"
