from __future__ import annotations

import subprocess
from pathlib import Path

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
        env: dict[str, str],
        text: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        captured["argv"] = argv
        captured["capture_output"] = capture_output
        captured["check"] = check
        captured["cwd"] = cwd
        captured["env"] = env
        captured["text"] = text
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(argv, 0, stdout="preview ok", stderr="")

    monkeypatch.setattr(beets_runner_module.subprocess, "run", fake_run)

    result = runner.preview_as_is(Path("/music/incoming/Artist/Album"))

    assert result.returncode == 0
    assert result.stdout == "preview ok"
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
        "env": {"PATH": "/usr/bin", "BEETSDIR": "/app/beets"},
        "text": True,
        "timeout": 45,
    }
