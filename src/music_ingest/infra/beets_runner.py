from __future__ import annotations

import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True, frozen=True)
class BeetsCommand:
    argv: tuple[str, ...]
    cwd: Path
    env: dict[str, str]
    timeout_seconds: int


class BeetsRunner:
    def __init__(
        self,
        *,
        executable: str,
        beetsdir: Path,
        config_file: Path,
        timeout_seconds: int,
        working_directory: Path | None = None,
        base_env: Mapping[str, str] | None = None,
    ) -> None:
        self._executable = executable
        self._beetsdir = beetsdir
        self._config_file = config_file
        self._timeout_seconds = timeout_seconds
        self._cwd = working_directory or beetsdir

        env = dict(base_env or {})
        env["BEETSDIR"] = str(beetsdir)
        self._env = env

    def build_preview_as_is(self, album_dir: Path) -> BeetsCommand:
        return self._build_command("import", "--pretend", "-A", str(album_dir))

    def build_run_as_is(self, album_dir: Path) -> BeetsCommand:
        return self._build_command("import", "-A", str(album_dir))

    def build_preview_release(self, album_dir: Path, release_ref: str) -> BeetsCommand:
        return self._build_command(
            "import", "--pretend", "--search-id", release_ref, str(album_dir)
        )

    def build_run_release(self, album_dir: Path, release_ref: str) -> BeetsCommand:
        return self._build_command("import", "--search-id", release_ref, str(album_dir))

    def preview_as_is(self, album_dir: Path) -> subprocess.CompletedProcess[str]:
        return self.execute(self.build_preview_as_is(album_dir))

    def run_as_is(self, album_dir: Path) -> subprocess.CompletedProcess[str]:
        return self.execute(self.build_run_as_is(album_dir))

    def preview_release(
        self, album_dir: Path, release_ref: str
    ) -> subprocess.CompletedProcess[str]:
        return self.execute(self.build_preview_release(album_dir, release_ref))

    def run_release(self, album_dir: Path, release_ref: str) -> subprocess.CompletedProcess[str]:
        return self.execute(self.build_run_release(album_dir, release_ref))

    def execute(self, command: BeetsCommand) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command.argv,
            capture_output=True,
            check=False,
            cwd=command.cwd,
            env=command.env,
            text=True,
            timeout=command.timeout_seconds,
        )

    def _build_command(self, *args: str) -> BeetsCommand:
        return BeetsCommand(
            argv=(self._executable, "-c", str(self._config_file), *args),
            cwd=self._cwd,
            env=dict(self._env),
            timeout_seconds=self._timeout_seconds,
        )
