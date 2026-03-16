from __future__ import annotations

from pathlib import Path


def find_album_dirs(incoming_root: Path) -> list[Path]:
    if not incoming_root.is_dir():
        return []

    album_dirs: list[Path] = []
    for artist_dir in sorted(incoming_root.iterdir()):
        if not artist_dir.is_dir():
            continue

        for album_dir in sorted(artist_dir.iterdir()):
            if not album_dir.is_dir():
                continue
            if _contains_flac_files(album_dir):
                album_dirs.append(album_dir)

    return album_dirs


def _contains_flac_files(album_dir: Path) -> bool:
    return any(path.is_file() and path.suffix.lower() == ".flac" for path in album_dir.iterdir())
