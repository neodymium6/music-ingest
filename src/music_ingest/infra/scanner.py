from __future__ import annotations

from pathlib import Path

from music_ingest.domain import IncomingAlbum


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


def scan_incoming_albums(incoming_root: Path) -> list[IncomingAlbum]:
    return [
        summarize_album_dir(album_dir, incoming_root)
        for album_dir in find_album_dirs(incoming_root)
    ]


def summarize_album_dir(album_dir: Path, incoming_root: Path) -> IncomingAlbum:
    relative_path = album_dir.relative_to(incoming_root)
    flac_files = sorted(
        path for path in album_dir.iterdir() if path.is_file() and path.suffix.lower() == ".flac"
    )
    return IncomingAlbum(
        album_dir=album_dir,
        relative_path=relative_path,
        artist_name=relative_path.parts[0],
        album_name=relative_path.parts[1],
        track_count=len(flac_files),
    )


def _contains_flac_files(album_dir: Path) -> bool:
    return any(path.is_file() and path.suffix.lower() == ".flac" for path in album_dir.iterdir())
