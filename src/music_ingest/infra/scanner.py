from __future__ import annotations

from pathlib import Path

from music_ingest.domain import IncomingAlbum


def find_album_dirs(incoming_root: Path) -> list[Path]:
    return [album_dir for album_dir, _ in _iter_album_dirs(incoming_root)]


def scan_incoming_albums(incoming_root: Path) -> list[IncomingAlbum]:
    return [
        summarize_album_dir(album_dir, incoming_root, flac_files=flac_files)
        for album_dir, flac_files in _iter_album_dirs(incoming_root)
    ]


def summarize_album_dir(
    album_dir: Path, incoming_root: Path, *, flac_files: list[Path] | None = None
) -> IncomingAlbum:
    try:
        relative_path = album_dir.relative_to(incoming_root)
    except ValueError as exc:
        raise ValueError(
            f"Album directory {album_dir!s} is not under incoming root {incoming_root!s}"
        ) from exc

    if len(relative_path.parts) != 2:
        raise ValueError(
            "Expected album directory to be exactly two levels under incoming root "
            f"(artist/album), got {relative_path!s}"
        )

    discovered_flac_files = flac_files if flac_files is not None else _flac_files_in_dir(album_dir)
    return IncomingAlbum(
        album_dir=album_dir,
        relative_path=relative_path,
        artist_name=relative_path.parts[0],
        album_name=relative_path.parts[1],
        tracks=tuple(f.name for f in discovered_flac_files),
    )


def _iter_album_dirs(incoming_root: Path) -> list[tuple[Path, list[Path]]]:
    if not incoming_root.is_dir():
        return []

    album_dirs: list[tuple[Path, list[Path]]] = []
    for artist_dir in sorted(incoming_root.iterdir()):
        if not artist_dir.is_dir():
            continue

        for album_dir in sorted(artist_dir.iterdir()):
            if not album_dir.is_dir():
                continue
            flac_files = _flac_files_in_dir(album_dir)
            if flac_files:
                album_dirs.append((album_dir, flac_files))

    return album_dirs


def _flac_files_in_dir(album_dir: Path) -> list[Path]:
    return sorted(
        path for path in album_dir.iterdir() if path.is_file() and path.suffix.lower() == ".flac"
    )
