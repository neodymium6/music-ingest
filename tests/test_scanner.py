from __future__ import annotations

from pathlib import Path

import pytest

from music_ingest.infra.scanner import find_album_dirs, scan_incoming_albums, summarize_album_dir


def test_find_album_dirs_only_returns_two_level_dirs_with_direct_flac_files(tmp_path: Path) -> None:
    incoming_root = tmp_path / "incoming"

    album_dir = incoming_root / "Artist A" / "Album One"
    album_dir.mkdir(parents=True)
    (album_dir / "01 - Intro.flac").write_text("", encoding="utf-8")
    (album_dir / "cover.jpg").write_text("", encoding="utf-8")

    no_flac_dir = incoming_root / "Artist B" / "Album Two"
    no_flac_dir.mkdir(parents=True)
    (no_flac_dir / "notes.txt").write_text("", encoding="utf-8")

    nested_flac_dir = incoming_root / "Artist C" / "Album Three"
    (nested_flac_dir / "disc1").mkdir(parents=True)
    (nested_flac_dir / "disc1" / "01 - Nested.flac").write_text("", encoding="utf-8")

    discovered = find_album_dirs(incoming_root)

    assert discovered == [album_dir]


def test_find_album_dirs_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert find_album_dirs(tmp_path / "missing") == []


def test_scan_incoming_albums_returns_sorted_album_summaries(tmp_path: Path) -> None:
    incoming_root = tmp_path / "incoming"

    second_album = incoming_root / "B Artist" / "Second Album"
    second_album.mkdir(parents=True)
    (second_album / "01.FLAC").write_text("", encoding="utf-8")

    first_album = incoming_root / "A Artist" / "First Album"
    first_album.mkdir(parents=True)
    (first_album / "01.flac").write_text("", encoding="utf-8")
    (first_album / "02.flac").write_text("", encoding="utf-8")

    albums = scan_incoming_albums(incoming_root)

    assert [album.artist_name for album in albums] == ["A Artist", "B Artist"]
    assert [album.album_name for album in albums] == ["First Album", "Second Album"]
    assert [album.track_count for album in albums] == [2, 1]
    assert albums[0].tracks == ("01.flac", "02.flac")
    assert albums[0].relative_path == Path("A Artist/First Album")


def test_summarize_album_dir_rejects_path_outside_incoming_root(tmp_path: Path) -> None:
    incoming_root = tmp_path / "incoming"
    incoming_root.mkdir()
    outside_album_dir = tmp_path / "other" / "Artist" / "Album"
    outside_album_dir.mkdir(parents=True)
    (outside_album_dir / "01.flac").write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="is not under incoming root"):
        summarize_album_dir(outside_album_dir, incoming_root)


def test_summarize_album_dir_rejects_non_two_level_path(tmp_path: Path) -> None:
    incoming_root = tmp_path / "incoming"
    shallow_album_dir = incoming_root / "Artist Only"
    shallow_album_dir.mkdir(parents=True)
    (shallow_album_dir / "01.flac").write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="exactly two levels"):
        summarize_album_dir(shallow_album_dir, incoming_root)
