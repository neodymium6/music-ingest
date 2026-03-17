from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from nicegui import ui

from music_ingest.domain import DuplicateAction, IncomingAlbum
from music_ingest.services import DuplicateActiveJobError, InvalidReleaseRefError
from music_ingest.ui.components import render_header

_DUPLICATE_ACTION_OPTIONS = {
    "Abort": DuplicateAction.ABORT,
    "Skip new": DuplicateAction.SKIP,
    "Remove old": DuplicateAction.REMOVE,
}

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp

logger = logging.getLogger(__name__)


def register_incoming_page(app: MusicIngestApp) -> None:
    @ui.page("/")
    def incoming_page() -> None:
        render_header(app.settings.app.title, "/")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-6 p-6"):
            with ui.row().classes("items-baseline gap-3"):
                ui.label("Incoming Albums").classes("text-2xl font-semibold")
                status = ui.badge("").props("outline").classes("text-xs")

            ui.label(f"Scan root: {app.incoming_root}").classes("text-sm text-gray-500")

            list_container = ui.column().classes("w-full gap-8")
            rendered_dirs: list[Path] = []

            def refresh_albums() -> None:
                albums = app.list_incoming_albums()
                current_dirs = [a.album_dir for a in albums]
                status.set_text(f"{len(albums)} found")

                if current_dirs == rendered_dirs:
                    return

                rendered_dirs.clear()
                rendered_dirs.extend(current_dirs)
                list_container.clear()
                with list_container:
                    if not albums:
                        with ui.card().classes("w-full"):
                            ui.label("No importable album directories found.").classes(
                                "font-medium"
                            )
                            ui.label(
                                "Expected layout: incoming_root/Artist/Album with at least one .flac"
                            ).classes("text-sm text-gray-500")
                        return

                    for album in albums:
                        _render_album_card(app, album)

            with ui.row().classes("items-center"):
                ui.button("Refresh", icon="refresh", on_click=refresh_albums).props(
                    "outline no-caps"
                )

            ui.timer(1.0, refresh_albums)
            refresh_albums()


def _render_album_card(app: MusicIngestApp, album: IncomingAlbum) -> None:
    with ui.card().classes("w-full"):
        with ui.column().classes("gap-0.5"):
            ui.label(f"{album.artist_name} / {album.album_name}").classes("text-base font-semibold")
            ui.label(str(album.relative_path)).classes("text-xs text-gray-500 font-mono")

        with (
            ui.expansion(f"{album.track_count} FLAC tracks", icon="audio_file").classes(
                "w-full text-xs text-gray-500"
            ),
            ui.column().classes("gap-0.5 pl-2"),
        ):
            for track in album.tracks:
                ui.label(track).classes("text-xs font-mono")

        with ui.row().classes("w-full items-center gap-3"):
            release_input = (
                ui.input(
                    "MusicBrainz release URL or MBID",
                    placeholder="https://musicbrainz.org/release/...",
                )
                .props("outlined")
                .classes("grow")
            )
            duplicate_select = ui.select(
                list(_DUPLICATE_ACTION_OPTIONS.keys()),
                value="Abort",
                label="If duplicate",
            ).classes("w-36")

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button(
                "Import as-is",
                icon="download_done",
                on_click=lambda: _enqueue_as_is(
                    app, album, _DUPLICATE_ACTION_OPTIONS[duplicate_select.value]
                ),
            ).props("outline no-caps color=secondary")
            ui.button(
                "Import with release URL",
                icon="library_music",
                on_click=lambda: _enqueue_release(
                    app,
                    album,
                    release_input.value,
                    _DUPLICATE_ACTION_OPTIONS[duplicate_select.value],
                ),
            ).props("no-caps color=primary")


def _enqueue_as_is(
    app: MusicIngestApp, album: IncomingAlbum, duplicate_action: DuplicateAction
) -> None:
    try:
        job = app.enqueue_as_is(album.album_dir, duplicate_action)
    except DuplicateActiveJobError as exc:
        ui.notify(str(exc), type="warning", position="top-right")
        return
    except Exception:
        logger.exception("Failed to enqueue as-is import for %s", album.album_dir)
        ui.notify(
            f"Failed to queue import for {album.relative_path}",
            type="negative",
            position="top-right",
        )
        return

    ui.notify(f"Queued: {job.id}", type="positive", position="top-right")


def _enqueue_release(
    app: MusicIngestApp,
    album: IncomingAlbum,
    release_ref: str | None,
    duplicate_action: DuplicateAction,
) -> None:
    try:
        job = app.enqueue_release(album.album_dir, release_ref or "", duplicate_action)
    except InvalidReleaseRefError as exc:
        ui.notify(str(exc), type="warning", position="top-right")
        return
    except DuplicateActiveJobError as exc:
        ui.notify(str(exc), type="warning", position="top-right")
        return
    except Exception:
        logger.exception("Failed to enqueue release import for %s", album.album_dir)
        ui.notify(
            f"Failed to queue release import for {album.relative_path}",
            type="negative",
            position="top-right",
        )
        return

    ui.notify(f"Queued: {job.id}", type="positive", position="top-right")
