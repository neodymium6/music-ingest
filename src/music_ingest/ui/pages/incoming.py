from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from nicegui import ui

from music_ingest.domain import IncomingAlbum
from music_ingest.services import DuplicateActiveJobError, InvalidReleaseRefError

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp

logger = logging.getLogger(__name__)


def register_incoming_page(app: MusicIngestApp) -> None:
    @ui.page("/")
    def incoming_page() -> None:
        with ui.header().classes("items-center justify-between"):
            ui.label(app.settings.app.title).classes("text-lg font-medium")
            ui.link("Jobs", "/jobs")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-4 p-4"):
            ui.label("Incoming albums").classes("text-2xl font-semibold")
            ui.label(f"Scan root: {app.incoming_root}").classes("text-sm text-slate-600")

            status = ui.label().classes("text-sm text-slate-600")
            list_container = ui.column().classes("w-full gap-4")

            def refresh_albums() -> None:
                albums = app.list_incoming_albums()
                status.set_text(f"{len(albums)} album directories found")

                list_container.clear()
                with list_container:
                    if not albums:
                        with ui.card().classes("w-full"):
                            ui.label("No importable album directories were found.")
                            ui.label(
                                "Expected layout: incoming_root/Artist/Album with at least one .flac"
                            ).classes("text-sm text-slate-600")
                        return

                    for album in albums:
                        _render_album_card(app, album)

            ui.button("Refresh", on_click=refresh_albums).props("outline")
            refresh_albums()


def _render_album_card(app: MusicIngestApp, album: IncomingAlbum) -> None:
    with ui.card().classes("w-full gap-3"):
        with ui.row().classes("w-full items-start justify-between gap-4"):
            with ui.column().classes("gap-1"):
                ui.label(f"{album.artist_name} / {album.album_name}").classes("text-lg font-medium")
                ui.label(str(album.relative_path)).classes("text-sm text-slate-600")
                ui.label(f"{album.track_count} FLAC tracks").classes("text-sm text-slate-600")

            ui.button(
                "Import as-is",
                on_click=lambda: _enqueue_as_is(app, album),
            ).props("color=primary")

        with ui.row().classes("w-full items-end gap-2"):
            release_input = ui.input("MusicBrainz release URL or MBID").classes("grow")
            ui.button(
                "Import with release URL",
                on_click=lambda: _enqueue_release(app, album, release_input.value),
            ).props("outline")


def _enqueue_as_is(app: MusicIngestApp, album: IncomingAlbum) -> None:
    try:
        job = app.enqueue_as_is(album.album_dir)
    except DuplicateActiveJobError as exc:
        ui.notify(str(exc), type="warning")
        return
    except Exception:
        logger.exception("Failed to enqueue as-is import for %s", album.album_dir)
        ui.notify(f"Failed to queue import for {album.relative_path}", type="negative")
        return

    ui.notify(f"Queued as-is import: {job.id}", type="positive")


def _enqueue_release(app: MusicIngestApp, album: IncomingAlbum, release_ref: str | None) -> None:
    try:
        job = app.enqueue_release(album.album_dir, release_ref or "")
    except InvalidReleaseRefError as exc:
        ui.notify(str(exc), type="warning")
        return
    except DuplicateActiveJobError as exc:
        ui.notify(str(exc), type="warning")
        return
    except Exception:
        logger.exception("Failed to enqueue release import for %s", album.album_dir)
        ui.notify(f"Failed to queue release import for {album.relative_path}", type="negative")
        return

    ui.notify(f"Queued release import: {job.id}", type="positive")
