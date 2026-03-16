from __future__ import annotations

from typing import TYPE_CHECKING

from nicegui import ui

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp


def register_incoming_page(app: MusicIngestApp) -> None:
    @ui.page("/")
    def incoming_page() -> None:
        ui.timer(1.0, app.run_pending_jobs)

        with ui.header().classes("items-center justify-between"):
            ui.label(app.settings.app.title).classes("text-lg font-medium")
            ui.link("Jobs", "/jobs")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-4 p-4"):
            ui.label("Incoming albums").classes("text-2xl font-semibold")
            ui.label(f"Scan root: {app.incoming_root}").classes("text-sm text-slate-600")
            ui.label("Incoming page actions are added in the next commit.").classes("text-sm")
