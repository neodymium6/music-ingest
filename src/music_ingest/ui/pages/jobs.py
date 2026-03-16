from __future__ import annotations

from typing import TYPE_CHECKING

from nicegui import ui

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp


def register_jobs_page(app: MusicIngestApp) -> None:
    @ui.page("/jobs")
    def jobs_page() -> None:
        ui.timer(1.0, app.run_pending_jobs)

        with ui.header().classes("items-center justify-between"):
            ui.label(app.settings.app.title).classes("text-lg font-medium")
            ui.link("Incoming", "/")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-4 p-4"):
            ui.label("Import jobs").classes("text-2xl font-semibold")
            ui.label("Jobs page details and polling refresh are added in the next commit.").classes(
                "text-sm"
            )
