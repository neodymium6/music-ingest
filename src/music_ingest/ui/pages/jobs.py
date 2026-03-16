from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from nicegui import ui

from music_ingest.domain import Job, JobStatus

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp


def register_jobs_page(app: MusicIngestApp) -> None:
    @ui.page("/jobs")
    def jobs_page() -> None:
        expansion_state: dict[tuple[str, str], bool] = {}

        with ui.header().classes("items-center justify-between"):
            ui.label(app.settings.app.title).classes("text-lg font-medium")
            ui.link("Incoming", "/")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-4 p-4"):
            ui.label("Import jobs").classes("text-2xl font-semibold")
            status = ui.label().classes("text-sm text-slate-600")
            list_container = ui.column().classes("w-full gap-4")

            def refresh_jobs() -> None:
                jobs = app.current_job_snapshot()
                status.set_text(f"{len(jobs)} jobs loaded")
                _prune_expansion_state(expansion_state, jobs)

                list_container.clear()
                with list_container:
                    if not jobs:
                        with ui.card().classes("w-full"):
                            ui.label("No jobs have been queued yet.")
                        return

                    for job in jobs:
                        _render_job_card(job, expansion_state)

            def refresh_from_source() -> None:
                app.refresh_job_snapshot()
                refresh_jobs()

            ui.button("Refresh", on_click=refresh_from_source).props("outline")
            ui.timer(2.0, refresh_from_source)
            refresh_from_source()


def _render_job_card(job: Job, expansion_state: dict[tuple[str, str], bool]) -> None:
    with ui.card().classes("w-full gap-3"):
        with ui.row().classes("w-full items-start justify-between gap-4"):
            with ui.column().classes("gap-1"):
                ui.label(job.album_dir.as_posix()).classes("text-base font-medium")
                ui.label(f"mode: {job.mode.value}").classes("text-sm text-slate-600")
                if job.release_ref is not None:
                    ui.label(f"release_ref: {job.release_ref}").classes("text-sm text-slate-600")

            ui.badge(job.status.value, color=_status_color(job.status))

        with ui.row().classes("flex-wrap gap-x-4 gap-y-1 text-sm text-slate-600"):
            ui.label(f"created: {_format_timestamp(job.created_at)}")
            if job.started_at is not None:
                ui.label(f"started: {_format_timestamp(job.started_at)}")
            if job.finished_at is not None:
                ui.label(f"finished: {_format_timestamp(job.finished_at)}")

        if job.preview_exit_code is not None:
            ui.label(f"preview exit: {job.preview_exit_code}").classes("text-sm")
        if job.run_exit_code is not None:
            ui.label(f"run exit: {job.run_exit_code}").classes("text-sm")

        _render_output_sections(job, expansion_state)


def _render_output_sections(job: Job, expansion_state: dict[tuple[str, str], bool]) -> None:
    if any(
        value not in (None, "")
        for value in (job.preview_stdout, job.preview_stderr, job.preview_exit_code)
    ):
        preview_key = (job.id, "preview")

        def handle_preview_expansion_change(
            event,
            *,
            key: tuple[str, str] = preview_key,
        ) -> None:
            expansion_state[key] = bool(event.value)

        with ui.expansion(
            "Preview output",
            value=expansion_state.get(preview_key, False),
            on_value_change=handle_preview_expansion_change,
        ).classes("w-full"):
            _render_output_block("stdout", job.preview_stdout)
            _render_output_block("stderr", job.preview_stderr)

    if any(
        value not in (None, "") for value in (job.run_stdout, job.run_stderr, job.run_exit_code)
    ):
        run_key = (job.id, "run")

        def handle_run_expansion_change(
            event,
            *,
            key: tuple[str, str] = run_key,
        ) -> None:
            expansion_state[key] = bool(event.value)

        with ui.expansion(
            "Run output",
            value=expansion_state.get(run_key, False),
            on_value_change=handle_run_expansion_change,
        ).classes("w-full"):
            _render_output_block("stdout", job.run_stdout)
            _render_output_block("stderr", job.run_stderr)


def _prune_expansion_state(expansion_state: dict[tuple[str, str], bool], jobs: list[Job]) -> None:
    active_keys = {(job.id, section) for job in jobs for section in ("preview", "run")}
    for key in list(expansion_state):
        if key not in active_keys:
            del expansion_state[key]


def _render_output_block(label: str, value: str | None) -> None:
    text = "(empty)" if value in (None, "") else str(value)
    ui.textarea(label=label, value=text).props("readonly autogrow outlined").classes("w-full")


def _status_color(status: JobStatus) -> str:
    return {
        JobStatus.PENDING: "warning",
        JobStatus.RUNNING: "primary",
        JobStatus.SUCCEEDED: "positive",
        JobStatus.FAILED: "negative",
    }[status]


def _format_timestamp(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
