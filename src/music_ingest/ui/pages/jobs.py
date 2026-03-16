from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from nicegui import ui

from music_ingest.domain import Job, JobStatus
from music_ingest.ui.components import render_header

if TYPE_CHECKING:
    from music_ingest.ui.app import MusicIngestApp


def register_jobs_page(app: MusicIngestApp) -> None:
    @ui.page("/jobs")
    def jobs_page() -> None:
        expansion_state: dict[tuple[str, str], bool] = {}

        render_header(app.settings.app.title, "/jobs")

        with ui.column().classes("w-full max-w-5xl mx-auto gap-4 p-6"):
            with ui.row().classes("items-baseline gap-3"):
                ui.label("Import Jobs").classes("text-2xl font-semibold")
                status = ui.badge("").props("outline").classes("text-xs")

            list_container = ui.column().classes("w-full gap-3")

            def refresh_jobs() -> None:
                jobs = app.current_job_snapshot()
                status.set_text(f"{len(jobs)} jobs")
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

            with ui.row().classes("items-center"):
                ui.button("Refresh", icon="refresh", on_click=refresh_from_source).props(
                    "outline no-caps"
                )

            ui.timer(2.0, refresh_from_source)
            refresh_from_source()


def _render_job_card(job: Job, expansion_state: dict[tuple[str, str], bool]) -> None:
    status_color = _status_color(job.status)
    with ui.card().classes("w-full"):
        with ui.row().classes("w-full items-start justify-between gap-4"):
            with ui.column().classes("gap-1 min-w-0"):
                ui.label(job.album_dir.name).classes("text-base font-semibold")
                ui.label(job.album_dir.as_posix()).classes(
                    "text-xs text-gray-500 font-mono break-all"
                )
                with ui.row().classes("gap-3 text-xs text-gray-500 flex-wrap"):
                    ui.label(f"mode: {job.mode.value}")
                    ui.label(f"duplicate: {job.duplicate_action.value}")
                    if job.release_ref is not None:
                        ui.label(f"ref: {job.release_ref}").classes("font-mono")

            ui.badge(job.status.value, color=status_color).props("outline").classes(
                "shrink-0 self-start"
            )

        with ui.row().classes("flex-wrap gap-x-4 gap-y-0.5 text-xs text-gray-500"):
            ui.label(f"created: {_format_timestamp(job.created_at)}")
            if job.started_at is not None:
                ui.label(f"started: {_format_timestamp(job.started_at)}")
            if job.finished_at is not None:
                ui.label(f"finished: {_format_timestamp(job.finished_at)}")

        if job.preview_exit_code is not None or job.run_exit_code is not None:
            with ui.row().classes("gap-4 text-xs"):
                if job.preview_exit_code is not None:
                    _exit_code_chip("preview", job.preview_exit_code)
                if job.run_exit_code is not None:
                    _exit_code_chip("run", job.run_exit_code)

        _render_output_sections(job, expansion_state)


def _exit_code_chip(phase: str, code: int) -> None:
    color = "teal-6" if code == 0 else "deep-orange-7"
    ui.badge(f"{phase}: exit {code}", color=color).props("rounded outline")


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
            icon="preview",
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
            icon="terminal",
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
        JobStatus.PENDING: "amber-7",
        JobStatus.RUNNING: "primary",
        JobStatus.SUCCEEDED: "teal-7",
        JobStatus.FAILED: "deep-orange-8",
        JobStatus.SKIPPED: "blue-grey-5",
    }[status]


def _format_timestamp(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
