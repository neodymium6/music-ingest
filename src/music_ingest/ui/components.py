from __future__ import annotations

from nicegui import app as nicegui_app
from nicegui import ui


def render_header(title: str, current_page: str) -> None:
    dark = ui.dark_mode(value=nicegui_app.storage.user.get("dark_mode", True))

    def toggle_dark() -> None:
        new_value = not nicegui_app.storage.user.get("dark_mode", False)
        nicegui_app.storage.user["dark_mode"] = new_value
        dark.enable() if new_value else dark.disable()

    with ui.header().classes("items-center justify-between px-6 py-2 gap-4"):
        ui.label(title).classes("text-lg font-semibold tracking-wide")

        with ui.row().classes("gap-1 items-center"):
            _nav_button("music_note", "Incoming", "/", current_page == "/")
            _nav_button("list_alt", "Jobs", "/jobs", current_page == "/jobs")

        ui.button(icon="dark_mode", on_click=toggle_dark).props(
            "flat round dense color=white"
        ).tooltip("Toggle dark mode")


def _nav_button(icon: str, label: str, path: str, active: bool) -> None:
    props = "flat no-caps"
    if active:
        props += " unelevated"
        ui.button(label, icon=icon).props(props + " color=white").classes(
            "font-semibold underline decoration-2 underline-offset-4"
        ).on("click", lambda: ui.navigate.to(path))
    else:
        ui.button(label, icon=icon).props(props + " color=white").classes("opacity-70").on(
            "click", lambda: ui.navigate.to(path)
        )
