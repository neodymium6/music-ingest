from __future__ import annotations

import asyncio

from music_ingest.bootstrap import bootstrap
from music_ingest.ui import MusicIngestApp, register_ui, run_ui
from music_ingest.worker import ThreadedImportWorker


def main() -> None:
    context = bootstrap()
    app: MusicIngestApp | None = None
    try:
        app = MusicIngestApp(
            settings=context.settings,
            import_service=context.import_service,
            worker=ThreadedImportWorker(
                settings=context.settings,
                beets_runner=context.beets_runner,
            ),
        )
        register_ui(app)
        run_ui(context.settings)
    finally:
        try:
            if app is not None:
                asyncio.run(app.stop_background_tasks())
        finally:
            context.connection.close()


if __name__ == "__main__":
    main()
