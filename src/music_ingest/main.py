from __future__ import annotations

from music_ingest.bootstrap import bootstrap
from music_ingest.ui import MusicIngestApp, register_ui, run_ui


def main() -> None:
    context = bootstrap()
    app = MusicIngestApp(
        settings=context.settings,
        import_service=context.import_service,
        worker=context.worker,
    )
    register_ui(app)
    run_ui(context.settings)


if __name__ == "__main__":
    main()
