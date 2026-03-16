from __future__ import annotations

import logging

from music_ingest.bootstrap import bootstrap


def main() -> None:
    context = bootstrap()
    logging.getLogger(__name__).info(
        "Bootstrap complete; UI wiring is not implemented yet for %s",
        context.settings.app.title,
    )


if __name__ == "__main__":
    main()
