# music-ingest

Web UI for importing ripped albums into a Beets-managed music library.

## Dev
```bash
# If you use direnv
direnv allow
just init
just test
uv run python -c "import music_ingest; print('ok')"

# Without direnv
nix develop
just init
just test
uv run python -c "import music_ingest; print('ok')"

# Run all checks
just check

# Optional: create a GitHub repository
just repo-create
# or: just repo-create your-repo-name
# or: just repo-create your-repo-name public
```

## Runtime

This repository does not provide a zero-config `docker compose up`.

Runtime paths are environment-specific. The container expects these mount points:

- `/music/incoming`
- `/music/library`
- `/app/data`
- `/app/beets`
- `/app/conf`

Before starting Docker Compose, set host paths for:

- `MUSIC_INCOMING_DIR`
- `MUSIC_LIBRARY_DIR`

Example:

```bash
export MUSIC_INCOMING_DIR=/path/to/incoming
export MUSIC_LIBRARY_DIR=/path/to/library
docker compose up --build
```

Then open `http://127.0.0.1:8080/`.

`conf/` and `beets/config.yaml` are committed in the repo, but the actual music
directories are not. Because of that, local direct execution is not treated as a
supported default runtime path yet; the current configuration is container-first.

## What The App Does

The UI exposes two import actions:

- `Import as-is`
- `Import with release URL`

`Import as-is` runs `beet import -A ...` and keeps the existing embedded tags as
the source of truth.

`Import with release URL` accepts either a full MusicBrainz release URL or a raw
release MBID. The worker first runs `beet import --pretend --search-id ...` and
then runs the real import if preview succeeds.

Preview and run output are stored in SQLite and shown on the `/jobs` page.

## Incoming Layout

Incoming album discovery is filesystem-based.

- `incoming_root/*/*` is treated as an album directory
- the directory must contain at least one `.flac` file

In practice, the expected layout is:

```text
incoming/
  Artist/
    Album/
      01 - Track.flac
      02 - Track.flac
```

## Beets Responsibility

This app does orchestration only. Tag writing, file moves, and final library
layout are delegated to Beets.

`beets/config.yaml` defines:

- the Beets library database at `/app/data/beets.db`
- the library destination root at `/music/library`
- path formatting via `paths:`

The default committed config moves imported files into the library and writes
tags during import.

## Data And Logs

The Compose setup persists application state in `data/`.

- `data/app.db`: app job database
- `data/beets.db`: Beets library database

The `/jobs` page is the primary place to inspect failures. It shows:

- job status and timestamps
- preview exit code and output
- run exit code and output

For container-level failures, use:

```bash
docker compose logs -f app
```
