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

Before starting Docker Compose, set the following environment variables:

- `MUSIC_INCOMING_DIR`: host path for incoming albums
- `MUSIC_LIBRARY_DIR`: host path for Beets library destination
- `MUSIC_INGEST_STORAGE_SECRET`: secret key for browser session storage encryption

Example:

```bash
export MUSIC_INCOMING_DIR=/path/to/incoming
export MUSIC_LIBRARY_DIR=/path/to/library
export MUSIC_INGEST_STORAGE_SECRET=your-secret-key
docker compose up --build
```

Then open `http://127.0.0.1:8080/`.

`conf/` and `beets/config.yaml` are committed in the repo, but the actual music
directories are not. Because of that, local direct execution is not treated as a
supported default runtime path yet; the current configuration is container-first.

Optionally, set `TZ` to an IANA timezone name (e.g. `Europe/Berlin`) to use a
local timezone for file log timestamps. Defaults to `UTC`.

## What The App Does

The UI exposes two import actions on the **Incoming** page (`/`):

- `Import as-is`
- `Import with release URL`

`Import as-is` runs `beet import -A ...` and keeps the existing embedded tags as
the source of truth.

`Import with release URL` accepts either a full MusicBrainz release URL or a raw
release MBID. The worker first runs `beet import --pretend --search-id ...` and
then runs the real import if preview succeeds.

Both actions accept a **duplicate handling** option: `Abort` (default), `Skip new`,
or `Remove old`.

Preview and run output are stored in SQLite and shown on the **Jobs** page (`/jobs`).

The UI defaults to dark mode. The toggle in the header persists the preference
across sessions per browser.

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

Each album card on the Incoming page shows the artist, album name, path, and an
expandable list of FLAC filenames.

## Configuration

The `conf/` directory uses [Hydra](https://hydra.cc/) for configuration composition.
The active config directory is set via the `MUSIC_INGEST_CONF_DIR` environment variable
(defaults to `./conf`).

### `conf/app/base.yaml`

| Key | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Host address the web server binds to |
| `port` | `8080` | Port the web server listens on |
| `title` | `music-ingest` | App title shown in the browser tab and header |

### `conf/paths/default.yaml`

| Key | Default | Description |
|---|---|---|
| `incoming_root` | `/music/incoming` | Root directory scanned for incoming albums |
| `logs_root` | `/app/data/logs` | Directory where rotating log files are written |

### `conf/db/sqlite.yaml`

| Key | Default | Description |
|---|---|---|
| `path` | `/app/data/app.db` | Path to the SQLite job database |
| `wal` | `true` | Enable WAL mode for better concurrent access |

### `conf/beets/default.yaml`

| Key | Default | Description |
|---|---|---|
| `executable` | `beet` | Beets CLI command name or absolute path |
| `beetsdir` | `/app/beets` | Beets state directory (passed as `BEETSDIR`) |
| `config_file` | `/app/beets/config.yaml` | Beets configuration file path |
| `timeout_seconds` | `300` | Timeout for a single beet subprocess call |

### `conf/logging/default.yaml`

| Key | Default | Description |
|---|---|---|
| `level` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `rich_tracebacks` | `true` | Enable Rich-formatted tracebacks on console |
| `timezone` | `UTC` | IANA timezone for file log timestamps (e.g. `Europe/Berlin`) |

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
- `data/logs/app.log`: rotating file log (max 10 MB, 5 backups)

The `/jobs` page is the primary place to inspect failures. It shows:

- job status and timestamps
- preview exit code and output
- run exit code and output

For container-level failures, use:

```bash
docker compose logs -f app
```
