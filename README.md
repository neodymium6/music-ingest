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

`conf/` and `beets/config.yaml` are committed in the repo, but the actual music
directories are not. Because of that, local direct execution is not treated as a
supported default runtime path yet; the current configuration is container-first.
