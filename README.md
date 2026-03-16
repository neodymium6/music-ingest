# music_ingest

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
