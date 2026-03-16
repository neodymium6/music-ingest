# Show available recipes.
default:
  @just --list

# Bootstrap the project (git init, deps, hooks).
init:
  if [ ! -d .git ]; then git init; fi
  uv sync
  pre-commit install --install-hooks

# Run tests.
test:
  uv run pytest -q

# Run all pre-commit checks.
check:
  pre-commit run --all-files

# Create a GitHub repository and set origin.
repo-create name='music_ingest' visibility='public': init
  case "{{visibility}}" in public|private|internal) ;; *) echo "visibility must be one of: public, private, internal"; exit 2 ;; esac
  if ! gh auth status >/dev/null 2>&1; then echo "GitHub CLI is not authenticated. Run: gh auth login"; exit 2; fi
  gh repo create "{{name}}" --source=. --remote=origin --"{{visibility}}"

# Remove development caches and build artifacts.
clean:
  rm -rf .venv .mypy_cache .pytest_cache .ruff_cache .pre-commit-cache
  rm -rf .coverage .coverage.* htmlcov
  rm -rf build dist *.egg-info
  find . -type d -name "__pycache__" -exec rm -rf {} +
