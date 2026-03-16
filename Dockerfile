FROM python:3.12-slim

ARG UV_VERSION=0.10.10

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md LICENSE /app/
COPY conf /app/conf
COPY beets /app/beets
COPY src /app/src

RUN pip install --no-cache-dir "uv==${UV_VERSION}" \
    && uv sync --frozen --no-dev --no-editable

ENV PATH="/app/.venv/bin:${PATH}"

ENV MUSIC_INGEST_CONF_DIR=/app/conf

CMD ["python", "-m", "music_ingest.main"]
