# Drone track compaction service.
# Single worker per container: scale horizontally with docker compose --scale
# or the two fixed instances in docker-compose.yml. All state lives in the
# shared /data volume (SQLite WAL + Parquet), so instances are interchangeable.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_ROOT=/data

WORKDIR /srv/app

COPY requirements.txt /srv/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /srv/app/app

# /data holds metadata.db and all Parquet fragments; mount a shared volume.
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# uvicorn's default lifespan triggers the startup recovery + GC loop.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
