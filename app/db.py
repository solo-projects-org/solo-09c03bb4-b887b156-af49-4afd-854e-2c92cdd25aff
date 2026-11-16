"""SQLite metadata store.

The database holds *only* metadata. Trajectory rows live in Parquet files.
WAL mode lets one writer (upload / merge) coexist with readers (export) and
with the second API instance.

Concurrency / recovery model
----------------------------
* ``instances`` is an incarnation registry: every process gets a unique
  ``incarnation_id`` at startup and renews a lease with a heartbeat. A crashed
  process stops heartbeating, so its lease expires and every other instance
  can tell its writes are dead.
* ``write_intents`` records every in-flight file write (upload or merge)
  together with its owning incarnation. Remnants are reaped only when the
  owner is provably dead (registry row dead or lease expired), so one
  instance starting up never touches another live instance's temp files.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS voyages (
    voyage_id        TEXT PRIMARY KEY,
    created_at       TEXT NOT NULL,
    total_row_count  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fragments (
    frag_id        TEXT PRIMARY KEY,
    voyage_id      TEXT NOT NULL REFERENCES voyages(voyage_id),
    kind           TEXT NOT NULL CHECK (kind IN ('batch', 'merged')),
    -- path relative to DATA_ROOT
    rel_path       TEXT NOT NULL UNIQUE,
    seq            INTEGER NOT NULL,
    row_count      INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    -- 'active' fragments are the current published data; 'superseded'
    -- fragments are kept until garbage collection removes the file.
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'superseded')),
    checksum       TEXT
);

CREATE INDEX IF NOT EXISTS idx_frag_voyage_status
    ON fragments(voyage_id, status, seq);

-- Only active fragments need a unique position. A merged fragment inherits
-- the seq of the superseded sources it replaces, so a partial index is
-- required. seq allocation also runs inside an immediate transaction.
CREATE UNIQUE INDEX IF NOT EXISTS idx_frag_active_voyage_seq
    ON fragments(voyage_id, seq) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS upload_batches (
    batch_id     TEXT PRIMARY KEY,
    voyage_id    TEXT NOT NULL,
    frag_id      TEXT NOT NULL REFERENCES fragments(frag_id),
    row_count    INTEGER NOT NULL,
    received_at  TEXT NOT NULL
);

-- Incarnation registry. One row per process start; the incarnation_id is
-- unique even when INSTANCE_ID is reused across rolling restarts.
CREATE TABLE IF NOT EXISTS instances (
    incarnation_id   TEXT PRIMARY KEY,
    instance_id      TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    last_heartbeat   TEXT NOT NULL,
    lease_expires    TEXT NOT NULL,
    -- 'alive' processes renew their lease; 'dead' rows are kept for audit
    -- and so reaper decisions stay explainable.
    state            TEXT NOT NULL DEFAULT 'alive'
                     CHECK (state IN ('alive', 'dead')),
    tmp_dir_rel      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_instances_lease
    ON instances(state, lease_expires);

-- Every file that a process is writing (tmp file and its final destination)
-- is registered here before any byte hits disk. Reapers delete remnants only
-- for intents whose owning incarnation is dead / lease-expired.
CREATE TABLE IF NOT EXISTS write_intents (
    intent_id          TEXT PRIMARY KEY,
    owner_incarnation  TEXT NOT NULL,
    -- 'upload' or 'merge'
    kind               TEXT NOT NULL
                       CHECK (kind IN ('upload', 'merge')),
    tmp_rel            TEXT NOT NULL,
    final_rel          TEXT NOT NULL,
    state              TEXT NOT NULL DEFAULT 'active'
                       CHECK (state IN ('active', 'committed',
                                        'aborted', 'reclaimed')),
    created_at         TEXT NOT NULL,
    reclaimed_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_owner_state
    ON write_intents(state, owner_incarnation);

CREATE TABLE IF NOT EXISTS merges (
    merge_id       TEXT PRIMARY KEY,
    voyage_id      TEXT NOT NULL,
    state          TEXT NOT NULL
                   CHECK (state IN ('writing', 'committed', 'failed')),
    lease_owner    TEXT NOT NULL,
    -- Incarnation that owns the streaming work. NULL only for rows written
    -- by older builds; startup recovery fails those immediately.
    owner_incarnation TEXT,
    lease_expires  TEXT NOT NULL,
    heartbeat_at   TEXT NOT NULL,
    new_frag_id    TEXT,
    row_count      INTEGER,
    created_at     TEXT NOT NULL,
    finished_at    TEXT
);

-- At most one in-flight merge per voyage. Partial indexes are supported by
-- every SQLite version that supports WAL (3.8+).
CREATE UNIQUE INDEX IF NOT EXISTS idx_merges_one_active
    ON merges(voyage_id) WHERE state = 'writing';

CREATE TABLE IF NOT EXISTS merge_sources (
    merge_id   TEXT NOT NULL REFERENCES merges(merge_id),
    frag_id    TEXT NOT NULL REFERENCES fragments(frag_id),
    PRIMARY KEY (merge_id, frag_id)
);

CREATE TABLE IF NOT EXISTS export_pins (
    pin_id       TEXT PRIMARY KEY,
    voyage_id    TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    released     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pin_fragments (
    pin_id    TEXT NOT NULL REFERENCES export_pins(pin_id),
    frag_id   TEXT NOT NULL REFERENCES fragments(frag_id),
    PRIMARY KEY (pin_id, frag_id)
);

CREATE TABLE IF NOT EXISTS gc_queue (
    frag_id      TEXT PRIMARY KEY REFERENCES fragments(frag_id),
    queued_at    TEXT NOT NULL,
    delete_after TEXT NOT NULL,
    deleted      INTEGER NOT NULL DEFAULT 0,
    deleted_at   TEXT
);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        settings.db_path,
        timeout=settings.sqlite_timeout_seconds,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=%d" % int(settings.sqlite_timeout_seconds * 1000))
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def init_db() -> None:
    settings.data_root.mkdir(parents=True, exist_ok=True)
    settings.fragments_dir.mkdir(parents=True, exist_ok=True)
    settings.merged_dir.mkdir(parents=True, exist_ok=True)
    settings.tmp_dir.mkdir(parents=True, exist_ok=True)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(SCHEMA)
        # Lightweight migrations for databases created by older builds.
        if "owner_incarnation" not in _table_columns(conn, "merges"):
            conn.execute(
                "ALTER TABLE merges ADD COLUMN owner_incarnation TEXT")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit immediate transaction (takes the WAL write lock up front)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
