"""Domain services: upload, merge, export pinning, garbage collection, recovery.

Design invariants
-----------------
* Parquet files are immutable once their metadata row is published.
* "Publishing" (new fragment becomes active / old fragments become
  superseded) happens inside one SQLite transaction. A crash before COMMIT
  leaves the previously published fragments fully usable.
* Files referenced by an active export pin are never deleted.
* No operation reads an entire voyage into memory.
"""
from __future__ import annotations

import logging
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

from . import storage
from .config import Settings, settings as default_settings
from .crashhooks import crash_if
from .db import connect, transaction
from .storage import (CSVValidationError, batch_to_csv_lines, format_csv_header,
                      merge_parquet_files, parquet_row_count, parse_csv,
                      write_parquet_atomic)

log = logging.getLogger("trackmerge")

VOYAGE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class ServiceError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _abs_path(rel_path: str, cfg: Settings) -> str:
    return str(cfg.data_root / rel_path)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def validate_voyage_id(voyage_id: str) -> None:
    if not VOYAGE_ID_RE.fullmatch(voyage_id):
        raise ServiceError(422, "voyage_id 仅允许 1-128 位字母、数字、下划线或连字符")


def _expire_writing_merges(conn, now_iso: str) -> int:
    """Mark writing merges whose lease has expired as failed. Returns count."""
    cur = conn.execute(
        "UPDATE merges SET state='failed', finished_at=? "
        "WHERE state='writing' AND lease_expires <= ?",
        (now_iso, now_iso),
    )
    return cur.rowcount


# --------------------------------------------------------------------------- #
# startup recovery
# --------------------------------------------------------------------------- #

def startup_recovery(cfg: Settings = default_settings) -> dict:
    """Clean up remnants of crashed processes. Safe to run on every instance."""
    result = {"tmp_files_removed": 0, "orphan_files_removed": 0,
              "writing_merges_failed": 0, "gc_entries_completed": 0}

    # 1. Every file in tmp/ is by definition an unpublished remnant.
    if cfg.tmp_dir.exists():
        for entry in cfg.tmp_dir.iterdir():
            if entry.is_file():
                entry.unlink(missing_ok=True)
                result["tmp_files_removed"] += 1

    conn = connect()
    try:
        with transaction(conn):
            now = _iso(datetime.now(timezone.utc))
            cur = conn.execute(
                "UPDATE merges SET state='failed', finished_at=? "
                "WHERE state='writing'", (now,))
            result["writing_merges_failed"] = cur.rowcount

            known = {r["rel_path"] for r in conn.execute(
                "SELECT rel_path FROM fragments")}

        # 2. Orphan files: on disk but never published (crash between rename
        #    and metadata commit). Scan outside the write transaction.
        for directory in (cfg.fragments_dir, cfg.merged_dir):
            for path in directory.rglob("*.parquet"):
                rel = str(path.relative_to(cfg.data_root))
                if rel not in known:
                    path.unlink(missing_ok=True)
                    result["orphan_files_removed"] += 1

        # 3. GC queue entries whose file is already gone are complete; the
        #    rest will be handled by the background GC loop.
        with transaction(conn):
            for row in conn.execute(
                    "SELECT g.frag_id AS frag_id, f.rel_path AS rel_path "
                    "FROM gc_queue g "
                    "JOIN fragments f ON f.frag_id = g.frag_id "
                    "WHERE g.deleted = 0"):
                if not Path(_abs_path(row["rel_path"], cfg)).exists():
                    conn.execute(
                        "UPDATE gc_queue SET deleted=1, deleted_at=? WHERE frag_id=?",
                        (_iso(datetime.now(timezone.utc)), row["frag_id"]))
                    result["gc_entries_completed"] += 1
    finally:
        conn.close()

    if any(result.values()):
        log.warning("startup recovery: %s", result)
    return result


# --------------------------------------------------------------------------- #
# upload
# --------------------------------------------------------------------------- #

def upload_batch(voyage_id: str, csv_text: str,
                 cfg: Settings = default_settings) -> dict:
    validate_voyage_id(voyage_id)
    try:
        parsed = parse_csv(csv_text, cfg.max_rows_per_batch)
    except CSVValidationError as exc:
        raise ServiceError(422, str(exc)) from exc

    now = datetime.now(timezone.utc)
    now_iso = _iso(now)
    batch_id = str(uuid.uuid4())
    frag_id = str(uuid.uuid4())

    frag_dir = cfg.fragments_dir / voyage_id
    frag_dir.mkdir(parents=True, exist_ok=True)
    rel_path = f"fragments/{voyage_id}/{frag_id}.parquet"
    final_path = _abs_path(rel_path, cfg)
    tmp_path = _abs_path(f"tmp/upload-{batch_id}.parquet", cfg)

    # Write the immutable fragment first; a crash here leaves only a tmp file
    # (cleaned on restart) and no metadata, so the client can safely retry.
    written = write_parquet_atomic(
        parsed.rows, tmp_path, final_path, cfg.parquet_batch_size)
    crash_if("upload_commit")

    conn = connect()
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO voyages(voyage_id, created_at, total_row_count) "
                "VALUES(?, ?, 0) ON CONFLICT(voyage_id) DO UPDATE SET "
                "voyage_id=voyage_id", (voyage_id, now_iso))
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM fragments "
                "WHERE voyage_id=?", (voyage_id,)).fetchone()
            seq = row["max_seq"] + 1
            conn.execute(
                "INSERT INTO fragments(frag_id, voyage_id, kind, rel_path, "
                "seq, row_count, created_at, status) "
                "VALUES(?, ?, 'batch', ?, ?, ?, ?, 'active')",
                (frag_id, voyage_id, rel_path, seq, written, now_iso))
            conn.execute(
                "INSERT INTO upload_batches(batch_id, voyage_id, frag_id, "
                "row_count, received_at) VALUES(?, ?, ?, ?, ?)",
                (batch_id, voyage_id, frag_id, written, now_iso))
            conn.execute(
                "UPDATE voyages SET total_row_count = total_row_count + ? "
                "WHERE voyage_id=?", (written, voyage_id))
        # A crash between the rename above and this commit leaves an orphan
        # file that startup recovery removes; the upload was never acked.
    finally:
        conn.close()

    return {"batch_id": batch_id, "frag_id": frag_id, "voyage_id": voyage_id,
            "row_count": written, "active_files": active_file_count(voyage_id, cfg)}


# --------------------------------------------------------------------------- #
# merge (manual compaction)
# --------------------------------------------------------------------------- #

def _lease_expiry(cfg: Settings, now: datetime) -> str:
    return _iso(now + timedelta(seconds=cfg.merge_lease_seconds))


class _Heartbeat(threading.Thread):
    """Keeps the merge lease alive while streaming runs."""

    def __init__(self, merge_id: str, cfg: Settings):
        super().__init__(name=f"hb-{merge_id[:8]}", daemon=True)
        self.merge_id = merge_id
        self.cfg = cfg
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.wait(self.cfg.heartbeat_interval_seconds):
            try:
                conn = connect()
                try:
                    now = datetime.now(timezone.utc)
                    with transaction(conn):
                        conn.execute(
                            "UPDATE merges SET lease_expires=?, heartbeat_at=? "
                            "WHERE merge_id=? AND state='writing'",
                            (_iso(now + timedelta(
                                seconds=self.cfg.merge_lease_seconds)),
                             _iso(now), self.merge_id))
                finally:
                    conn.close()
            except Exception:
                log.exception("merge heartbeat failed for %s", self.merge_id)

    def stop(self) -> None:
        self.stop_event.set()


def merge_voyage(voyage_id: str, cfg: Settings = default_settings) -> dict:
    validate_voyage_id(voyage_id)
    merge_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    conn = connect()
    try:
        # ---- acquire: snapshot the active fragments at merge start -------
        with transaction(conn):
            now_iso = _iso(now)
            _expire_writing_merges(conn, now_iso)
            blocked = conn.execute(
                "SELECT merge_id FROM merges WHERE voyage_id=? AND state='writing'",
                (voyage_id,)).fetchone()
            if blocked is not None:
                raise ServiceError(409,
                    f"航次 {voyage_id} 已有进行中的合并 {blocked['merge_id']}")
            voyage = conn.execute(
                "SELECT voyage_id FROM voyages WHERE voyage_id=?",
                (voyage_id,)).fetchone()
            if voyage is None:
                raise ServiceError(404, f"航次 {voyage_id} 不存在")
            sources = conn.execute(
                "SELECT frag_id, rel_path, row_count FROM fragments "
                "WHERE voyage_id=? AND status='active' ORDER BY seq",
                (voyage_id,)).fetchall()
            if len(sources) < 2:
                raise ServiceError(422,
                    f"航次 {voyage_id} 活动片段数为 {len(sources)}，无需合并")
            conn.execute(
                "INSERT INTO merges(merge_id, voyage_id, state, lease_owner, "
                "lease_expires, heartbeat_at, created_at) "
                "VALUES(?, ?, 'writing', ?, ?, ?, ?)",
                (merge_id, voyage_id, cfg.instance_id,
                 _lease_expiry(cfg, now), _iso(now), _iso(now)))
            source_ids = [r["frag_id"] for r in sources]
            source_paths = [_abs_path(r["rel_path"], cfg) for r in sources]
            expected_rows = sum(r["row_count"] for r in sources)
    finally:
        conn.close()

    # ---- stream fragments into one new Parquet file (bounded memory) -----
    merged_dir = cfg.merged_dir / voyage_id
    merged_dir.mkdir(parents=True, exist_ok=True)
    new_rel = f"merged/{voyage_id}/{merge_id}.parquet"
    final_path = _abs_path(new_rel, cfg)
    tmp_path = _abs_path(f"tmp/merge-{merge_id}.parquet", cfg)

    hb = _Heartbeat(merge_id, cfg)
    hb.start()
    stream_failed = False
    if cfg.merge_sleep_seconds > 0:
        import time as _time
        _time.sleep(cfg.merge_sleep_seconds)
    try:
        try:
            written = merge_parquet_files(
                source_paths, tmp_path, final_path, cfg.parquet_batch_size)
            actual_rows = parquet_row_count(final_path)
            if actual_rows != expected_rows or written != expected_rows:
                stream_failed = True
                raise ServiceError(500,
                    f"合并行数校验失败: 期望 {expected_rows}，写出 {written}，"
                    f"文件 {actual_rows}")
            crash_if("merge_file")

            # ---- publish atomically: switch registry in one transaction --
            conn = connect()
            try:
                with transaction(conn):
                    now2 = _iso(datetime.now(timezone.utc))
                    _expire_writing_merges(conn, now2)
                    mine = conn.execute(
                        "SELECT state FROM merges WHERE merge_id=?",
                        (merge_id,)).fetchone()
                    if mine is None or mine["state"] != "writing":
                        raise ServiceError(409,
                            "合并租约已失效（可能被另一实例接管），本次发布取消")
                    # New batches arriving meanwhile get seq beyond the
                    # sources; they must remain active and must not be touched.
                    changed = conn.execute(
                        "SELECT frag_id FROM fragments "
                        "WHERE frag_id IN (%s) AND status != 'active'"
                        % ",".join("?" * len(source_ids)), source_ids).fetchall()
                    if changed:
                        raise ServiceError(409,
                            "合并期间源片段状态发生变化，检测到并发冲突，已取消")
                    # The merged fragment inherits the position of the
                    # fragments it replaces (min source seq). Batches that
                    # arrived during the merge keep their larger seq, so they
                    # naturally trail the merged fragment in export order.
                    min_seq = conn.execute(
                        "SELECT MIN(seq) AS m FROM fragments "
                        "WHERE frag_id IN (%s)"
                        % ",".join("?" * len(source_ids)),
                        source_ids).fetchone()["m"]
                    delete_after = _iso(datetime.now(timezone.utc)
                                        + timedelta(seconds=cfg.gc_grace_seconds))
                    # Supersede first so their active seq positions are freed;
                    # the merged fragment then reuses min source seq. All of
                    # this is one transaction, so readers never see the gap.
                    conn.executemany(
                        "UPDATE fragments SET status='superseded' "
                        "WHERE frag_id=?",
                        [(fid,) for fid in source_ids])
                    new_frag_id = str(uuid.uuid4())
                    conn.execute(
                        "INSERT INTO fragments(frag_id, voyage_id, kind, "
                        "rel_path, seq, row_count, created_at, status) "
                        "VALUES(?, ?, 'merged', ?, ?, ?, ?, 'active')",
                        (new_frag_id, voyage_id, new_rel, min_seq,
                         expected_rows, now2))
                    conn.executemany(
                        "INSERT INTO gc_queue(frag_id, queued_at, delete_after) "
                        "VALUES(?, ?, ?)",
                        [(fid, now2, delete_after) for fid in source_ids])
                    conn.executemany(
                        "INSERT INTO merge_sources(merge_id, frag_id) "
                        "VALUES(?, ?)",
                        [(merge_id, fid) for fid in source_ids])
                    conn.execute(
                        "UPDATE merges SET state='committed', new_frag_id=?, "
                        "row_count=?, finished_at=? WHERE merge_id=?",
                        (new_frag_id, expected_rows, now2, merge_id))
                    crash_if("merge_publish")
                crash_if("merge_after_commit")
            finally:
                conn.close()
        except ServiceError:
            # Stream/verify failures mark our merge failed; publish-time
            # conflicts raised inside the transaction leave the merge row
            # untouched (still 'writing', lease valid or already expired).
            if stream_failed:
                _fail_merge(merge_id, cfg)
            raise
        except BaseException:
            stream_failed = True
            _fail_merge(merge_id, cfg)
            raise
    finally:
        hb.stop()
        hb.join(timeout=10)

    return {"merge_id": merge_id, "voyage_id": voyage_id,
            "merged_fragment": new_frag_id, "source_fragments": source_ids,
            "row_count": expected_rows,
            "active_files": active_file_count(voyage_id, cfg)}


def _fail_merge(merge_id: str, cfg: Settings) -> None:
    try:
        conn = connect()
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE merges SET state='failed', "
                    "finished_at=? WHERE merge_id=? AND state='writing'",
                    (_iso(datetime.now(timezone.utc)), merge_id))
        finally:
            conn.close()
    except Exception:
        log.exception("could not mark merge %s failed", merge_id)
    # Any leftover tmp file / unpublished merged file is reclaimed at startup
    # (or as an orphan since no fragment row references it).


# --------------------------------------------------------------------------- #
# export
# --------------------------------------------------------------------------- #

def pin_active_fragments(voyage_id: str,
                         cfg: Settings = default_settings
                         ) -> Tuple[str, List[dict], int]:
    validate_voyage_id(voyage_id)
    pin_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    conn = connect()
    try:
        with transaction(conn):
            voyage = conn.execute(
                "SELECT voyage_id FROM voyages WHERE voyage_id=?",
                (voyage_id,)).fetchone()
            if voyage is None:
                raise ServiceError(404, f"航次 {voyage_id} 不存在")
            fragments = conn.execute(
                "SELECT frag_id, rel_path FROM fragments "
                "WHERE voyage_id=? AND status='active' ORDER BY seq",
                (voyage_id,)).fetchall()
            conn.execute(
                "INSERT INTO export_pins(pin_id, voyage_id, created_at, "
                "expires_at) VALUES(?, ?, ?, ?)",
                (pin_id, voyage_id, _iso(now),
                 _iso(now + timedelta(seconds=cfg.pin_ttl_seconds))))
            conn.executemany(
                "INSERT INTO pin_fragments(pin_id, frag_id) VALUES(?, ?)",
                [(pin_id, r["frag_id"]) for r in fragments])
        paths = [_abs_path(r["rel_path"], cfg) for r in fragments]
        total = sum(parquet_row_count(p) for p in paths)
        return pin_id, [{"frag_id": r["frag_id"], "path": _abs_path(r["rel_path"], cfg)}
                        for r in fragments], total
    finally:
        conn.close()


def release_pin(pin_id: str) -> None:
    try:
        conn = connect()
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE export_pins SET released=1 WHERE pin_id=?",
                    (pin_id,))
        finally:
            conn.close()
    except Exception:
        log.exception("failed to release pin %s", pin_id)


def stream_voyage_csv(voyage_id: str, cfg: Settings = default_settings):
    """Generator yielding the voyage CSV in bounded-memory chunks.

    Pins the active fragment set first, so concurrent merges cannot remove any
    file this export still reads. The pin is released when streaming ends
    (including client disconnect); an expired pin also unblocks GC.
    """
    pin_id, fragments, total = pin_active_fragments(voyage_id, cfg)
    try:
        yield "﻿" + format_csv_header()
        for index, frag in enumerate(fragments):
            if index > 0 and cfg.export_sleep_seconds > 0:
                import time as _time
                _time.sleep(cfg.export_sleep_seconds)
            for batch in storage.iter_parquet_batches(
                    frag["path"], cfg.parquet_batch_size):
                for chunk in batch_to_csv_lines(batch):
                    yield chunk
    finally:
        release_pin(pin_id)


# --------------------------------------------------------------------------- #
# queries
# --------------------------------------------------------------------------- #

def active_file_count(voyage_id: str, cfg: Settings = default_settings) -> int:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM fragments "
            "WHERE voyage_id=? AND status='active'", (voyage_id,)).fetchone()
        return int(row["c"])
    finally:
        conn.close()


def voyage_status(voyage_id: str, cfg: Settings = default_settings) -> dict:
    conn = connect()
    try:
        voyage = conn.execute(
            "SELECT * FROM voyages WHERE voyage_id=?", (voyage_id,)).fetchone()
        if voyage is None:
            raise ServiceError(404, f"航次 {voyage_id} 不存在")
        fragments = conn.execute(
            "SELECT frag_id, kind, seq, row_count, status, created_at "
            "FROM fragments WHERE voyage_id=? ORDER BY seq",
            (voyage_id,)).fetchall()
        active = [f for f in fragments if f["status"] == "active"]
        return {
            "voyage_id": voyage_id,
            "created_at": voyage["created_at"],
            "total_row_count": voyage["total_row_count"],
            "active_file_count": len(active),
            "active_row_count": sum(f["row_count"] for f in active),
            "fragments": [dict(f) for f in fragments],
        }
    finally:
        conn.close()


def list_voyages(cfg: Settings = default_settings) -> List[dict]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT v.voyage_id, v.created_at, v.total_row_count, "
            "COUNT(CASE WHEN f.status='active' THEN 1 END) AS active_file_count "
            "FROM voyages v LEFT JOIN fragments f ON f.voyage_id = v.voyage_id "
            "GROUP BY v.voyage_id ORDER BY v.created_at").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# garbage collection
# --------------------------------------------------------------------------- #

def run_gc_once(cfg: Settings = default_settings) -> dict:
    """Delete superseded files past their grace time and not pinned."""
    result = {"expired_pins": 0, "files_deleted": 0, "bytes_reclaimed": 0}
    conn = connect()
    try:
        with transaction(conn):
            now = _iso(datetime.now(timezone.utc))
            cur = conn.execute(
                "UPDATE export_pins SET released=1 WHERE released=0 AND expires_at<=?",
                (now,))
            result["expired_pins"] = cur.rowcount

            candidates = conn.execute(
                "SELECT g.frag_id AS frag_id, f.rel_path AS rel_path "
                "FROM gc_queue g "
                "JOIN fragments f ON f.frag_id = g.frag_id "
                "WHERE g.deleted=0 AND g.delete_after<=? "
                "AND NOT EXISTS (SELECT 1 FROM pin_fragments pf "
                "  JOIN export_pins p ON p.pin_id = pf.pin_id "
                "  WHERE pf.frag_id = g.frag_id AND p.released=0)",
                (now,)).fetchall()

        for cand in candidates:
            path = Path(_abs_path(cand["rel_path"], cfg))
            crash_if("gc_delete")
            size = path.stat().st_size if path.exists() else 0
            path.unlink(missing_ok=True)
            with transaction(conn):
                conn.execute(
                    "UPDATE gc_queue SET deleted=1, deleted_at=? WHERE frag_id=?",
                    (_iso(datetime.now(timezone.utc)), cand["frag_id"]))
            result["files_deleted"] += 1
            result["bytes_reclaimed"] += size
    finally:
        conn.close()
    return result


def gc_loop(cfg: Settings = default_settings) -> None:
    import time
    while True:
        try:
            run_gc_once(cfg)
        except Exception:
            log.exception("GC loop error")
        time.sleep(cfg.gc_interval_seconds)
