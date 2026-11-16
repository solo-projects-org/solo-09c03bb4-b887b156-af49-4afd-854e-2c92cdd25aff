"""Instance incarnation registry, write-intent tracking and remnant reaping.

Why this module exists
----------------------
Two API instances share one data directory. The naive "wipe tmp/ and fail all
writing merges at startup" recovery is wrong on a rolling restart: it deletes
the *live* peer's in-flight files and breaks its healthy merge lease.

The model used here:

1. Every process start registers a unique **incarnation** (even when
   ``INSTANCE_ID`` is reused) and keeps a lease alive with a heartbeat.
   SIGKILL stops the heartbeat, so the lease expiry proves the writer is dead.
2. Every file write is announced beforehand as a **write intent** owned by an
   incarnation. Reapers only touch files whose owner is provably dead
   (registry row marked dead, or lease expired).
3. Temp files live under ``tmp/<instance>-<incarnation>/`` so no instance ever
   removes another live instance's working directory.
4. Publish transactions fence on the still-active intent + live incarnation;
   a writer whose remnants were reaped cannot register the reclaimed file or
   overwrite a newer result.
"""
from __future__ import annotations

import logging
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .config import Settings, settings as default_settings
from .crashhooks import crash_if
from .db import connect, transaction

log = logging.getLogger("trackmerge")

_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_-]")

_lock = threading.RLock()
_incarnation: Optional[str] = None
_tmp_rel: Optional[str] = None
_bound_root: Optional[Path] = None
_heartbeat_thread: Optional["InstanceHeartbeat"] = None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _safe_instance_id(instance_id: str) -> str:
    safe = _SAFE_ID_RE.sub("_", (instance_id or "").strip())[:64]
    return safe or "inst"


def _resolve_under(root: Path, rel: str) -> Optional[Path]:
    """Resolve rel against root, rejecting path traversal escapes."""
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


def current_incarnation(cfg: Settings = default_settings) -> str:
    """Return this process' incarnation id, registering it on first use."""
    ensure_instance(cfg)
    assert _incarnation is not None
    return _incarnation


def incarnation_tmp_rel(cfg: Settings = default_settings) -> str:
    ensure_instance(cfg)
    assert _tmp_rel is not None
    return _tmp_rel


def incarnation_tmp_path(name: str, cfg: Settings = default_settings) -> str:
    """Absolute path for a temp file private to this incarnation."""
    base = _resolve_under(cfg.data_root, incarnation_tmp_rel(cfg))
    assert base is not None
    base.mkdir(parents=True, exist_ok=True)
    return str(base / name)


def ensure_instance(cfg: Settings = default_settings) -> str:
    """Register this incarnation once and start its lease heartbeat thread.

    The binding is keyed on DATA_ROOT: test fixtures point the settings
    singleton at a fresh temp directory between tests, which forces a fresh
    incarnation registration instead of reusing one tied to the old root.
    """
    global _incarnation, _tmp_rel, _heartbeat_thread, _bound_root
    with _lock:
        root = cfg.data_root.resolve()
        if _incarnation is not None and _bound_root == root:
            return _incarnation
        incarnation = str(uuid.uuid4())
        tmp_rel = f"tmp/{_safe_instance_id(cfg.instance_id)}-{incarnation[:8]}"
        tmp_dir = _resolve_under(cfg.data_root, tmp_rel)
        assert tmp_dir is not None
        tmp_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc)
        conn = connect()
        try:
            with transaction(conn):
                # A rolling restart reusing INSTANCE_ID supersedes the prior
                # incarnation immediately; its remnants become reclaimable.
                conn.execute(
                    "UPDATE instances SET state='dead' "
                    "WHERE instance_id=? AND state='alive' AND incarnation_id<>?",
                    (cfg.instance_id, incarnation))
                conn.execute(
                    "INSERT INTO instances(incarnation_id, instance_id, "
                    "started_at, last_heartbeat, lease_expires, state, "
                    "tmp_dir_rel) VALUES(?, ?, ?, ?, ?, 'alive', ?)",
                    (incarnation, cfg.instance_id, _iso(now), _iso(now),
                     _iso(now + timedelta(seconds=cfg.instance_lease_seconds)),
                     tmp_rel))
        finally:
            conn.close()
        _incarnation = incarnation
        _tmp_rel = tmp_rel
        _bound_root = root
        if _heartbeat_thread is not None:
            _heartbeat_thread.stop()
            _heartbeat_thread.join(timeout=5)
        _heartbeat_thread = InstanceHeartbeat(incarnation, cfg)
        _heartbeat_thread.start()
        log.info("registered incarnation %s (instance %s, tmp %s)",
                 incarnation[:8], cfg.instance_id, tmp_rel)
        return incarnation


def shutdown_instance(cfg: Settings = default_settings) -> None:
    """Mark this incarnation dead on graceful shutdown (best effort)."""
    global _heartbeat_thread
    with _lock:
        incarnation = _incarnation
        thread = _heartbeat_thread
        _heartbeat_thread = None
    if thread is not None:
        thread.stop()
        thread.join(timeout=10)
    if incarnation is None:
        return
    try:
        conn = connect()
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE instances SET state='dead' "
                    "WHERE incarnation_id=? AND state='alive'",
                    (incarnation,))
        finally:
            conn.close()
    except Exception:
        log.exception("could not mark incarnation %s dead", incarnation[:8])


class InstanceHeartbeat(threading.Thread):
    """Renews the incarnation lease while the process is alive."""

    def __init__(self, incarnation_id: str, cfg: Settings):
        super().__init__(name=f"inst-hb-{incarnation_id[:8]}", daemon=True)
        self.incarnation_id = incarnation_id
        self.cfg = cfg
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.wait(self.cfg.instance_heartbeat_seconds):
            try:
                conn = connect()
                try:
                    now = datetime.now(timezone.utc)
                    with transaction(conn):
                        conn.execute(
                            "UPDATE instances SET last_heartbeat=?, "
                            "lease_expires=? WHERE incarnation_id=? "
                            "AND state='alive'",
                            (_iso(now), _iso(now + timedelta(
                                seconds=self.cfg.instance_lease_seconds)),
                             self.incarnation_id))
                finally:
                    conn.close()
            except Exception:
                log.exception("instance heartbeat failed for %s",
                              self.incarnation_id[:8])

    def stop(self) -> None:
        self.stop_event.set()


# --------------------------------------------------------------------------- #
# write intents
# --------------------------------------------------------------------------- #

def register_intent(conn, kind: str, tmp_rel: str, final_rel: str,
                    cfg: Settings = default_settings) -> str:
    """Announce a file write. Must run inside an open transaction."""
    intent_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO write_intents(intent_id, owner_incarnation, kind, "
        "tmp_rel, final_rel, state, created_at) VALUES(?, ?, ?, ?, ?, "
        "'active', ?)",
        (intent_id, current_incarnation(cfg), kind, tmp_rel, final_rel,
         _iso(datetime.now(timezone.utc))))
    return intent_id


def abort_intent(intent_id: str, cfg: Settings = default_settings) -> None:
    """Best-effort cancellation of an intent owned by this process."""
    if not intent_id:
        return
    conn = connect()
    try:
        paths: list[str] = []
        with transaction(conn):
            row = conn.execute(
                "SELECT tmp_rel, final_rel FROM write_intents "
                "WHERE intent_id=? AND state='active'", (intent_id,)).fetchone()
            if row is None:
                return
            conn.execute(
                "UPDATE write_intents SET state='aborted' WHERE intent_id=?",
                (intent_id,))
            paths = [row["tmp_rel"], row["final_rel"]]
    finally:
        conn.close()
    for rel in paths:
        path = _resolve_under(cfg.data_root, rel)
        if path is not None:
            path.unlink(missing_ok=True)


def assert_publish_allowed(conn, intent_id: str, now_iso: str) -> None:
    """Fence used inside a publish transaction.

    A writer proceeds only while its incarnation lease is alive and its intent
    has not been reaped. If a reaper won the race, the writer fails its
    request instead of registering a deleted file or clobbering newer data.
    """
    incarnation = current_incarnation()
    live = conn.execute(
        "SELECT 1 FROM instances WHERE incarnation_id=? AND state='alive' "
        "AND lease_expires > ?", (incarnation, now_iso)).fetchone()
    intent = conn.execute(
        "SELECT state FROM write_intents WHERE intent_id=?",
        (intent_id,)).fetchone()
    if live is None or intent is None or intent["state"] != "active":
        raise _LeaseLost()


class _LeaseLost(Exception):
    """Raised internally when a publish fence fails; mapped by services."""


# --------------------------------------------------------------------------- #
# dead-owner detection and merge lease cleanup
# --------------------------------------------------------------------------- #

def expire_writing_merges(conn, now_iso: str) -> int:
    """Fail writing merges whose owner is dead / lease-expired / unknown.

    Healthy merges owned by a live, heartbeating incarnation are never touched
    here, so a peer starting up cannot break an in-flight merge.
    """
    cur = conn.execute(
        "UPDATE merges SET state='failed', finished_at=? "
        "WHERE state='writing' AND ("
        "  lease_expires <= ? "
        "  OR owner_incarnation IS NULL "
        "  OR NOT EXISTS ("
        "      SELECT 1 FROM instances i "
        "      WHERE i.incarnation_id = merges.owner_incarnation "
        "        AND i.state='alive' AND i.lease_expires > ?))",
        (now_iso, now_iso, now_iso))
    return cur.rowcount


def _dead_incarnation_ids(conn, now_iso: str) -> list[str]:
    return [r["incarnation_id"] for r in conn.execute(
        "SELECT incarnation_id FROM instances "
        "WHERE state='alive' AND lease_expires <= ?", (now_iso,))]


# --------------------------------------------------------------------------- #
# remnant reaper
# --------------------------------------------------------------------------- #

def reap_remnants(cfg: Settings = default_settings) -> dict:
    """Reclaim temp files / unpublished fragments of dead incarnations.

    Safe to run concurrently from both instances and repeatedly after a crash:
    each intent is claimed with a conditional UPDATE, so two reapers can never
    double-handle it. Files of live incarnations are never touched.
    """
    result = {"dead_instances": 0, "intents_reclaimed": 0,
              "files_removed": 0, "tmp_dirs_removed": 0,
              "legacy_tmp_removed": 0}
    now_iso = _iso(datetime.now(timezone.utc))

    conn = connect()
    try:
        # 0. Recover in-flight claims from a previous reaper that died between
        #    marking an intent reclaimed and finishing the file deletes: any
        #    reclaimed intent whose files still exist goes back to active so
        #    this run (or the next) retries it. Idempotent and crash-safe.
        with transaction(conn):
            stalled = conn.execute(
                "SELECT intent_id, tmp_rel, final_rel FROM write_intents "
                "WHERE state='reclaimed'").fetchall()
            for row in stalled:
                leftovers = []
                for rel in (row["tmp_rel"], row["final_rel"]):
                    path = _resolve_under(cfg.data_root, rel)
                    if path is not None and path.exists():
                        leftovers.append(path)
                if leftovers:
                    conn.execute(
                        "UPDATE write_intents SET state='active', "
                        "reclaimed_at=NULL WHERE intent_id=?",
                        (row["intent_id"],))
                # If neither file exists anymore, the delete completed before
                # the crash; the reclaimed state is already final.

        # 1. Expire leases of incarnations that stopped heartbeating.
        with transaction(conn):
            dead = _dead_incarnation_ids(conn, now_iso)
            if dead:
                conn.executemany(
                    "UPDATE instances SET state='dead' "
                    "WHERE incarnation_id=? AND state='alive'",
                    [(i,) for i in dead])
                result["dead_instances"] = len(dead)
            expire_writing_merges(conn, now_iso)
            # Housekeeping: drop registry rows of dead incarnations once they
            # no longer own any active write intent or writing merge, so the
            # table stays bounded across many rolling restarts.
            conn.execute(
                "DELETE FROM instances WHERE state='dead' AND NOT EXISTS ("
                "  SELECT 1 FROM write_intents w "
                "  WHERE w.owner_incarnation = instances.incarnation_id "
                "    AND w.state='active') AND NOT EXISTS ("
                "  SELECT 1 FROM merges m "
                "  WHERE m.owner_incarnation = instances.incarnation_id "
                "    AND m.state='writing')")

        # 2. Claim active intents without a live owner, one at a time. The
        #    files are deleted *before* the intent is marked reclaimed, so a
        #    crash or I/O error mid-delete leaves the intent active and the
        #    next run resumes; unlink(missing_ok=True) makes the retry safe.
        intents = conn.execute(
            "SELECT w.intent_id AS intent_id, w.tmp_rel AS tmp_rel, "
            "w.final_rel AS final_rel, w.owner_incarnation AS owner "
            "FROM write_intents w "
            "WHERE w.state='active' AND NOT EXISTS ("
            "  SELECT 1 FROM instances i "
            "  WHERE i.incarnation_id = w.owner_incarnation "
            "    AND i.state='alive' AND i.lease_expires > ?)",
            (now_iso,)).fetchall()
        for row in intents:
            with transaction(conn):
                claimed = conn.execute(
                    "UPDATE write_intents SET state='reclaimed', "
                    "reclaimed_at=? WHERE intent_id=? AND state='active'",
                    (now_iso, row["intent_id"])).rowcount
            if not claimed:
                continue  # another reaper or the writer's commit won the race
            crash_if("reap_delete")
            delete_error = False
            for rel in (row["tmp_rel"], row["final_rel"]):
                path = _resolve_under(cfg.data_root, rel)
                if path is None:
                    continue
                try:
                    if path.exists():
                        path.unlink(missing_ok=True)
                        result["files_removed"] += 1
                except OSError:
                    # Transient I/O failure: keep the intent active so a later
                    # cycle retries instead of leaking the file forever.
                    log.exception("reaper could not delete %s", path)
                    delete_error = True
            if delete_error:
                with transaction(conn):
                    conn.execute(
                        "UPDATE write_intents SET state='active', "
                        "reclaimed_at=NULL WHERE intent_id=? AND state='reclaimed'",
                        (row["intent_id"],))
                continue
            result["intents_reclaimed"] += 1
            log.warning("reclaimed intent %s of dead incarnation %s",
                        row["intent_id"][:8], row["owner"][:8])

        # 3. Remove temp directories of dead / unknown incarnations.
        if cfg.tmp_dir.exists():
            known = {r["incarnation_id"]: r["state"]
                     for r in conn.execute(
                         "SELECT incarnation_id, state FROM instances")}
            live_ids = {i for i, s in known.items() if s == "alive"}
            for entry in list(cfg.tmp_dir.iterdir()):
                if not entry.is_dir():
                    continue  # legacy flat files handled below
                suffix = entry.name.rsplit("-", 1)[-1]
                owner = next((i for i in known if i.startswith(suffix)), None)
                if owner is not None and owner in live_ids:
                    continue
                for f in entry.rglob("*"):
                    if f.is_file():
                        f.unlink(missing_ok=True)
                        result["files_removed"] += 1
                try:
                    entry.rmdir()
                    result["tmp_dirs_removed"] += 1
                except OSError:
                    pass

            # 4. Legacy flat tmp files (pre-upgrade layout): only remove ones
            #    older than the incarnation lease window.
            cutoff = datetime.now(timezone.utc) - timedelta(
                seconds=cfg.instance_lease_seconds)
            for entry in list(cfg.tmp_dir.iterdir()):
                if not entry.is_file():
                    continue
                mtime = datetime.fromtimestamp(
                    entry.stat().st_mtime, tz=timezone.utc)
                if mtime <= cutoff:
                    entry.unlink(missing_ok=True)
                    result["legacy_tmp_removed"] += 1
    finally:
        conn.close()
    return result


def is_published_or_intended(conn, rel_path: str) -> bool:
    """True if rel_path is a registered fragment or a live write target."""
    row = conn.execute(
        "SELECT 1 FROM fragments WHERE rel_path=? "
        "UNION ALL "
        "SELECT 1 FROM write_intents WHERE final_rel=? AND state='active' "
        "LIMIT 1", (rel_path, rel_path)).fetchone()
    return row is not None
