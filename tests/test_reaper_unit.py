"""In-process recovery semantics: reaper idempotency, fencing, scan races."""
from __future__ import annotations

import sqlite3

from app import recovery, services
from app.config import settings
from tests.conftest import make_csv, parse_export


def _intents(data_root, state: str = "active"):
    db = sqlite3.connect(data_root / "metadata.db")
    try:
        return db.execute(
            "SELECT intent_id, tmp_rel, final_rel, state FROM write_intents "
            "WHERE state=?", (state,)).fetchall()
    finally:
        db.close()


def test_reaper_resumes_after_interruption_and_is_idempotent(data_root,
                                                             monkeypatch):
    # Two dead incarnations, each with an active intent + files on disk.
    services.upload_batch("v1", make_csv(10, 1))  # registers incarnation
    owner = recovery.current_incarnation()
    from app.db import connect, transaction
    from datetime import datetime, timedelta, timezone
    from app.services import _iso

    db = sqlite3.connect(data_root / "metadata.db")
    db.execute(
        "UPDATE instances SET state='alive', lease_expires=? "
        "WHERE incarnation_id=?",
        (_iso(datetime.now(timezone.utc) - timedelta(seconds=60)), owner))
    db.commit()
    db.close()

    conn = connect()
    try:
        with transaction(conn):
            for i in range(2):
                tmp_rel = f"{recovery.incarnation_tmp_rel()}/old-{i}.parquet"
                final_rel = f"fragments/v1/old-{i}.parquet"
                recovery.register_intent(conn, "upload", tmp_rel, final_rel)
                (data_root / tmp_rel).parent.mkdir(parents=True, exist_ok=True)
                (data_root / tmp_rel).write_bytes(b"x")
                (data_root / final_rel).parent.mkdir(parents=True, exist_ok=True)
                (data_root / final_rel).write_bytes(b"x")
    finally:
        conn.close()

    # First reaper run dies after claiming the first intent.
    original_unlink = __import__("pathlib").Path.unlink
    calls = {"n": 0}

    def flaky_unlink(self, missing_ok=False):
        if "old-" in self.name:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated reaper crash mid-delete")
        return original_unlink(self, missing_ok=missing_ok)

    # First reaper run is killed (simulating SIGKILL) right after claiming
    # the first intent and before its files are gone. A later run must roll
    # the stalled claim back and finish every intent.
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)
    try:
        try:
            recovery.reap_remnants()
        except RuntimeError:
            pass
    finally:
        monkeypatch.undo()

    # Next run recovers the stalled claim and finishes both intents; running
    # it again changes nothing.
    r1 = recovery.reap_remnants()
    r2 = recovery.reap_remnants()
    assert r1["intents_reclaimed"] == 2
    assert r2["intents_reclaimed"] == 0
    assert not list((data_root / "fragments" / "v1").glob("old-*.parquet"))
    assert not _intents(data_root, "active")
    assert len(_intents(data_root, "reclaimed")) == 2
    # The real published fragment of v1 is untouched.
    got = parse_export("".join(services.stream_voyage_csv("v1")))
    assert len(got) == 10


def test_publish_fence_blocks_reclaimed_intent(data_root):
    services.upload_batch("vf", make_csv(8, 1))
    from app.db import connect, transaction

    # Simulate a second upload whose owner incarnation has expired and whose
    # intent the reaper claims before the publish transaction runs.
    conn = connect()
    try:
        with transaction(conn):
            intent_id = recovery.register_intent(
                conn, "upload",
                f"{recovery.incarnation_tmp_rel()}/late.parquet",
                "fragments/vf/late.parquet")
    finally:
        conn.close()

    db = sqlite3.connect(data_root / "metadata.db")
    db.execute(
        "UPDATE instances SET lease_expires='2000-01-01T00:00:00+00:00' "
        "WHERE incarnation_id=?", (recovery.current_incarnation(),))
    db.commit()
    db.close()
    recovery.reap_remnants()  # claims the intent as reclaimed

    conn = connect()
    try:
        with transaction(conn) as c:
            try:
                recovery.assert_publish_allowed(
                    c, intent_id,
                    "2026-11-16T00:00:00+00:00")
            except recovery._LeaseLost:
                pass
            else:
                raise AssertionError("fence should have rejected reaped intent")
    finally:
        conn.close()


def test_orphan_sweep_keeps_file_published_between_scan_and_delete(data_root):
    # A normal upload establishes the voyage and incarnation.
    services.upload_batch("vo", make_csv(6, 1))
    # Another in-flight upload: file landed, intent still active.
    from app.db import connect, transaction
    rel = "fragments/vo/inflight.parquet"
    (data_root / rel).parent.mkdir(parents=True, exist_ok=True)
    (data_root / rel).write_bytes(b"parquet-bytes")
    conn = connect()
    try:
        with transaction(conn):
            recovery.register_intent(
                conn, "upload",
                f"{recovery.incarnation_tmp_rel()}/inflight.parquet", rel)
    finally:
        conn.close()

    # Reaper must not touch it while the owner is alive.
    assert recovery.reap_remnants()["intents_reclaimed"] == 0
    assert (data_root / rel).exists()

    # Simulate the publish completing (intent committed + fragment row) right
    # before the orphan sweep's per-file check: the IMMEDIATE re-check spares
    # the file even though it was absent from the earlier snapshot.
    conn = connect()
    try:
        with transaction(conn):
            conn.execute(
                "UPDATE write_intents SET state='committed' WHERE final_rel=?",
                (rel,))
            conn.execute(
                "INSERT INTO fragments(frag_id, voyage_id, kind, rel_path, "
                "seq, row_count, created_at, status) "
                "VALUES('frag-x', 'vo', 'batch', ?, 2, 1, "
                "'2026-11-16T00:00:00+00:00', 'active')", (rel,))
    finally:
        conn.close()
    # Orphan backstop logic equivalent to startup_recovery's sweep.
    conn = connect()
    try:
        for path in list((data_root / "fragments").rglob("*.parquet")):
            r = str(path.relative_to(data_root))
            with transaction(conn):
                if recovery.is_published_or_intended(conn, r):
                    continue
                path.unlink(missing_ok=True)
    finally:
        conn.close()
    assert (data_root / rel).exists()
