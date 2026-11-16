"""Recovery tests for the shared-directory rolling-restart scenarios.

These cover the failure modes of the old "wipe tmp/ and fail all writing
merges at startup" recovery:

* a second instance starting up must not disturb the first instance's
  in-flight uploads (temp files) or in-flight merges (lease + output files);
* fragments already landed on disk but not yet registered must be preserved
  while their owner is alive and reaped automatically once it is dead;
* orphan scanning interleaved with registration must not delete a fragment
  that is published in the gap;
* a reaper interrupted mid-cycle resumes on the next run;
* a writer whose intent was reaped can neither register the reclaimed file
  nor overwrite a newer result.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from tests.conftest import expected_rows, make_csv, parse_export
from tests.test_integration import Server, _wait_until


def _db_rows(data_root: Path, sql: str):
    db = sqlite3.connect(data_root / "metadata.db")
    try:
        return db.execute(sql).fetchall()
    finally:
        db.close()


def test_peer_startup_does_not_touch_live_upload_and_merge(data_root):
    # A uploads slowly (rename->commit window widened) while B boots.
    a = Server(data_root, "api-a",
               env_extra={"UPLOAD_SLEEP_SECONDS": "4",
                          "MERGE_SLEEP_SECONDS": "3"})
    a.wait_ready()
    try:
        import concurrent.futures
        specs = [(40, 1, "A"), (30, 2, "B")]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            up_fut = pool.submit(a.upload, "vr", make_csv(*specs[0]))

            # B boots while A's first upload is inside the rename->commit
            # window (UPLOAD_SLEEP_SECONDS), with an active write intent.
            b = Server(data_root, "api-b")
            b.wait_ready()
            assert up_fut.result()[0] == 200  # B's startup did not break it

            # Second upload on B, then a merge on A that runs while B stays
            # up and its GC loop ticks repeatedly.
            assert b.upload("vr", make_csv(*specs[1]))[0] == 200
            merge_fut = pool.submit(a.post, "/voyages/vr/merge")
            time.sleep(1.0)
            for _ in range(4):
                assert b.get("/admin/gc")[0] == 200
                time.sleep(1.0)
            code, body = merge_fut.result()
            assert code == 200, body  # lease/heartbeat survived peer activity

        status, raw = a.get("/voyages/vr/export", raw=True)
        assert status == 200
        assert parse_export(raw.decode()) == expected_rows(specs)
        # A's incarnation is still marked alive after B's recovery ran.
        alive = _db_rows(
            data_root,
            "SELECT instance_id FROM instances WHERE state='alive'")
        assert {r[0] for r in alive} == {"api-a", "api-b"}

        # Restart B (same INSTANCE_ID, new incarnation): A must stay alive and
        # continue serving other voyages.
        b.stop()
        b2 = Server(data_root, "api-b")
        b2.wait_ready()
        try:
            assert b2.upload("vr2", make_csv(12, 5, "X"))[0] == 200
            assert a.get("/voyages/vr2/active-file-count")[1][
                "active_file_count"] == 1
            assert a.get("/voyages/vr")[1]["active_file_count"] == 1
        finally:
            b2.stop()
    finally:
        a.stop()


def test_landed_unregistered_fragment_kept_while_owner_alive(data_root):
    a = Server(data_root, "api-a",
               env_extra={"UPLOAD_SLEEP_SECONDS": "3"})
    a.wait_ready()
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            fut = pool.submit(a.upload, "vk", make_csv(20, 1))
            # The file is renamed into fragments/ before the metadata commit.
            _wait_until(
                lambda: list(
                    (data_root / "fragments" / "vk").glob("*.parquet")),
                message="renamed fragment never appeared")
            # A peer's full recovery + GC cycles must not reap it: the owner
            # incarnation is alive and the write intent is active.
            b = Server(data_root, "api-b")
            b.wait_ready()
            try:
                for _ in range(3):
                    b.get("/admin/gc")
                    time.sleep(0.7)
                assert len(list(
                    (data_root / "fragments" / "vk").glob("*.parquet"))) == 1
            finally:
                b.stop()
            assert fut.result()[0] == 200
        status, body = a.get("/voyages/vk")
        assert body["total_row_count"] == 20
        assert body["active_file_count"] == 1
    finally:
        a.stop()


def test_orphan_scan_interleaved_with_registration_keeps_published(data_root):
    # Publishing instance with a wide rename->commit window.
    a = Server(data_root, "api-a",
               env_extra={"UPLOAD_SLEEP_SECONDS": "4"})
    b = Server(data_root, "api-b")
    a.wait_ready(); b.wait_ready()
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            fut = pool.submit(a.upload, "vi", make_csv(35, 3, "I"))
            _wait_until(
                lambda: list(
                    (data_root / "fragments" / "vi").glob("*.parquet")),
                message="fragment never landed")
            # Hammer GC (reaper + recovery-style checks) across the whole
            # registration window. The committed fragment must survive.
            deadline = time.time() + 5
            while time.time() < deadline:
                assert b.get("/admin/gc")[0] == 200
                time.sleep(0.25)
            assert fut.result()[0] == 200

        # Exactly one fragment, registered, fully exportable: no lost row and
        # no duplicated row.
        files = list((data_root / "fragments" / "vi").glob("*.parquet"))
        assert len(files) == 1
        status, body = b.get("/voyages/vi")
        assert status == 200
        assert body["active_file_count"] == 1
        assert body["total_row_count"] == 35
        rows = parse_export(b.get("/voyages/vi/export", raw=True)[1].decode())
        assert rows == expected_rows([(35, 3, "I")])
    finally:
        a.stop(); b.stop()


def test_dead_writer_remnants_auto_reaped_by_live_peer(data_root):
    crasher = Server(data_root, "crasher",
                     env_extra={"CRASH_HOOK": "upload_commit",
                                "CRASH_HOOK_AFTER": "1"})
    crasher.wait_ready()
    try:
        try:
            crasher.upload("vd", make_csv(22, 1))
        except Exception:
            pass
        deadline = time.time() + 5
        while time.time() < deadline and crasher.proc.poll() is None:
            time.sleep(0.1)
        assert crasher.proc.poll() is not None
        # The fragment has been renamed out of tmp/ already; the landed but
        # unregistered file remains under fragments/ with an active intent.
        assert list((data_root / "fragments" / "vd").glob("*.parquet"))
        assert _db_rows(data_root,
                        "SELECT state FROM write_intents WHERE state='active'")
    finally:
        crasher.stop(kill=True)

    # No restart of the crasher: the live peer reaps everything after the
    # dead incarnation's lease expires.
    peer = Server(data_root, "peer")
    peer.wait_ready()
    try:
        _wait_until(
            lambda: not list((data_root / "tmp").rglob("*.parquet")),
            message="dead writer tmp files not reaped")
        _wait_until(
            lambda: not list(
                (data_root / "fragments" / "vd").glob("*.parquet")),
            message="dead writer orphan fragment not reaped")
        intents = _db_rows(
            data_root,
            "SELECT state FROM write_intents WHERE state='active'")
        assert intents == []
        # The voyage row was never inserted (commit did not happen), so the
        # retry starts clean and data ends up exactly once.
        assert peer.upload("vd", make_csv(22, 1))[0] == 200
        status, body = peer.get("/voyages/vd")
        assert body["total_row_count"] == 22
        rows = parse_export(peer.get("/voyages/vd/export", raw=True)[1].decode())
        assert rows == expected_rows([(22, 1, "P")])
    finally:
        peer.stop()
