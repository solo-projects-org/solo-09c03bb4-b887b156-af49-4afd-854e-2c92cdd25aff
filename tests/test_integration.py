"""Integration tests with real uvicorn subprocesses.

Covers the durability requirements that an in-process test cannot exercise:
* SIGKILL at every crash point (tmp write, registry switch, GC delete)
* automatic recovery on restart: no lost/duplicated/half-published rows
* two instances sharing one DATA_ROOT (SQLite WAL), interleaved operations
* export pinning across a slow download
"""
from __future__ import annotations

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from tests.conftest import expected_rows, make_csv, parse_export

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, data_root: Path, instance_id: str,
                 env_extra: dict | None = None):
        self.port = _free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        env = os.environ.copy()
        env.update({
            "DATA_ROOT": str(data_root),
            "INSTANCE_ID": instance_id,
            "GC_INTERVAL_SECONDS": "2",
            "GC_GRACE_SECONDS": "1",
            # Short incarnation lease so a SIGKILLed writer's remnants become
            # reclaimable quickly while live peers keep renewing theirs.
            "INSTANCE_LEASE_SECONDS": "3",
            "INSTANCE_HEARTBEAT_SECONDS": "1",
            "PYTHONPATH": str(REPO_ROOT),
        })
        if env_extra:
            env.update(env_extra)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--workers", "1", "--log-level", "warning"],
            cwd=str(REPO_ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def wait_ready(self, timeout: float = 25.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise RuntimeError(f"server exited early:\n{out}")
            try:
                with urllib.request.urlopen(self.base + "/health", timeout=1) as r:
                    if r.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError):
                time.sleep(0.2)
        raise TimeoutError("server did not become ready")

    def stop(self, kill: bool = False) -> str:
        if self.proc.poll() is None:
            if kill:
                self.proc.send_signal(signal.SIGKILL)
            else:
                self.proc.terminate()
        try:
            out, _ = self.proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate()
        return out or ""

    # -- thin HTTP client -------------------------------------------------- #
    def request(self, method: str, path: str, data: bytes | None = None,
                content_type: str = "text/csv", raw: bool = False):
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                return resp.status, body if raw else json.loads(body or b"{}")
        except urllib.error.HTTPError as e:
            body = e.read()
            try:
                return e.code, json.loads(body)
            except json.JSONDecodeError:
                return e.code, body

    def upload(self, voyage: str, csv_text: str):
        boundary = "----tmtestboundary"
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="b.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
            f"{csv_text}\r\n--{boundary}--\r\n"
        ).encode()
        return self.request(
            "POST", f"/voyages/{voyage}/batches", data=body,
            content_type=f"multipart/form-data; boundary={boundary}")

    def get(self, path: str, raw: bool = False):
        return self.request("GET", path, raw=raw)

    def stream_get(self, path: str):
        """Return (response, first_chunk_future) so the caller can interleave."""
        return urllib.request.urlopen(self.base + path, timeout=30)

    def post(self, path: str):
        return self.request("POST", path, data=b"",
                            content_type="application/json")


@pytest.fixture()
def data_root(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    return root


def _wait_until(condition, timeout: float = 15.0, interval: float = 0.3,
                message: str = "condition not met in time"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(message)


def _wait_gc_drained(data_root: Path, timeout: float = 15.0) -> None:
    def drained():
        db = sqlite3.connect(data_root / "metadata.db")
        left = db.execute(
            "SELECT COUNT(*) FROM gc_queue WHERE deleted=0").fetchone()[0]
        db.close()
        return left == 0
    _wait_until(drained, timeout, message="gc_queue did not drain")


def _wait_remnants_reaped(data_root: Path) -> None:
    """Dead-owner tmp/orphan remnants disappear once the incarnation lease
    expires and the reaper (startup or GC loop) claims them."""
    def clean():
        tmp_left = [p for p in (data_root / "tmp").rglob("*") if p.is_file()]
        return not tmp_left
    _wait_until(clean, message="dead-owner tmp remnants were not reaped")


def _assert_voyage_intact(server: Server, voyage: str, specs):
    status, body = server.get(f"/voyages/{voyage}")
    assert status == 200, body
    expected_total = sum(n for n, _, _ in specs)
    assert body["total_row_count"] == expected_total
    assert body["active_row_count"] == expected_total
    status, raw = server.get(f"/voyages/{voyage}/export", raw=True)
    assert status == 200
    rows = parse_export(raw.decode())
    assert rows == expected_rows(specs)
    assert len(rows) == expected_total


def test_full_lifecycle_with_restart(data_root):
    server = Server(data_root, "s1")
    server.wait_ready()
    try:
        specs = [(120, 1, "A"), (80, 2, "B"), (40, 3, "C")]
        for n, seed, prefix in specs:
            code, body = server.upload("v1", make_csv(n, seed, prefix))
            assert code == 200, body
        _assert_voyage_intact(server, "v1", specs)

        code, body = server.post("/voyages/v1/merge")
        assert code == 200, body
        assert body["active_files"] == 1
        _assert_voyage_intact(server, "v1", specs)
    finally:
        server.stop()

    # Restart: recovery runs, data stays intact, GC eventually removes files.
    server2 = Server(data_root, "s1-restart")
    server2.wait_ready()
    try:
        _assert_voyage_intact(server2, "v1", specs)
        # New batch after restart then a second merge.
        code, _ = server2.upload("v1", make_csv(25, 9, "Z"))
        assert code == 200
        code, body = server2.post("/voyages/v1/merge")
        assert code == 200
        _assert_voyage_intact(
            server2, "v1", specs + [(25, 9, "Z")])
        # Give the background GC loop time to reclaim superseded files.
        deadline = time.time() + 15
        while time.time() < deadline:
            db = sqlite3.connect(data_root / "metadata.db")
            left = db.execute(
                "SELECT COUNT(*) FROM gc_queue WHERE deleted=0").fetchone()[0]
            db.close()
            if left == 0:
                break
            time.sleep(0.5)
        db = sqlite3.connect(data_root / "metadata.db")
        assert db.execute(
            "SELECT COUNT(*) FROM gc_queue WHERE deleted=0").fetchone()[0] == 0
        db.close()
    finally:
        server2.stop()


@pytest.mark.parametrize("hook", [
    "merge_stream", "merge_file", "merge_publish", "merge_after_commit",
])
def test_crash_during_merge_then_restart(data_root, hook):
    specs = [(100, 1, "A"), (70, 2, "B"), (30, 3, "C")]
    server = Server(data_root, "crasher", env_extra={"CRASH_HOOK": hook})
    server.wait_ready()
    try:
        for n, seed, prefix in specs:
            assert server.upload("v9", make_csv(n, seed, prefix))[0] == 200
        # The hook kills the process mid-request, so the connection drops
        # instead of returning an HTTP response.
        try:
            server.post("/voyages/v9/merge")
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        # The process must have been killed by the hook.
        deadline = time.time() + 5
        while time.time() < deadline and server.proc.poll() is None:
            time.sleep(0.1)
        assert server.proc.poll() is not None
    finally:
        server.stop(kill=True)

    # No tmp remnants and no orphan merged file should survive restart once
    # the crashed incarnation's lease expires and the reaper runs.
    server2 = Server(data_root, "recovered")
    server2.wait_ready()
    try:
        _assert_voyage_intact(server2, "v9", specs)
        _wait_remnants_reaped(data_root)
        status, body = server2.get("/voyages/v9")
        if hook == "merge_after_commit":
            # The publish transaction had committed before the crash: the
            # merged fragment is the single active published artifact.
            assert body["active_file_count"] == 1
            assert list((data_root / "merged").rglob("*.parquet"))
        else:
            # Earlier crash points leave the original batches fully published;
            # the unpublished merged file is reaped with its write intent.
            assert body["active_file_count"] == 3
            _wait_until(
                lambda: not list((data_root / "merged").rglob("*.parquet")),
                message="orphan merged file was not reaped")
        assert not [p for p in (data_root / "tmp").rglob("*") if p.is_file()]

        # A fresh merge after recovery must succeed and preserve every row.
        code, body = server2.post("/voyages/v9/merge")
        # Nothing to compact when the commit already happened.
        if hook == "merge_after_commit":
            assert code == 422
        else:
            assert code == 200, body
        _assert_voyage_intact(server2, "v9", specs)
    finally:
        server2.stop()


def test_crash_during_upload_commit_is_recovered(data_root):
    specs = [(90, 1, "A"), (70, 2, "B")]
    server = Server(data_root, "up-crash",
                    env_extra={"CRASH_HOOK": "upload_commit",
                               "CRASH_HOOK_AFTER": "2"})
    server.wait_ready()
    try:
        assert server.upload("vu", make_csv(*specs[0]))[0] == 200
        # Second upload crashes after the file rename but before the
        # metadata commit, leaving an orphan parquet file.
        try:
            server.upload("vu", make_csv(*specs[1]))
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        deadline = time.time() + 5
        while time.time() < deadline and server.proc.poll() is None:
            time.sleep(0.1)
        assert server.proc.poll() is not None
        orphan_dir = data_root / "fragments" / "vu"
        on_disk_before = list(orphan_dir.glob("*.parquet"))
        assert len(on_disk_before) == 2  # one published + one orphan
    finally:
        server.stop(kill=True)

    server2 = Server(data_root, "up-recovery")
    server2.wait_ready()
    try:
        # The landed-but-unregistered fragment is kept while the crashed
        # incarnation's lease could theoretically still commit it, then
        # reclaimed automatically once the lease expires (no restart needed).
        _wait_until(
            lambda: len(list(
                (data_root / "fragments" / "vu").glob("*.parquet"))) == 1,
            message="orphan fragment was not reaped after lease expiry")
        status, body = server2.get("/voyages/vu")
        assert status == 200
        assert body["total_row_count"] == 90
        rows = parse_export(server2.get("/voyages/vu/export", raw=True)[1].decode())
        assert rows == expected_rows([specs[0]])
        # Client retries the lost batch; the voyage becomes complete.
        assert server2.upload("vu", make_csv(*specs[1]))[0] == 200
        _assert_voyage_intact(server2, "vu", specs)
    finally:
        server2.stop()


def test_crash_during_gc_keeps_exportable_data(data_root):
    specs = [(60, 1, "A"), (40, 2, "B")]
    server = Server(data_root, "gc-crash",
                    env_extra={"CRASH_HOOK": "gc_delete"})
    server.wait_ready()
    try:
        for n, seed, prefix in specs:
            assert server.upload("vg", make_csv(n, seed, prefix))[0] == 200
        assert server.post("/voyages/vg/merge")[0] == 200
        # GC loop ticks every 2s with 1s grace and crashes on first delete.
        time.sleep(4.0)
        assert server.proc.poll() is not None  # killed mid-GC
    finally:
        server.stop(kill=True)

    server2 = Server(data_root, "gc-recovery")
    server2.wait_ready()
    try:
        # The merged, published data is intact; GC eventually finishes.
        _assert_voyage_intact(server2, "vg", specs)
        deadline = time.time() + 15
        while time.time() < deadline:
            server2.get("/admin/gc")
            db = sqlite3.connect(data_root / "metadata.db")
            left = db.execute(
                "SELECT COUNT(*) FROM gc_queue WHERE deleted=0").fetchone()[0]
            db.close()
            if left == 0:
                break
            time.sleep(0.5)
        else:
            raise AssertionError("GC did not finish reclaiming after restart")
        _assert_voyage_intact(server2, "vg", specs)
    finally:
        server2.stop()


def test_two_instances_interleaved(data_root):
    a = Server(data_root, "api-a")
    b = Server(data_root, "api-b")
    a.wait_ready(); b.wait_ready()
    try:
        # Interleave uploads across the two instances.
        specs = [(50, 1, "A"), (50, 2, "B"), (50, 3, "C"), (50, 4, "D")]
        for i, (n, seed, prefix) in enumerate(specs):
            target = a if i % 2 == 0 else b
            assert target.upload("vdual", make_csv(n, seed, prefix))[0] == 200

        # Both see the same metadata.
        assert a.get("/voyages/vdual/active-file-count")[1] == \
            b.get("/voyages/vdual/active-file-count")[1]
        code_a, body_a = a.post("/voyages/vdual/merge")
        assert code_a == 200, body_a
        # b immediately observes the published merge.
        assert b.get("/voyages/vdual/active-file-count")[1]["active_file_count"] == 1
        _assert_voyage_intact(a, "vdual", specs)
        _assert_voyage_intact(b, "vdual", specs)

        # A second concurrent merge on b has nothing to compact (422).
        code_b, body_b = b.post("/voyages/vdual/merge")
        assert code_b == 422

        # Upload on b during a merge running on a.
        a.upload("vdual", make_csv(20, 7, "L"))
        assert a.post("/voyages/vdual/merge")[0] == 200
        _assert_voyage_intact(a, "vdual", specs + [(20, 7, "L")])
    finally:
        a.stop(); b.stop()


def test_concurrent_merge_across_instances_conflicts(data_root):
    a = Server(data_root, "api-a", env_extra={"MERGE_SLEEP_SECONDS": "3"})
    b = Server(data_root, "api-b")
    a.wait_ready(); b.wait_ready()
    try:
        for seed in range(3):
            assert a.upload("vc", make_csv(30, seed))[0] == 200

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(a.post, "/voyages/vc/merge")
            time.sleep(1.0)  # let a acquire the writing merge row
            fut_b = pool.submit(b.post, "/voyages/vc/merge")
            code_a, _ = fut_a.result()
            code_b, _ = fut_b.result()

        assert sorted([code_a, code_b]) == [200, 409]
        status, body = a.get("/voyages/vc")
        assert body["active_file_count"] == 1
        assert body["active_row_count"] == 90
    finally:
        a.stop(); b.stop()


def test_slow_export_protected_from_merge_upload_and_gc(data_root):
    # 4 fragments with a 1.5s pause between fragments while streaming.
    server = Server(data_root, "exporter",
                    env_extra={"EXPORT_SLEEP_SECONDS": "1.5",
                               "GC_GRACE_SECONDS": "1",
                               "GC_INTERVAL_SECONDS": "1"})
    server.wait_ready()
    try:
        specs = [(40, 1, "A"), (40, 2, "B"), (40, 3, "C"), (40, 4, "D")]
        for n, seed, prefix in specs:
            assert server.upload("ve", make_csv(n, seed, prefix))[0] == 200

        # Start the export and only read the first chunk (download stays open).
        resp = server.stream_get("/voyages/ve/export")
        first = resp.read(256)
        assert "点号" in first.decode()

        # While the download is in flight: merge, upload a new batch,
        # and force several GC cycles.
        code, body = server.post("/voyages/ve/merge")
        assert code == 200, body
        assert server.upload("ve", make_csv(20, 8, "Q"))[0] == 200
        for _ in range(3):
            server.get("/admin/gc")
            time.sleep(1.2)

        # Finish reading the slow export: it must contain the full snapshot
        # taken at export start (160 rows, no late batch).
        rest = resp.read()
        rows = parse_export((first + rest).decode())
        assert rows == expected_rows(specs)
        assert len(rows) == 160

        # The pin now released; GC can reclaim the superseded source files.
        deadline = time.time() + 15
        while time.time() <= deadline:
            db = sqlite3.connect(data_root / "metadata.db")
            left = db.execute(
                "SELECT COUNT(*) FROM gc_queue WHERE deleted=0").fetchone()[0]
            db.close()
            if left == 0:
                break
            server.get("/admin/gc")
            time.sleep(0.5)
        else:
            raise AssertionError("superseded files not reclaimed after pin release")

        # Current data after merge + late batch is still complete.
        _assert_voyage_intact(server, "ve", specs + [(20, 8, "Q")])
    finally:
        server.stop()
