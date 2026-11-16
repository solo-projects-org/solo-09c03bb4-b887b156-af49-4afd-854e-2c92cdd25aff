"""Main-flow tests: upload, query, merge, export, GC, concurrency conflicts."""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from tests.conftest import expected_rows, make_csv, parse_export


@pytest.fixture()
def client(data_root):
    from app.main import app
    with TestClient(app) as c:
        yield c


def test_upload_creates_fragment_and_counts(client):
    r = client.post("/voyages/v1/batches",
                    files={"file": ("b1.csv", make_csv(100, 1), "text/csv")})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_count"] == 100
    assert body["active_files"] == 1

    r2 = client.post("/voyages/v1/batches",
                     files={"file": ("b2.csv", make_csv(50, 2), "text/csv")})
    assert r2.status_code == 200
    assert r2.json()["active_files"] == 2

    r3 = client.get("/voyages/v1/active-file-count")
    assert r3.json()["active_file_count"] == 2

    status = client.get("/voyages/v1").json()
    assert status["total_row_count"] == 150
    assert status["active_row_count"] == 150
    assert len(status["fragments"]) == 2


def test_export_matches_input_row_for_row(client):
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(120, 1, "A"), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b2.csv", make_csv(80, 2, "B"), "text/csv")})
    exported = parse_export(client.get("/voyages/v1/export").text)
    assert exported == expected_rows([(120, 1, "A"), (80, 2, "B")])
    assert len(exported) == 200


def test_merge_replaces_fragments_with_same_rows(client):
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(120, 1, "A"), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b2.csv", make_csv(80, 2, "B"), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b3.csv", make_csv(40, 3, "C"), "text/csv")})
    want = expected_rows([(120, 1, "A"), (80, 2, "B"), (40, 3, "C")])

    merged = client.post("/voyages/v1/merge")
    assert merged.status_code == 200, merged.text
    body = merged.json()
    assert body["active_files"] == 1
    assert body["row_count"] == 240

    status = client.get("/voyages/v1").json()
    assert status["total_row_count"] == 240
    kinds = sorted((f["kind"], f["status"]) for f in status["fragments"])
    assert kinds == [("batch", "superseded"), ("batch", "superseded"),
                     ("batch", "superseded"), ("merged", "active")]

    got = parse_export(client.get("/voyages/v1/export").text)
    assert got == want  # identical order and values


def test_new_batch_after_merge_remains_separate(client):
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(60, 1), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b2.csv", make_csv(40, 2), "text/csv")})
    client.post("/voyages/v1/merge").raise_for_status()
    r = client.post("/voyages/v1/batches",
                    files={"file": ("b3.csv", make_csv(30, 3, "D"), "text/csv")})
    assert r.json()["active_files"] == 2  # merged fragment + fresh batch
    status = client.get("/voyages/v1").json()
    assert status["active_row_count"] == 130
    # Re-merge compacts the merged fragment + new batch.
    r2 = client.post("/voyages/v1/merge")
    assert r2.status_code == 200
    assert r2.json()["active_files"] == 1
    got = parse_export(client.get("/voyages/v1/export").text)
    assert got == expected_rows([(60, 1, "P"), (40, 2, "P"), (30, 3, "D")])


def test_merge_single_or_unknown_voyage_rejected(client):
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(10, 1), "text/csv")})
    r = client.post("/voyages/v1/merge")
    assert r.status_code == 422
    assert client.post("/voyages/ghost/merge").status_code == 404


def test_concurrent_merges_conflict(client, monkeypatch):
    from app import services
    for seed in range(4):
        client.post("/voyages/v1/batches",
                    files={"file": (f"b{seed}.csv", make_csv(20, seed), "text/csv")})

    results: list = []
    entered = threading.Event()
    release = threading.Event()
    real = services.merge_parquet_files

    def slow_merge(*args, **kwargs):
        entered.set()
        release.wait(timeout=10)
        return real(*args, **kwargs)

    monkeypatch.setattr(services, "merge_parquet_files", slow_merge)

    def worker():
        try:
            services.merge_voyage("v1")
            results.append(200)
        except services.ServiceError as exc:
            results.append(exc.status_code)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert entered.wait(timeout=10)  # first merge holds the writing row
    t2.start()
    # Give t2 time to hit the conflict and record its result.
    import time
    for _ in range(50):
        if 409 in results:
            break
        time.sleep(0.05)
    release.set()
    t1.join(); t2.join()

    assert sorted(results) == [200, 409]
    # Exactly one active merged fragment, rows unchanged.
    status = client.get("/voyages/v1").json()
    assert status["active_row_count"] == 80
    assert status["active_file_count"] == 1


def test_gc_waits_for_export_pin(client, data_root):
    from app import services
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(50, 1), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b2.csv", make_csv(30, 2), "text/csv")})

    pin_id, fragments, total = services.pin_active_fragments("v1")
    assert total == 80 and len(fragments) == 2
    client.post("/voyages/v1/merge").raise_for_status()

    # Grace time elapses, but the pin still protects the old files.
    gc = services.run_gc_once()
    assert gc["files_deleted"] == 0
    superseded = [f for f in client.get("/voyages/v1").json()["fragments"]
                  if f["status"] == "superseded"]
    for f in superseded:
        assert (data_root / "fragments" / "v1").exists()
    services.release_pin(pin_id)
    # Backdate the GC queue so grace time has passed.
    import sqlite3
    conn = sqlite3.connect(data_root / "metadata.db")
    conn.execute("UPDATE gc_queue SET delete_after='2000-01-01T00:00:00+00:00'")
    conn.commit(); conn.close()
    gc = services.run_gc_once()
    assert gc["files_deleted"] == 2
    # Active merged data still exports fully.
    got = parse_export(client.get("/voyages/v1/export").text)
    assert len(got) == 80


def test_upload_during_merge_keeps_new_batch(client, monkeypatch):
    client.post("/voyages/v1/batches",
                files={"file": ("b1.csv", make_csv(40, 1), "text/csv")})
    client.post("/voyages/v1/batches",
                files={"file": ("b2.csv", make_csv(35, 2), "text/csv")})

    from app import services
    real = services.merge_parquet_files

    def merge_with_upload(*args, **kwargs):
        # A new batch lands while the merge is streaming.
        client.post("/voyages/v1/batches",
                    files={"file": ("late.csv", make_csv(25, 9, "Z"), "text/csv")})
        return real(*args, **kwargs)

    monkeypatch.setattr(services, "merge_parquet_files", merge_with_upload)
    r = client.post("/voyages/v1/merge")
    assert r.status_code == 200
    assert r.json()["row_count"] == 75  # only the start-of-merge fragments
    status = client.get("/voyages/v1").json()
    assert status["active_file_count"] == 2  # merged + late batch
    assert status["active_row_count"] == 100
    got = parse_export(client.get("/voyages/v1/export").text)
    assert got == expected_rows([(40, 1, "P"), (35, 2, "P"), (25, 9, "Z")])


def test_concurrent_uploads_get_distinct_seq(client):
    from app import services
    errors: list[Exception] = []

    def worker(seed: int):
        try:
            services.upload_batch("vpar", make_csv(10, seed))
        except Exception as exc:  # noqa: BLE001 - recorded for assertion
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(s,)) for s in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    status = client.get("/voyages/vpar").json()
    seqs = [f["seq"] for f in status["fragments"]]
    assert sorted(seqs) == list(range(1, 9))
    assert status["total_row_count"] == 80


def test_upload_validation_errors(client):
    # over the 10k row cap
    big = make_csv(10_001, 1)
    r = client.post("/voyages/v1/batches",
                    files={"file": ("big.csv", big, "text/csv")})
    assert r.status_code == 422
    # bad column
    r2 = client.post("/voyages/v1/batches",
                     files={"file": ("bad.csv", "a,b\n1,2\n", "text/csv")})
    assert r2.status_code == 422
    # bad voyage id (path traversal attempt)
    r3 = client.post("../etc/x/batches",
                     files={"file": ("b.csv", make_csv(1), "text/csv")})
    assert r3.status_code in (404, 422)


def test_accepts_english_header_aliases(client):
    csv_text = ("point_id,utc_time,lon,lat,alt\n"
                "X1,2026-11-16T00:00:00Z,116.3,39.9,120.5\n"
                "X2,2026-11-16T00:00:01Z,116.4,39.8,121.0\n")
    r = client.post("/voyages/veng/batches",
                    files={"file": ("e.csv", csv_text, "text/csv")})
    assert r.status_code == 200, r.text
    got = parse_export(client.get("/voyages/veng/export").text)
    assert got[0]["point_id"] == "X1"
    assert got[1]["alt"] == 121.0
