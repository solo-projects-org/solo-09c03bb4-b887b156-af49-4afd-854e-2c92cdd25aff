"""Shared test helpers: CSV generation, temp data roots, row comparison."""
from __future__ import annotations

import csv
import io
import os
import random
import uuid
from pathlib import Path

import pytest


@pytest.fixture()
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    # The settings singleton is read at call time everywhere, so mutate it in
    # place (it is a frozen dataclass) instead of reloading modules.
    from app.config import settings
    from app.db import init_db
    object.__setattr__(settings, "data_root", root.resolve())
    object.__setattr__(settings, "gc_interval_seconds", 3600.0)
    object.__setattr__(settings, "gc_grace_seconds", 0)
    init_db()  # creates DATA_ROOT, fragment dirs and the schema
    yield root


def make_csv(rows: int, seed: int = 0, prefix: str = "P") -> str:
    rng = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["点号", "UTC时刻", "经度", "纬度", "高度"])
    for i in range(rows):
        writer.writerow([
            f"{prefix}{i:06d}",
            f"2026-11-16T00:{i % 60:02d}:{(i * 7) % 60:02d}Z",
            f"{116.0 + rng.random():.7f}",
            f"{39.0 + rng.random():.7f}",
            f"{100.0 + rng.random() * 500:.3f}",
        ])
    return buf.getvalue()


def parse_export(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    rows = []
    for r in reader:
        rows.append({
            "point_id": r["点号"],
            "utc_time": r["UTC时刻"],
            "lon": float(r["经度"]),
            "lat": float(r["纬度"]),
            "alt": float(r["高度"]),
        })
    return rows


def expected_rows(batch_specs: list[tuple[int, int, str]]) -> list[dict]:
    """Reconstruct the expected ordered rows from (n, seed, prefix) specs."""
    out: list[dict] = []
    for n, seed, prefix in batch_specs:
        out.extend(parse_export(make_csv(n, seed, prefix)))
    return out
