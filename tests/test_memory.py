"""Verify that no operation loads a whole voyage into memory.

The data set is deliberately larger than the configured batch size, and
tracemalloc is used to bound the peak Python allocation attributable to each
operation. Absolute allocations vary by interpreter, so the assertion is
relative: streaming a voyage of N rows must not scale linearly with N beyond
a small constant (i.e. peak memory stays close to one-batch size).
"""
from __future__ import annotations

import io
import tracemalloc

from app import services, storage
from app.config import settings
from tests.conftest import make_csv, parse_export


def _force_small_batch_size(monkeypatch):
    object.__setattr__(settings, "parquet_batch_size", 256)


def test_large_voyage_merge_and_export_are_bounded_memory(data_root, monkeypatch):
    _force_small_batch_size(monkeypatch)
    # 6 batches x 4,000 rows = 24,000 rows, far above the 256-row batch size.
    specs = [(4_000, seed, f"S{seed}") for seed in range(6)]
    total_rows = sum(n for n, _, _ in specs)
    for n, seed, prefix in specs:
        services.upload_batch("big", make_csv(n, seed, prefix))

    # Merge: peak allocations must stay well below holding all rows.
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    result = services.merge_voyage("big")
    _, peak_merge = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert result["row_count"] == total_rows
    # One row of Python objects is roughly 300-500 bytes; loading 24k rows
    # would need ~8-12 MB. A streaming merge with 256-row batches stays under
    # 3 MB of traced allocations on this interpreter.
    assert peak_merge - before < 3_000_000, peak_merge - before

    # Export: collect the stream but measure only the generator's footprint.
    gen = services.stream_voyage_csv("big")
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    chunks = []
    for chunk in gen:
        chunks.append(chunk)
    _, peak_export = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak_export - before < 3_000_000, peak_export - before

    exported = parse_export("".join(chunks))
    assert len(exported) == total_rows
    from tests.conftest import expected_rows
    assert exported == expected_rows(specs)


def test_parquet_writers_never_hold_more_than_one_batch(data_root, monkeypatch):
    _force_small_batch_size(monkeypatch)
    parsed = storage.parse_csv(make_csv(3_000, 5), settings.max_rows_per_batch)
    assert len(parsed.rows) == 3_000

    tmp = str(settings.tmp_dir / "unit.parquet.tmp")
    final = str(settings.fragments_dir / "unit.parquet")
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    written = storage.write_parquet_atomic(
        parsed.rows, tmp, final, settings.parquet_batch_size)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert written == 3_000
    # The input list itself (~3k dicts) exists before measurement starts; the
    # writer's own incremental allocations must stay small.
    assert peak - before < 2_000_000, peak - before
    assert storage.parquet_row_count(final) == 3_000


def test_export_stream_is_a_generator(data_root):
    services.upload_batch("g", make_csv(500, 1))
    import types
    gen = services.stream_voyage_csv("g")
    assert isinstance(gen, types.GeneratorType)
    # Consume to release the pin.
    buf = io.StringIO()
    for chunk in gen:
        buf.write(chunk)
    assert len(parse_export(buf.getvalue())) == 500
