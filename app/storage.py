"""Streaming Parquet / CSV helpers built on PyArrow.

No function here ever loads a whole voyage into memory: files are produced and
consumed one record batch at a time.
"""
from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass
from typing import Iterable, Iterator, List

import pyarrow as pa
import pyarrow.parquet as pq

# Canonical CSV headers (Chinese, per the field contract).
COLUMNS = ["点号", "UTC时刻", "经度", "纬度", "高度"]
# Aliases accepted on upload.
HEADER_ALIASES = {
    "点号": "点号", "point_id": "点号", "id": "点号",
    "utc时刻": "UTC时刻", "utc": "UTC时刻", "utc_time": "UTC时刻",
    "time": "UTC时刻", "timestamp": "UTC时刻",
    "经度": "经度", "lon": "经度", "lng": "经度", "longitude": "经度",
    "纬度": "纬度", "lat": "纬度", "latitude": "纬度",
    "高度": "高度", "alt": "高度", "altitude": "高度", "height": "高度",
}

SCHEMA = pa.schema([
    pa.field("point_id", pa.string()),
    pa.field("utc_time", pa.string()),
    pa.field("lon", pa.float64()),
    pa.field("lat", pa.float64()),
    pa.field("alt", pa.float64()),
])


@dataclass
class ParsedCSV:
    rows: List[dict]
    header: List[str]


class CSVValidationError(ValueError):
    pass


def _normalize_header(raw_header: List[str]) -> List[str]:
    normalized: List[str] = []
    for name in raw_header:
        key = (name or "").strip().lstrip("﻿").lower()
        canonical = HEADER_ALIASES.get(key)
        if canonical is None:
            raise CSVValidationError(f"未知列: {name!r}，期望列: {COLUMNS}")
        normalized.append(canonical)
    for required in COLUMNS:
        if required not in normalized:
            raise CSVValidationError(f"缺少必需列: {required}，实际列: {raw_header}")
    if len(set(normalized)) != len(normalized):
        raise CSVValidationError(f"存在重复列: {raw_header}")
    return normalized


def parse_csv(text: str, max_rows: int) -> ParsedCSV:
    """Parse and validate an upload CSV, preserving every input row."""
    buffer = io.StringIO(text)
    reader = csv.reader(buffer)
    try:
        raw_header = next(reader)
    except StopIteration:
        raise CSVValidationError("CSV 为空，缺少表头")
    header = _normalize_header(raw_header)

    rows: List[dict] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(cell.strip() == "" for cell in raw):
            continue  # tolerate trailing blank lines
        if len(raw) != len(header):
            raise CSVValidationError(
                f"第 {line_no} 行列数 {len(raw)} 与表头 {len(header)} 不一致")
        record = dict(zip(header, raw))
        point_id = record["点号"].strip()
        utc_time = record["UTC时刻"].strip()
        if not point_id:
            raise CSVValidationError(f"第 {line_no} 行点号为空")
        if not utc_time:
            raise CSVValidationError(f"第 {line_no} 行 UTC 时刻为空")
        try:
            lon = float(record["经度"])
            lat = float(record["纬度"])
            alt = float(record["高度"])
        except ValueError as exc:
            raise CSVValidationError(f"第 {line_no} 行数值字段无法解析: {exc}") from exc
        if not (-180.0 <= lon <= 180.0):
            raise CSVValidationError(f"第 {line_no} 行经度越界: {lon}")
        if not (-90.0 <= lat <= 90.0):
            raise CSVValidationError(f"第 {line_no} 行纬度越界: {lat}")
        rows.append({
            "point_id": point_id,
            "utc_time": utc_time,
            "lon": lon,
            "lat": lat,
            "alt": alt,
        })
        if len(rows) > max_rows:
            raise CSVValidationError(f"数据行数超过上限 {max_rows}")
    if not rows:
        raise CSVValidationError("CSV 没有任何数据行")
    return ParsedCSV(rows=rows, header=header)


def rows_to_table(rows: Iterable[dict]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=SCHEMA)


def write_parquet_atomic(rows: List[dict], tmp_path: str, final_path: str,
                         batch_size: int) -> int:
    """Write rows to a Parquet file via a temp file, fsync and atomic rename.

    Returns the number of rows written.
    """
    total = 0
    writer = None
    try:
        writer = pq.ParquetWriter(tmp_path, SCHEMA, compression="snappy")
        for start in range(0, len(rows), batch_size):
            table = rows_to_table(rows[start:start + batch_size])
            writer.write_table(table)
            total += table.num_rows
        writer.close()
        writer = None
        # Durable temp file before the rename.
        with open(tmp_path, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)
        _fsync_parent(os.path.dirname(final_path))
    finally:
        if writer is not None:
            writer.close()
    return total


def _fsync_parent(directory: str) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def iter_parquet_batches(path: str, batch_size: int) -> Iterator[pa.RecordBatch]:
    """Yield record batches from a Parquet file without loading it all."""
    pf = pq.ParquetFile(path)
    yield from pf.iter_batches(batch_size=batch_size)


def merge_parquet_files(src_paths: List[str], tmp_path: str, final_path: str,
                        batch_size: int) -> int:
    """Concatenate Parquet fragment files, streaming batch by batch.

    Row order and values are preserved: fragments are concatenated in the
    given order and batches inside a fragment keep their on-disk order.
    """
    from .crashhooks import crash_if

    total = 0
    writer = pq.ParquetWriter(tmp_path, SCHEMA, compression="snappy")
    try:
        first = True
        for path in src_paths:
            for batch in iter_parquet_batches(path, batch_size):
                writer.write_table(pa.Table.from_batches([batch]))
                total += batch.num_rows
                if first:
                    # Crash once, after the first streamed batch.
                    crash_if("merge_stream")
                    first = False
        writer.close()
        with open(tmp_path, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)
        _fsync_parent(os.path.dirname(final_path))
    except BaseException:
        # Leave the temp file behind for startup cleanup; never rename it.
        try:
            writer.close()
        except Exception:
            pass
        raise
    return total


def parquet_row_count(path: str) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def format_csv_header() -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(COLUMNS)
    return buffer.getvalue()


def batch_to_csv_lines(batch: pa.RecordBatch) -> Iterator[str]:
    """Serialize a record batch to CSV text chunks (canonical headers)."""
    names = batch.schema.names
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for i in range(batch.num_rows):
        writer.writerow([batch.column(names.index(col))[i].as_py()
                         for col in ("point_id", "utc_time", "lon", "lat", "alt")])
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)
