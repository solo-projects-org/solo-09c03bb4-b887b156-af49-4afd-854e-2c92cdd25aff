"""FastAPI application: drone trajectory ingest, compaction and export."""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from . import recovery, services
from .config import settings
from .db import init_db
from .services import ServiceError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("trackmerge")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    recovered = services.startup_recovery(settings)
    log.info("startup recovery result: %s", recovered)
    gc_thread = threading.Thread(
        target=services.gc_loop, name="gc-loop", daemon=True,
        kwargs={"cfg": settings})
    gc_thread.start()
    try:
        yield
    finally:
        # Graceful stop: release the incarnation lease immediately so peers
        # can reclaim our remnants without waiting for the lease timeout.
        recovery.shutdown_instance(settings)


app = FastAPI(
    title="无人机航迹小文件合并服务",
    description=(
        "航迹批次以 Parquet 片段存储，SQLite 仅保存元数据。支持按航次手动合并、"
        "活动文件数查询与整航次 CSV 流式导出。合并发布为单事务原子切换，"
        "导出通过 pin 保护在途文件，崩溃重启后自动清理残件并继续回收。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["运维"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "instance_id": settings.instance_id}


@app.post("/voyages/{voyage_id}/batches", tags=["航迹"],
          summary="上传单航次的一个 CSV 批次（最多 1 万行，每批存为独立片段）")
def upload_batch(voyage_id: str, file: UploadFile = File(...)) -> dict:
    # Plain def -> Starlette runs it in its threadpool, so the blocking
    # parse/Parquet/SQLite work never stalls the event loop (uploads still
    # complete while a slow export is streaming).
    raw = file.file.read()
    # Uploads are capped at 10k rows; decode here with replacement disabled so
    # malformed uploads get a 422 rather than silent mojibake.
    try:
        csv_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"文件不是合法 UTF-8 文本: {exc}")
    try:
        return services.upload_batch(voyage_id, csv_text, settings)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message)


@app.get("/voyages", tags=["航迹"], summary="列出全部航次")
def list_voyages() -> dict:
    return {"voyages": services.list_voyages(settings)}


@app.get("/voyages/{voyage_id}", tags=["航迹"], summary="查询航次状态与片段清单")
def voyage_status(voyage_id: str) -> dict:
    try:
        return services.voyage_status(voyage_id, settings)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message)


@app.get("/voyages/{voyage_id}/active-file-count", tags=["航迹"],
         summary="查询活动文件（已发布片段）数")
def active_file_count(voyage_id: str) -> dict:
    try:
        return {"voyage_id": voyage_id,
                "active_file_count": services.active_file_count(voyage_id, settings)}
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message)


@app.post("/voyages/{voyage_id}/merge", tags=["合并"],
          summary="按航次手动合并：开始时的多个片段原子替换为一个片段")
def merge_voyage(voyage_id: str) -> dict:
    try:
        return services.merge_voyage(voyage_id, settings)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message)


@app.get("/voyages/{voyage_id}/export", tags=["导出"],
         summary="导出航次完整 CSV（固定导出开始时的活动片段，流式输出）")
def export_voyage(voyage_id: str) -> StreamingResponse:
    # Validate existence before opening the stream so unknown voyages get a
    # normal JSON 404 instead of an error inside the response body.
    try:
        services.voyage_status(voyage_id, settings)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message)
    return StreamingResponse(
        services.stream_voyage_csv(voyage_id, settings),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                f'attachment; filename="{voyage_id}-track.csv"',
            "X-Export-Pin": "held-for-duration-of-download",
        },
    )


@app.get("/admin/gc", tags=["运维"], summary="立即执行一次旧片段回收")
def gc_now() -> dict:
    return services.run_gc_once(settings)
