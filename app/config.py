"""Runtime configuration, driven entirely by environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    data_root: Path
    merge_lease_seconds: int = 120
    heartbeat_interval_seconds: float = 5.0
    pin_ttl_seconds: int = 30 * 60
    gc_interval_seconds: float = 10.0
    # A superseded file is only physically removed after this grace period
    # (and once no export pin references it).
    gc_grace_seconds: int = 300
    sqlite_timeout_seconds: float = 30.0
    max_rows_per_batch: int = 10_000
    parquet_batch_size: int = 5_000
    # Test-only slowdown for the merge streaming phase; 0 disables it.
    merge_sleep_seconds: float = 0.0
    # Test-only slowdown between export fragments; 0 disables it.
    export_sleep_seconds: float = 0.0
    instance_id: str = ""

    @property
    def db_path(self) -> Path:
        return self.data_root / "metadata.db"

    @property
    def fragments_dir(self) -> Path:
        return self.data_root / "fragments"

    @property
    def merged_dir(self) -> Path:
        return self.data_root / "merged"

    @property
    def tmp_dir(self) -> Path:
        return self.data_root / "tmp"


def load_settings() -> Settings:
    data_root = Path(os.environ.get("DATA_ROOT", "./data")).resolve()
    return Settings(
        data_root=data_root,
        merge_lease_seconds=_int_env("MERGE_LEASE_SECONDS", 120),
        heartbeat_interval_seconds=float(os.environ.get("HEARTBEAT_INTERVAL", "5")),
        pin_ttl_seconds=_int_env("PIN_TTL_SECONDS", 30 * 60),
        gc_interval_seconds=float(os.environ.get("GC_INTERVAL_SECONDS", "10")),
        gc_grace_seconds=_int_env("GC_GRACE_SECONDS", 300),
        sqlite_timeout_seconds=float(os.environ.get("SQLITE_TIMEOUT", "30")),
        max_rows_per_batch=_int_env("MAX_ROWS_PER_BATCH", 10_000),
        parquet_batch_size=_int_env("PARQUET_BATCH_SIZE", 5_000),
        merge_sleep_seconds=float(os.environ.get("MERGE_SLEEP_SECONDS", "0")),
        export_sleep_seconds=float(os.environ.get("EXPORT_SLEEP_SECONDS", "0")),
        instance_id=os.environ.get("INSTANCE_ID", os.uname().nodename),
    )


settings = load_settings()
