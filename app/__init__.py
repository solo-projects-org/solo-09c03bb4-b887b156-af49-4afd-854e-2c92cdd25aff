"""Drone track compaction service.

FastAPI + PyArrow + SQLite(WAL). Parquet files hold trajectory data; SQLite
holds only metadata. All publishing steps are made atomic through a single
SQLite transaction, so a crash can never leave a half-published voyage.
"""

__version__ = "1.0.0"
