import fcntl
import shutil
from contextlib import contextmanager
from pathlib import Path

import duckdb


@contextmanager
def single_writer(work: Path):
    """Локальный межпроцессный lock: только один writer до завершения публикации."""
    work.mkdir(parents=True, exist_ok=True)
    with (work / "writer.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def merge(frame, database: Path):
    with duckdb.connect(str(database)) as con:
        con.register("incoming", frame)
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("CREATE TABLE IF NOT EXISTS raw.trips AS SELECT * FROM incoming LIMIT 0")
        con.execute("""
            MERGE INTO raw.trips AS target USING incoming AS source
            ON target.idempotency_key = source.idempotency_key
            WHEN MATCHED THEN UPDATE BY NAME
            WHEN NOT MATCHED THEN INSERT BY NAME
        """)


def fingerprint(database: Path):
    with duckdb.connect(str(database), read_only=True) as con:
        return con.execute("""
            SELECT count(*), count(DISTINCT idempotency_key),
                   bit_xor(hash(idempotency_key, fare_amount, trip_distance))
            FROM raw.trips
        """).fetchone()


def version_path(work: Path, version: str):
    if Path(version).name != version or not version.endswith(".duckdb"):
        raise ValueError("Ожидается имя снимка *.duckdb без пути")
    return work / "versions" / version


def read_version(work: Path, version: str):
    """Time travel по неизменяемому снимку всей согласованной БД, без смены current."""
    with duckdb.connect(str(version_path(work, version)), read_only=True) as con:
        return con.sql("SELECT * FROM analytics.daily_revenue ORDER BY ALL").df()


def restore(work: Path, version: str):
    with single_writer(work):
        candidate = work / "restore.duckdb"
        shutil.copy2(version_path(work, version), candidate)
        candidate.replace(work / "warehouse.duckdb")
