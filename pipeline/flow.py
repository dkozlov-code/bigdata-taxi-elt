import json
import math
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from prefect import flow, task
from prefect.tasks import exponential_backoff

from pipeline.config import Settings
from pipeline.extract import read_source, split_valid
from pipeline.observability import event, inject, measured
from pipeline.quality import validate
from pipeline.report import write_report
from pipeline.storage import fingerprint, merge, single_writer
from pipeline.transform import dbt


def retry_operational(task, task_run, state):
    result = state.result(raise_on_failure=False)
    return isinstance(result, (OSError, subprocess.TimeoutExpired))


def step(function):
    return task(
        retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=1),
        retry_condition_fn=retry_operational, cache_policy=None, log_prints=True,
    )(function)


@step
def extract(path: Path, run: Path, fault: str | None):
    with measured(run, "extract"):
        inject(run, "extract", fault)
        frame = read_source(path, run)
        target = run / f"{path.stem}-raw.parquet"
        frame.to_parquet(target, index=False)
        return target


@step
def check_input(path: Path, run: Path, fault: str | None):
    with measured(run, "validate"):
        inject(run, "validate", fault)
        frame = split_valid(pd.read_parquet(path), run)
        validate(frame, run, f"input_{uuid4().hex[:8]}", "trips")
        target = path.with_name(path.stem + "-valid.parquet")
        frame.to_parquet(target, index=False)
        return target


@step
def load(path: Path, database: Path, run: Path, fault: str | None):
    with measured(run, "load"):
        inject(run, "load", fault)
        merge(pd.read_parquet(path), database)


@flow(name="region-batch", log_prints=True)
def region_batch(path: Path, database: Path, run: Path, fault: str | None):
    load(path, database, run, fault)


@flow(name="source-batch", log_prints=True)
def batch(path: Path, database: Path, run: Path, fault: str | None):
    # Повторно используемый subflow; список источников определяется во время запуска.
    raw = extract(path, run, fault)
    clean = check_input(raw, run, fault)
    frame = pd.read_parquet(clean)
    regions = run / "regions" / path.stem
    regions.mkdir(parents=True, exist_ok=True)
    lat = (frame.pickup_latitude / .01).map(math.floor)
    lon = (frame.pickup_longitude / .012).map(math.floor)
    for (cell_lat, cell_lon), group in frame.groupby([lat, lon]):
        target = regions / f"{cell_lat}_{cell_lon}.parquet"
        group.to_parquet(target, index=False)
        region_batch.with_options(flow_run_name=f"region-{cell_lat}-{cell_lon}")(
            target, database, run, fault,
        )


@step
def transform(database: Path, run: Path, fault: str | None):
    with measured(run, "transform"):
        inject(run, "transform", fault)
        dbt(database, run)


@step
def check_output(database: Path, run: Path, fault: str | None):
    with measured(run, "quality"):
        inject(run, "quality", fault)
        with duckdb.connect(str(database), read_only=True) as con:
            mart = con.sql("""
                SELECT *, (SELECT count(*) FROM analytics.stg_trips t
                           ANTI JOIN analytics.dim_vendors v USING (vendor_id)) AS orphan_count
                FROM analytics.daily_revenue
            """).df()
        return validate(mart, run, f"mart_{uuid4().hex[:8]}", "mart")


@step
def publish(database: Path, settings: Settings, run: Path, fault: str | None):
    with measured(run, "publish"):
        inject(run, "publish", fault)
        versions = settings.work / "versions"
        versions.mkdir(exist_ok=True)
        version = versions / f"{run.name}.duckdb"
        # Все подключения закрыты: snapshot и current содержат raw + проверенные marts.
        # Current меняется атомарно на том же filesystem, только после всех gates.
        shutil.copy2(database, version.with_suffix(".tmp"))
        version.with_suffix(".tmp").replace(version)
        database.replace(settings.warehouse)
        return version.name


def dlq_count(work: Path):
    unique = {path.name: path for path in work.glob("runs/*/dlq/*.json")}
    return sum(len(json.loads(path.read_text())["rows"]) for path in unique.values())


@flow(name="taxi-daily-revenue", log_prints=True)
def run_pipeline(settings: Settings | None = None, fault: str | None = None):
    settings = settings or Settings.from_env()
    start = time.perf_counter()
    run = settings.work / "runs" / uuid4().hex
    run.mkdir(parents=True)
    try:
        with single_writer(settings.work):
            database = run / "candidate.duckdb"
            if settings.warehouse.exists():
                shutil.copy2(settings.warehouse, database)
            paths = sorted(p for p in settings.source.iterdir() if p.suffix in {".csv", ".parquet"})
            if not paths:
                raise FileNotFoundError(f"Нет CSV/Parquet в {settings.source}")
            for path in paths:
                batch(path, database, run, fault)
            transform(database, run, fault)
            score = check_output(database, run, fault)
            version = publish(database, settings, run, fault)
            rows, unique, _ = fingerprint(settings.warehouse)
        duration = time.perf_counter() - start
        errors = dlq_count(settings.work)
        now = datetime.now(ZoneInfo("America/New_York"))
        if duration > settings.max_seconds or now.hour >= 6:
            event(settings.work, "alert", reason="sla", seconds=duration, local_time=now)
        if errors >= settings.dlq_threshold:
            event(settings.work, "alert", reason="dlq_threshold", count=errors)
        metrics = {"rows": rows, "unique_keys": unique, "version": version,
                   "data_quality_score": score, "dlq_error_count": errors,
                   "pipeline_duration_sec": duration, "run": str(run)}
        (run / "metrics.json").write_text(json.dumps(metrics, indent=2))
        write_report(run, metrics)
        event(settings.work, "pipeline_success", **metrics)
        return metrics
    except Exception as exc:
        event(settings.work, "alert", reason="pipeline_failed_previous_version_retained", error=str(exc))
        raise


def demonstrate(settings: Settings):
    first = run_pipeline(settings)
    initial = fingerprint(settings.warehouse)
    second = run_pipeline(settings)
    passed = initial == fingerprint(settings.warehouse) and second["rows"] == second["unique_keys"]
    assert passed, "Пайплайн не идемпотентен"
    assert second["dlq_error_count"] > 0, "DLQ не заполняется"
    print("## METRICS ##")
    print(f"IDEMPOTENCY_TEST_PASSED={passed}")
    print(f"DLQ_ERROR_COUNT={second['dlq_error_count']}")
    print(f"DATA_QUALITY_SCORE={second['data_quality_score']:.2f}")
    print(f"PIPELINE_DURATION_SEC={second['pipeline_duration_sec']:.2f}")
    return first, second
