import json

import duckdb
import pandas as pd
import pytest

from pipeline.extract import SchemaDriftError, read_source, split_valid
from pipeline.flow import dlq_count, run_pipeline
from pipeline.quality import validate
from pipeline.storage import fingerprint, merge, read_version, restore


def test_merge_idempotency_and_correction(settings):
    settings.work.mkdir()
    data = split_valid(read_source(settings.source / "trips.csv", settings.work), settings.work)
    merge(data, settings.warehouse)
    initial = fingerprint(settings.warehouse)
    merge(data, settings.warehouse)
    assert fingerprint(settings.warehouse) == initial
    corrected = data.iloc[:1].copy()
    corrected["fare_amount"] = 99
    merge(corrected, settings.warehouse)
    assert fingerprint(settings.warehouse)[:2] == (6, 6)
    with duckdb.connect(str(settings.warehouse)) as con:
        assert con.execute("SELECT count(*) FROM raw.trips WHERE fare_amount=99").fetchone()[0] == 1


def test_schema_projection_and_breaking_change(settings):
    settings.work.mkdir()
    source = settings.source / "trips.csv"
    frame = pd.read_csv(source)
    frame.assign(new_optional="ignored").to_csv(source, index=False)
    assert "new_optional" not in read_source(source, settings.work)
    frame.drop(columns="fare_amount").to_csv(source, index=False)
    with pytest.raises(SchemaDriftError):
        read_source(source, settings.work)
    assert list((settings.work / "dlq").glob("*.json"))


def test_gx_failed_report(settings):
    settings.work.mkdir()
    frame = read_source(settings.source / "trips.csv", settings.work)
    with pytest.raises(ValueError, match="Great Expectations"):
        validate(frame, settings.work, "bad_input", "trips")
    assert list((settings.work / "quality").rglob("*.html"))
    assert any("gx_failed" in p.read_text() for p in (settings.work / "dlq").glob("*.json"))


def test_full_pipeline_idempotency_dlq_time_travel_and_schema_fallback(settings):
    first = run_pipeline(settings)
    initial = fingerprint(settings.warehouse)
    second = run_pipeline(settings)
    assert fingerprint(settings.warehouse) == initial
    assert first["rows"] == second["rows"] == second["unique_keys"] == 6
    assert dlq_count(settings.work) == 2
    assert second["data_quality_score"] == 1
    before = read_version(settings.work, first["version"])
    assert before.revenue_usd.sum() == 111.5
    source = settings.source / "trips.csv"
    data = pd.read_csv(source)
    data.loc[[0, 6], "fare_amount"] = 100
    data.to_csv(source, index=False)
    third = run_pipeline(settings)
    assert third["rows"] == 6
    assert read_version(settings.work, first["version"]).equals(before)
    assert read_version(settings.work, third["version"]).revenue_usd.sum() == 199
    restore(settings.work, first["version"])
    assert fingerprint(settings.warehouse) == initial
    data.drop(columns="fare_amount").to_csv(source, index=False)
    with pytest.raises(SchemaDriftError):
        run_pipeline(settings)
    assert fingerprint(settings.warehouse) == initial
    assert "schema_drift" in next((settings.work / "runs").glob("*/events.jsonl")).read_text() or any(
        "schema_drift" in p.read_text() for p in (settings.work / "runs").glob("*/events.jsonl")
    )


@pytest.mark.parametrize("stage", ["extract", "validate", "load", "transform", "quality", "publish"])
def test_transient_recovery(settings, stage):
    result = run_pipeline(settings, fault=stage)
    assert result["rows"] == result["unique_keys"] == 6
    events = [json.loads(line) for line in (settings.work / "runs" / result["version"].removesuffix(".duckdb") / "events.jsonl").read_text().splitlines()]
    assert any(e["event"] == "stage_failed" and e["stage"] == stage for e in events)
    assert any(e["event"] == "stage_finished" and e["stage"] == stage for e in events)


def test_failed_transform_does_not_publish(settings, monkeypatch):
    import pipeline.flow as module

    run_pipeline(settings)
    initial = fingerprint(settings.warehouse)
    def broken(*args):
        raise ValueError("Invalid SQL/schema: not retryable")
    monkeypatch.setattr(module, "dbt", broken)
    with pytest.raises(ValueError, match="Invalid SQL"):
        run_pipeline(settings)
    assert fingerprint(settings.warehouse) == initial


def test_retry_budget(settings, monkeypatch):
    import pipeline.flow as module

    attempts = []
    def unavailable(*args):
        attempts.append(1)
        raise OSError("Storage unavailable")
    monkeypatch.setattr(module, "read_source", unavailable)
    with pytest.raises(OSError, match="Storage unavailable"):
        run_pipeline(settings)
    assert len(attempts) == 4  # первая попытка + три повтора
    assert not settings.warehouse.exists()
