"""Staging, canary 5% → 100%, smoke gates и rollback локальной публикации."""
import argparse
import json
import math
import shutil
from pathlib import Path

import duckdb
import pandas as pd

from pipeline.config import ROOT, Settings
from pipeline.flow import run_pipeline
from pipeline.storage import single_writer


def metrics(database: Path):
    with duckdb.connect(str(database), read_only=True) as con:
        rows, keys, revenue = con.execute("""
            SELECT count(*), count(DISTINCT idempotency_key), sum(fare_amount)
            FROM raw.trips
        """).fetchone()
        trips, mart_revenue = con.execute("""
            SELECT sum(trips), sum(revenue_usd) FROM analytics.daily_revenue
        """).fetchone()
        distribution = dict(con.execute("""
            SELECT vendor_id, count(*)::DOUBLE / (SELECT count(*) FROM raw.trips)
            FROM raw.trips GROUP BY vendor_id
        """).fetchall())
    if not rows or rows != keys or trips != rows or abs(float(revenue) - float(mart_revenue)) > .01:
        raise ValueError("Smoke test: keys, trips or revenue do not reconcile")
    return {"rows": rows, "revenue_usd": float(mart_revenue), "vendor_shares": distribution}


def stage(source: Path, work: Path):
    canary = work / "canary-source"
    canary.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.iterdir()):
        if path.suffix not in {".csv", ".parquet"}:
            continue
        frame = pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)
        frame.head(max(1, math.ceil(len(frame) * .05))).to_parquet(canary / f"{path.stem}.parquet", index=False)
    small = run_pipeline(Settings(source=canary, work=work / "canary"))
    metrics(work / "canary/warehouse.duckdb")
    full = run_pipeline(Settings(source=source, work=work / "staging"))
    business = metrics(work / "staging/warehouse.duckdb")
    report = {"canary_fraction": .05, "canary": small, "staging": full, "business": business}
    (work / "comparison.json").write_text(json.dumps(report, indent=2))
    return report


def promote(candidate: Path, production: Path, *, fail_smoke=False):
    production.mkdir(parents=True, exist_ok=True)
    current, previous = production / "warehouse.duckdb", production / "previous.duckdb"
    report = {"candidate": metrics(candidate)}
    with single_writer(production):
        had_previous = current.exists()
        if had_previous:
            report["before"] = metrics(current)
            before = report["before"]["vendor_shares"]
            after = report["candidate"]["vendor_shares"]
            # Total variation distance: 0 — прежнее распределение, 1 — полностью другое.
            drift = sum(abs(before.get(k, 0) - after.get(k, 0)) for k in before.keys() | after.keys()) / 2
            report["vendor_drift"] = drift
            if drift > .2:
                report["status"] = "blocked_by_drift"
                (production / "deployment.json").write_text(json.dumps(report, indent=2))
                raise ValueError(f"Data drift {drift:.3f} exceeds 0.2; publication blocked")
            shutil.copy2(current, previous)
        shutil.copy2(candidate, production / "candidate.duckdb")
        (production / "candidate.duckdb").replace(current)
        try:
            if fail_smoke:
                raise ValueError("Injected post-deploy smoke failure")
            report["after"] = metrics(current)
        except (ValueError, duckdb.Error):
            if had_previous:
                shutil.copy2(previous, production / "rollback.duckdb")
                (production / "rollback.duckdb").replace(current)
            else:
                current.unlink()
            report["status"] = "rolled_back"
            (production / "deployment.json").write_text(json.dumps(report, indent=2))
            raise
        report["status"] = "promoted"
        (production / "deployment.json").write_text(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["stage", "promote"])
    parser.add_argument("--work", type=Path, default=ROOT / "runtime/release")
    parser.add_argument("--source", type=Path, default=ROOT / "data/sample")
    args = parser.parse_args()
    result = stage(args.source, args.work) if args.command == "stage" else promote(
        args.work / "staging/warehouse.duckdb", args.work / "production",
    )
    print(json.dumps(result, indent=2))
