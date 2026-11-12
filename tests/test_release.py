import json
import shutil

import duckdb
import pytest

from pipeline.flow import run_pipeline
from pipeline.release import metrics, promote
from pipeline.storage import fingerprint


def test_promotion_smoke_and_rollback(settings):
    run_pipeline(settings)
    candidate = settings.work / "release.duckdb"
    shutil.copy2(settings.warehouse, candidate)
    production = settings.work / "production"
    assert promote(candidate, production)["status"] == "promoted"
    before = fingerprint(production / "warehouse.duckdb")
    assert metrics(candidate)["rows"] == 6
    with duckdb.connect(str(candidate)) as con:
        con.execute("UPDATE raw.trips SET fare_amount=fare_amount+1")
        con.execute("UPDATE analytics.stg_trips SET fare_usd=fare_usd+1")
        con.execute("UPDATE analytics.daily_revenue SET revenue_usd=revenue_usd+trips")
    assert fingerprint(candidate) != before
    with pytest.raises(ValueError, match="post-deploy"):
        promote(candidate, production, fail_smoke=True)
    assert fingerprint(production / "warehouse.duckdb") == before
    assert json.loads((production / "deployment.json").read_text())["status"] == "rolled_back"
    with duckdb.connect(str(candidate)) as con:
        con.execute("UPDATE raw.trips SET vendor_id=2")
    with pytest.raises(ValueError, match="Data drift"):
        promote(candidate, production)
    assert fingerprint(production / "warehouse.duckdb") == before
