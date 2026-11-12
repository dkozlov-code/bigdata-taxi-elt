import runpy
from types import SimpleNamespace

from pipeline import flow
from pipeline.config import ROOT


def test_daily_deployment(monkeypatch):
    calls = {}
    monkeypatch.setattr(flow, "demonstrate", lambda settings: calls.update(demo=True))
    monkeypatch.setattr(flow, "run_pipeline", SimpleNamespace(serve=lambda **kwargs: calls.update(kwargs)))
    runpy.run_path(str(ROOT / "prefect/dags/etl_pipeline.py"), run_name="__main__")
    assert calls["demo"]
    assert calls["schedule"].cron == "0 4 * * *"
    assert calls["schedule"].timezone == "America/New_York"
    assert calls["limit"] == 1
