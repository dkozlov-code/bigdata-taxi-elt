import os
import shutil

import pytest
from prefect.testing.utilities import prefect_test_harness

from pipeline.config import ROOT, Settings

os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("DO_NOT_TRACK", "1")


@pytest.fixture(scope="session", autouse=True)
def prefect_server():
    # Изолированная metadata DB; остановка сервера до закрытия pytest capture.
    with prefect_test_harness():
        yield


@pytest.fixture
def settings(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(ROOT / "data/sample", source)
    return Settings(source=source, work=tmp_path / "work")
