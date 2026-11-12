import os
import subprocess
import sys
from pathlib import Path

from pipeline.config import ROOT


def dbt(database: Path, output: Path):
    env = os.environ | {
        "TAXI_DB_PATH": str(database.resolve()),
        "DBT_TARGET_PATH": str(output / "dbt"),
        "DBT_LOG_PATH": str(output / "dbt-logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    executable = str(Path(sys.executable).with_name("dbt"))
    for args in (["build"], ["docs", "generate", "--static"]):
        with (output / f"dbt-{args[0]}.log").open("w") as log:
            subprocess.run(
                [executable, *args, "--project-dir", str(ROOT / "dbt"),
                 "--profiles-dir", str(ROOT / "dbt")],
                env=env, stdout=log, stderr=subprocess.STDOUT, timeout=600, check=True,
            )
