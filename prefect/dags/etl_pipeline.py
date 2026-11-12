"""Первый демонстрационный запуск и ежедневный deployment в Prefect UI."""
import sys
from pathlib import Path

from prefect.schedules import Cron

# Каталог prefect не является Python-пакетом и не затеняет библиотеку Prefect.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.config import Settings
from pipeline.flow import demonstrate, run_pipeline

if __name__ == "__main__":
    demonstrate(Settings.from_env())
    run_pipeline.serve(
        name="daily-revenue", schedule=Cron("0 4 * * *", timezone="America/New_York"),
        pause_on_shutdown=False, limit=1,
    )
