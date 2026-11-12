import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    source: Path = ROOT / "data/sample"
    work: Path = ROOT / "runtime"
    dlq_threshold: int = 1
    max_seconds: float = 3600

    @property
    def warehouse(self) -> Path:
        return self.work / "warehouse.duckdb"

    @classmethod
    def from_env(cls):
        return cls(
            source=Path(os.getenv("TAXI_SOURCE", ROOT / "data/sample")),
            work=Path(os.getenv("TAXI_WORK", ROOT / "runtime")),
            dlq_threshold=int(os.getenv("DLQ_THRESHOLD", "1")),
            max_seconds=float(os.getenv("MAX_DURATION_SEC", "3600")),
        )
