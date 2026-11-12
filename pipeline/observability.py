import json
import logging
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger("taxi")


def event(work: Path, kind: str, **values):
    work.mkdir(parents=True, exist_ok=True)
    record = {"ts": datetime.now(UTC).isoformat(), "event": kind, **values}
    with (work / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    logger.info("%s", record)


@contextmanager
def measured(work: Path, stage: str):
    start = time.perf_counter()
    event(work, "stage_start", stage=stage)
    try:
        yield
    except Exception as exc:
        event(work, "stage_failed", stage=stage, error=str(exc))
        raise
    else:
        event(work, "stage_finished", stage=stage, seconds=time.perf_counter() - start)


def inject(work: Path, stage: str, fault: str | None):
    """Одноразовый операционный сбой; marker переживает повтор задачи."""
    if fault == stage:
        marker = work / f"fault-{stage}"
        if not marker.exists():
            marker.touch()
            raise OSError(f"Injected transient failure: {stage}")
