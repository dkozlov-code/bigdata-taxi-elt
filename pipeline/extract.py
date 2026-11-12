import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.observability import event

NUMERIC = ["vendor_id", "pickup_longitude", "pickup_latitude", "fare_amount", "trip_distance"]
DATES = ["pickup_datetime", "dropoff_datetime"]
COLUMNS = ["vendor_id", *DATES, "pickup_longitude", "pickup_latitude", "fare_amount", "trip_distance"]
# Нет исходного trip_id: фиксируем составной естественный ключ без изменяемой стоимости.
KEY = ["vendor_id", *DATES, "pickup_longitude", "pickup_latitude"]
ALIASES = {"VendorID": "vendor_id", "tpep_pickup_datetime": "pickup_datetime",
           "tpep_dropoff_datetime": "dropoff_datetime"}


class SchemaDriftError(ValueError):
    pass


def quarantine(frame: pd.DataFrame, work: Path, reason: str):
    directory = work / "dlq"
    directory.mkdir(parents=True, exist_ok=True)
    # Адресация по содержимому: повторы не накапливают одинаковые ошибки.
    payload = frame.to_json(orient="records", date_format="iso")
    digest = hashlib.sha256((reason + payload).encode()).hexdigest()
    (directory / f"{digest}.json").write_text(
        json.dumps({"reason": reason, "rows": json.loads(payload)}, ensure_ascii=False)
    )
    event(work, "quarantine", reason=reason, rows=len(frame), file=digest)


def read_source(path: Path, work: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    frame = frame.rename(columns=ALIASES)
    missing = sorted(set(COLUMNS) - set(frame))
    extra = sorted(set(frame) - set(COLUMNS))
    if missing:
        quarantine(frame, work, f"missing_columns:{','.join(missing)}")
        event(work, "alert", reason="schema_drift", missing=missing)
        raise SchemaDriftError(f"Нет обязательных полей: {missing}; публикация сохранена")
    if extra:
        # Совместимое расширение: явная проекция на предыдущий контракт.
        event(work, "alert", reason="schema_projection", ignored=extra)
    frame = frame[COLUMNS].copy()
    for column in NUMERIC:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in DATES:
        frame[column] = pd.to_datetime(frame[column], errors="coerce")
    normalized = frame[KEY].copy()
    for column in ["vendor_id", "pickup_longitude", "pickup_latitude"]:
        normalized[column] = normalized[column].astype(float)
    canonical = normalized.astype(str).agg("|".join, axis=1)
    frame["idempotency_key"] = canonical.map(lambda value: hashlib.sha256(value.encode()).hexdigest())
    return frame


def split_valid(frame: pd.DataFrame, work: Path):
    valid = (
        frame[COLUMNS].notna().all(axis=1)
        & frame.vendor_id.isin([1, 2])
        & frame.fare_amount.between(0, 10000)
        & frame.trip_distance.between(0.001, 1000)
        & frame.pickup_latitude.between(40.49, 40.92)
        & frame.pickup_longitude.between(-74.27, -73.68)
        & (frame.dropoff_datetime > frame.pickup_datetime)
    )
    bad = frame.loc[~valid]
    if len(bad):
        quarantine(bad, work, "invalid_values")
    good = frame.loc[valid].drop_duplicates("idempotency_key", keep="last").copy()
    if good.empty:
        raise ValueError("Нет валидных строк; предыдущая публикация сохранена")
    return good
