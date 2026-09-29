"""Partitioned, append-and-dedupe Parquet store.

Layout (partition day = UTC date of timestamp_utc):
  data/snapshots/<stream>/year=YYYY/month=MM/<stream>_YYYY-MM-DD.parquet
  data/trend/year=YYYY/month=MM/trend_YYYY-MM-DD.parquet

Merging is idempotent: re-ingesting the same rows never adds rows, and a file is
only rewritten when its content actually changes (so re-runs produce no git diff).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config

TS_UTC = pa.timestamp("ms", tz="UTC")
TS_LOCAL = pa.timestamp("ms")

SNAPSHOT_SCHEMA = pa.schema([
    ("timestamp_utc", TS_UTC),
    ("timestamp_local", TS_LOCAL),
    ("stream", pa.string()),
    ("entity", pa.string()),
    ("field", pa.string()),
    ("value", pa.float64()),
    ("value_text", pa.string()),
    ("unit", pa.string()),
    ("source_update_utc", TS_UTC),
    ("fetched_at_utc", TS_UTC),
    ("chunk", pa.string()),
])
SNAPSHOT_KEY = ["stream", "entity", "field", "timestamp_utc"]

TREND_SCHEMA = pa.schema([
    ("timestamp_utc", TS_UTC),
    ("timestamp_local", TS_LOCAL),
    ("stream", pa.string()),
    ("entity", pa.string()),
    ("field", pa.string()),
    ("value", pa.float64()),
    ("unit", pa.string()),
    ("minute_label_local", TS_LOCAL),
    ("first_seen_update_utc", TS_UTC),
    ("last_seen_update_utc", TS_UTC),
])
TREND_KEY = ["entity", "timestamp_utc", "value"]


def _local_from_utc(s: pd.Series) -> pd.Series:
    return (s.dt.tz_convert("UTC") - pd.Timedelta(hours=5)).dt.tz_localize(None)


def _normalise(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    df = df.copy()
    for f in schema:
        if f.name not in df.columns:
            df[f.name] = None
    df = df[[f.name for f in schema]]
    for f in schema:
        if pa.types.is_timestamp(f.type):
            s = pd.to_datetime(df[f.name], utc=f.type.tz is not None)
            df[f.name] = s.dt.floor("ms").astype("datetime64[ms, UTC]" if f.type.tz else "datetime64[ms]")
        elif pa.types.is_floating(f.type):
            df[f.name] = pd.to_numeric(df[f.name], errors="coerce").astype("float64")
        else:
            df[f.name] = df[f.name].astype(object).where(df[f.name].notna(), None)
    # Local wall clock is always derived from UTC with the fixed -5h offset.
    df["timestamp_local"] = _local_from_utc(df["timestamp_utc"]).astype("datetime64[ms]")
    return df


def _to_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    return table.replace_schema_metadata(None)


def read_parquet(path: Path, schema: pa.Schema) -> pd.DataFrame:
    table = pq.read_table(path)
    df = table.to_pandas()
    return _normalise(df, schema)


def _write(path: Path, df: pd.DataFrame, schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(
        _to_table(df, schema), tmp,
        compression="zstd", compression_level=9, use_dictionary=True,
        write_statistics=True, row_group_size=1_000_000,
    )
    tmp.replace(path)


def _same(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if len(a) != len(b):
        return False
    return a.reset_index(drop=True).equals(b.reset_index(drop=True))


# --------------------------------------------------------------------------
# Snapshots
# --------------------------------------------------------------------------
def snapshot_path(root: Path, stream: str, day: pd.Timestamp) -> Path:
    return root / stream / f"year={day:%Y}" / f"month={day:%m}" / f"{stream}_{day:%Y-%m-%d}.parquet"


def dedupe_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["fetched_at_utc", "chunk"], kind="stable")
    df = df.drop_duplicates(SNAPSHOT_KEY, keep="first")
    return df.sort_values(["stream", "entity", "field", "timestamp_utc"], kind="stable").reset_index(drop=True)


def merge_snapshots(new: pd.DataFrame, root: Path = config.SNAPSHOT_DIR) -> list[Path]:
    """Merge new snapshot rows into their daily partitions. Returns changed files."""
    if new.empty:
        return []
    new = _normalise(new, SNAPSHOT_SCHEMA)
    changed = []
    day = new["timestamp_utc"].dt.floor("D")
    for (stream, d), part in new.groupby([new["stream"], day], sort=True):
        path = snapshot_path(root, stream, d)
        existing = read_parquet(path, SNAPSHOT_SCHEMA) if path.exists() else None
        combined = part if existing is None else pd.concat([existing, part], ignore_index=True)
        combined = dedupe_snapshots(combined)
        if existing is not None and _same(dedupe_snapshots(existing), combined):
            continue
        _write(path, combined, SNAPSHOT_SCHEMA)
        changed.append(path)
    return changed


# --------------------------------------------------------------------------
# Trend
# --------------------------------------------------------------------------
def trend_path(root: Path, day: pd.Timestamp) -> Path:
    return root / f"year={day:%Y}" / f"month={day:%m}" / f"trend_{day:%Y-%m-%d}.parquet"


def dedupe_trend(df: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct (series, sample instant, value). A value that is
    revised by the source gets a second row; first/last_seen record when each
    version was published. Aggregation is min/max, so it is idempotent."""
    if df.empty:
        return df
    agg = (
        df.groupby(TREND_KEY, sort=False, dropna=False)
        .agg(
            timestamp_local=("timestamp_local", "first"),
            stream=("stream", "first"),
            field=("field", "first"),
            unit=("unit", "first"),
            minute_label_local=("minute_label_local", "first"),
            first_seen_update_utc=("first_seen_update_utc", "min"),
            last_seen_update_utc=("last_seen_update_utc", "max"),
        )
        .reset_index()
    )
    agg = agg[[f.name for f in TREND_SCHEMA]]
    return agg.sort_values(["entity", "timestamp_utc", "first_seen_update_utc", "value"], kind="stable").reset_index(drop=True)


def merge_trend(new: pd.DataFrame, root: Path = config.TREND_DIR) -> list[Path]:
    if new.empty:
        return []
    new = _normalise(new, TREND_SCHEMA)
    changed = []
    day = new["timestamp_utc"].dt.floor("D")
    for d, part in new.groupby(day, sort=True):
        path = trend_path(root, d)
        existing = read_parquet(path, TREND_SCHEMA) if path.exists() else None
        combined = part if existing is None else pd.concat([existing, part], ignore_index=True)
        combined = dedupe_trend(_normalise(combined, TREND_SCHEMA))
        if existing is not None and _same(dedupe_trend(existing), combined):
            continue
        _write(path, combined, TREND_SCHEMA)
        changed.append(path)
    return changed


def load_trend(root: Path = config.TREND_DIR) -> pd.DataFrame:
    files = sorted(root.glob("year=*/month=*/trend_*.parquet"))
    if not files:
        return _normalise(pd.DataFrame(), TREND_SCHEMA).iloc[0:0]
    return pd.concat([read_parquet(f, TREND_SCHEMA) for f in files], ignore_index=True)


def canonical_trend(samples: pd.DataFrame) -> pd.DataFrame:
    """Latest published value per (series, sample instant)."""
    if samples.empty:
        return samples
    s = samples.sort_values(["entity", "timestamp_utc", "last_seen_update_utc"], kind="stable")
    return s.drop_duplicates(["entity", "timestamp_utc"], keep="last").reset_index(drop=True)


def minute_trend(samples: pd.DataFrame) -> pd.DataFrame:
    """Canonical 1-minute series (long format): per series and minute label, the
    mean of the canonical values of every sample held within that minute, and
    how many distinct sample instants contributed."""
    c = canonical_trend(samples)
    if c.empty:
        return pd.DataFrame(columns=["timestamp_utc", "timestamp_local", "stream", "entity", "field", "value", "unit", "n_samples"])
    g = c.groupby(["entity", "minute_label_local"], sort=True).agg(value=("value", "mean"), n_samples=("value", "size")).reset_index()
    g["timestamp_local"] = g["minute_label_local"]
    g["timestamp_utc"] = (g["minute_label_local"] + pd.Timedelta(hours=5)).dt.tz_localize("UTC")
    g["stream"] = "sin.trend.minute"
    g["field"] = "value"
    g["unit"] = "MW"
    return g[["timestamp_utc", "timestamp_local", "stream", "entity", "field", "value", "unit", "n_samples"]]
