"""Consolidated, human-openable exports (published weekly to the `exports` release).

export/
  gap_report.txt
  trend/sitr_trend_minute_YYYY.{parquet,csv}   canonical 1-minute series, long format
  trend/sitr_trend_samples_YYYY.parquet        every sample and every revision
  snapshots/<stream>/<stream>_YYYY-MM.parquet  one file per stream per month
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config, gaps, store


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(None)
    pq.write_table(table, path, compression="zstd", compression_level=9)


def export_all(data_dir: Path = config.DATA_DIR, out: Path = Path("export")) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    report = gaps.report(data_dir)
    (out / "gap_report.txt").write_text(report + "\n", encoding="utf-8")
    written.append(out / "gap_report.txt")

    samples = store.load_trend(data_dir / "trend")
    if not samples.empty:
        minute = store.minute_trend(samples)
        for year, part in samples.groupby(samples["timestamp_utc"].dt.year):
            p = out / "trend" / f"sitr_trend_samples_{year}.parquet"
            _write_parquet(part.reset_index(drop=True), p)
            written.append(p)
        for year, part in minute.groupby(minute["timestamp_utc"].dt.year):
            part = part.reset_index(drop=True)
            p = out / "trend" / f"sitr_trend_minute_{year}.parquet"
            _write_parquet(part, p)
            c = p.with_suffix(".csv")
            csv = part.copy()
            csv["timestamp_utc"] = csv["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            csv["timestamp_local"] = csv["timestamp_local"].dt.strftime("%Y-%m-%d %H:%M:%S")
            csv.to_csv(c, index=False, lineterminator="\n", encoding="utf-8")
            written += [p, c]

    for stream_dir in sorted((data_dir / "snapshots").glob("*")):
        files = sorted(stream_dir.glob("year=*/month=*/*.parquet"))
        by_month: dict[str, list[Path]] = {}
        for f in files:
            by_month.setdefault(f.stem.rsplit("_", 1)[-1][:7], []).append(f)
        for month, fs in by_month.items():
            df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
            p = out / "snapshots" / stream_dir.name / f"{stream_dir.name}_{month}.parquet"
            _write_parquet(df, p)
            written.append(p)
    print(f"[export] {len(written)} files in {out}", flush=True)
    return written
