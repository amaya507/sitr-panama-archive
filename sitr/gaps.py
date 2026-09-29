"""Completeness accounting.

Trend: a minute (Panama-local label) is HELD when every expected series has at
least one sample in it. Coverage is measured from the first held minute to the
last minute covered by any ingested fetch. Also reported: how many distinct
sample instants (seconds offsets) were captured per minute. The source file is
regenerated roughly every 15 s with a drifting offset, so each fetch samples
every minute at its own second; more fetches -> more samples per minute.

Snapshots: per UTC day, how many distinct source updates of gen.unit were
stored and the spacing between them (the effective polling resolution).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config, store


def trend_coverage(data_dir: Path = config.DATA_DIR, since: str | None = None, until: str | None = None) -> dict:
    samples = store.load_trend(data_dir / "trend")
    if samples.empty:
        return {"held_minutes": 0, "expected_minutes": 0, "missing": pd.DataFrame(), "held": pd.DataFrame()}
    series = sorted(samples["entity"].unique())
    per = samples.groupby(["minute_label_local", "entity"]).size().unstack("entity")
    per = per.reindex(columns=series)
    complete = per.notna().all(axis=1)
    held_minutes = per.index[complete]
    start = pd.Timestamp(since) if since else held_minutes.min()
    end = pd.Timestamp(until) if until else per.index.max()
    grid = pd.date_range(start, end, freq="min")
    held_set = set(held_minutes)
    is_held = pd.Series([t in held_set for t in grid], index=grid)
    missing = _intervals(grid[~is_held.values])
    held = _intervals(grid[is_held.values])
    phases = samples.drop_duplicates(["minute_label_local", "timestamp_utc"]).groupby("minute_label_local").size()
    phases = phases.reindex(grid).fillna(0)
    return {
        "start_local": start, "end_local": end, "series": series,
        "expected_minutes": len(grid), "held_minutes": int(is_held.sum()),
        "missing": missing, "held": held,
        "phase_mean": float(phases[is_held.values].mean()) if is_held.any() else 0.0,
        "phase_hist": phases[is_held.values].value_counts().sort_index().to_dict(),
        "revised_samples": _revision_stats(samples),
    }


def _revision_stats(samples: pd.DataFrame) -> dict:
    n_versions = samples.groupby(["entity", "timestamp_utc"]).size()
    out = {}
    for ent, s in n_versions.groupby(level=0):
        out[ent] = {"samples": int(len(s)), "revised": int((s > 1).sum())}
    return out


def _intervals(times: pd.DatetimeIndex) -> pd.DataFrame:
    cols = ["start_local", "end_local", "start_utc", "end_utc", "minutes"]
    if len(times) == 0:
        return pd.DataFrame(columns=cols)
    t = pd.Series(times)
    brk = t.diff() != pd.Timedelta(minutes=1)
    grp = brk.cumsum()
    iv = t.groupby(grp).agg(["min", "max", "size"]).reset_index(drop=True)
    iv.columns = ["start_local", "end_local", "minutes"]
    iv["start_utc"] = (iv["start_local"] + pd.Timedelta(hours=5)).dt.tz_localize("UTC")
    iv["end_utc"] = (iv["end_local"] + pd.Timedelta(hours=5)).dt.tz_localize("UTC")
    return iv[cols]


def snapshot_cadence(data_dir: Path = config.DATA_DIR, stream: str = "gen.unit") -> pd.DataFrame:
    files = sorted((data_dir / "snapshots" / stream).glob("year=*/month=*/*.parquet"))
    rows = []
    for f in files:
        ts = pd.read_parquet(f, columns=["source_update_utc"])["source_update_utc"].dropna().drop_duplicates().sort_values()
        if ts.empty:
            continue
        gaps = ts.diff().dropna().dt.total_seconds() / 60
        rows.append({
            "day_utc": f.stem.rsplit("_", 1)[-1], "observations": len(ts),
            "median_spacing_min": round(float(gaps.median()), 2) if len(gaps) else None,
            "p95_spacing_min": round(float(gaps.quantile(0.95)), 2) if len(gaps) else None,
            "max_spacing_min": round(float(gaps.max()), 2) if len(gaps) else None,
            "pct_of_288": round(100 * len(ts) / 288, 1),
        })
    return pd.DataFrame(rows)


def write_manifests(data_dir: Path = config.DATA_DIR) -> dict:
    cov = trend_coverage(data_dir)
    mdir = data_dir / "manifest"
    mdir.mkdir(parents=True, exist_ok=True)
    fmt = "%Y-%m-%d %H:%M"
    for name in ("held", "missing"):
        df = cov[name].copy() if isinstance(cov.get(name), pd.DataFrame) else pd.DataFrame()
        for c in ("start_local", "end_local"):
            if c in df:
                df[c] = pd.to_datetime(df[c]).dt.strftime(fmt)
        for c in ("start_utc", "end_utc"):
            if c in df:
                df[c] = pd.to_datetime(df[c]).dt.strftime(fmt + "Z")
        df.to_csv(mdir / f"trend_{name}_intervals.csv", index=False, lineterminator="\n")
    snapshot_cadence(data_dir).to_csv(mdir / "snapshot_cadence.csv", index=False, lineterminator="\n")
    return cov


def report(data_dir: Path = config.DATA_DIR, since: str | None = None, until: str | None = None) -> str:
    cov = trend_coverage(data_dir, since, until)
    lines = ["SITR capture — completeness report", "=" * 40]
    if not cov["expected_minutes"]:
        lines.append("No trend data ingested yet.")
    else:
        pct = 100 * cov["held_minutes"] / cov["expected_minutes"]
        miss = cov["missing"]
        lines += [
            f"Trend series ({', '.join(cov['series'])})",
            f"  window (Panama local): {cov['start_local']:%Y-%m-%d %H:%M} -> {cov['end_local']:%Y-%m-%d %H:%M}",
            f"  minutes expected: {cov['expected_minutes']:,}   held: {cov['held_minutes']:,}   "
            f"missing: {cov['expected_minutes'] - cov['held_minutes']:,}",
            f"  COMPLETENESS: {pct:.3f}%",
            f"  distinct sample instants per held minute: mean {cov['phase_mean']:.2f}; min {min(cov['phase_hist']) if cov['phase_hist'] else 0}",
            f"  missing intervals: {len(miss)}",
        ]
        for r in miss.head(50).itertuples():
            lines.append(f"    {r.start_local:%Y-%m-%d %H:%M} -> {r.end_local:%Y-%m-%d %H:%M} local  ({r.minutes} min)")
        if len(miss) > 50:
            lines.append(f"    ... {len(miss) - 50} more in data/manifest/trend_missing_intervals.csv")
        lines.append("  revisions (samples whose value the source changed after first publication):")
        for ent, s in cov["revised_samples"].items():
            lines.append(f"    {ent}: {s['revised']:,} of {s['samples']:,} samples revised")
    cad = snapshot_cadence(data_dir)
    lines += ["", "Snapshot cadence (gen.unit distinct source updates per UTC day; 288 = every 5 min)"]
    if cad.empty:
        lines.append("  no snapshot data ingested yet.")
    else:
        lines.append(cad.tail(31).to_string(index=False))
    return "\n".join(lines)
