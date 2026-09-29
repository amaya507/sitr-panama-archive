"""End-to-end check: does the stored trend match what the site serves right now?

Fetches sin.json once, parses it with the same code as the pipeline, and
compares against data/trend:
  * every minute in the fresh 24 h window that is older than the newest stored
    sample must be HELD in the store (at least one sample per series);
  * where the store has a sample at the exact same instant (same 15-s phase),
    the values are compared. Generation should match exactly; Demanda Real may
    differ by a few MW because the source revises it.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config, store
from .fetch import fetch
from .parse import parse_endpoint


def verify(data_dir: Path = config.DATA_DIR, body: bytes | None = None) -> tuple[bool, str]:
    if body is None:
        r = fetch("sin")
        if not r.ok:
            return False, f"FAIL: could not fetch sin.json ({r.status} {r.error})"
        body, fetched = r.body, r.fetched_at_utc
    else:
        fetched = pd.Timestamp.now(tz="UTC").to_pydatetime()
    _, trend_rows, drift, update_utc = parse_endpoint("sin", body, fetched_at_utc=fetched)
    fresh = pd.DataFrame(trend_rows)
    stored = store.load_trend(data_dir / "trend")
    lines = [f"fresh sin.json update (UTC): {update_utc}", f"fresh points: {len(fresh)//3} x 3 series"]
    if drift:
        lines.append(f"drift in fresh file: {[d.kind for d in drift]}")
    if stored.empty:
        return False, "\n".join(lines + ["FAIL: store is empty"])

    newest = stored["timestamp_utc"].max()
    fresh["timestamp_utc"] = pd.to_datetime(fresh["timestamp_utc"], utc=True)
    window = fresh[fresh["timestamp_utc"] <= newest]
    minutes = sorted(window["minute_label_local"].unique())
    held = stored.groupby("minute_label_local")["entity"].nunique()
    series_n = fresh["entity"].nunique()
    missing = [m for m in minutes if held.get(m, 0) < series_n]
    lines += [
        f"newest stored sample (UTC): {newest}",
        f"minutes of the fresh window already in store's time range: {len(minutes)}",
        f"of those, held in store: {len(minutes) - len(missing)}   missing: {len(missing)}",
    ]
    canon = store.canonical_trend(stored)[["entity", "timestamp_utc", "value"]]
    canon["timestamp_utc"] = pd.to_datetime(canon["timestamp_utc"], utc=True)
    j = window.merge(canon, on=["entity", "timestamp_utc"], suffixes=("_fresh", "_stored"))
    if len(j):
        j["diff"] = (j["value_fresh"] - j["value_stored"]).abs()
        for ent, g in j.groupby("entity"):
            lines.append(f"  same-instant comparisons, {ent}: n={len(g)}, exact={int((g['diff'] == 0).sum())}, "
                         f"max |diff|={g['diff'].max():.0f} MW")
    else:
        lines.append("  no same-instant samples to compare (fresh file sampled at a new seconds offset)")

    # Minute-level agreement: fresh sample vs the stored mean for that minute.
    # A misdecoded month/day/hour would break this badly (wrong load shape),
    # while sampling noise and demand revisions stay within a few MW.
    minute = store.minute_trend(stored)[["entity", "timestamp_local", "value"]]
    minute = minute.rename(columns={"timestamp_local": "minute_label_local", "value": "value_stored"})
    w = window.merge(minute, on=["entity", "minute_label_local"])
    corr_ok = True
    for ent, g in w.groupby("entity"):
        d = (g["value"] - g["value_stored"]).abs()
        corr = g["value"].corr(g["value_stored"]) if len(g) > 2 else float("nan")
        lines.append(f"  minute-level, {ent}: n={len(g)}, median |diff|={d.median():.1f} MW, "
                     f"p99 |diff|={d.quantile(0.99):.1f} MW, corr={corr:.4f}")
        if ent in ("Generación", "Demanda Real") and len(g) > 60 and not (corr > 0.98 and d.median() < 15):
            corr_ok = False
    total_minutes = stored["minute_label_local"].nunique()
    lines.append(f"store holds {total_minutes:,} distinct minutes ({total_minutes / 1440:.2f} days)")
    gen = j[j["entity"] == "Generación"] if len(j) else j
    ok = not missing and corr_ok and (len(gen) == 0 or (gen["diff"] == 0).mean() > 0.99)
    lines.append("PASS" if ok else "FAIL")
    return ok, "\n".join(lines)
