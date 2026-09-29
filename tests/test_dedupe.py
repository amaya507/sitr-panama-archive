"""Idempotency: re-running any ingest must never add rows or rewrite files."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from conftest import ALL_1024, make_chunk, raw
from sitr import store
from sitr.consolidate import consolidate


def _hashes(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.md5(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*.parquet")) if "fetch_log" not in str(p)}


def _all_snapshots(data_dir: Path) -> pd.DataFrame:
    return pd.concat([pd.read_parquet(p) for p in (data_dir / "snapshots").rglob("*.parquet")], ignore_index=True)


def test_reingest_same_chunk_is_a_no_op(chunk_dir, data_dir):
    make_chunk(chunk_dir, "20260929T102401Z", {ep: raw(f) for ep, f in ALL_1024.items()})
    r1 = consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    assert len(r1.ingested) == 1 and r1.changed_files
    before = _hashes(data_dir)
    n_before = len(_all_snapshots(data_dir))

    # Ledger prevents re-work...
    r2 = consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    assert r2.ingested == []
    # ...and even with the ledger ignored, nothing changes.
    r3 = consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"), use_ledger=False)
    assert r3.changed_files == []
    assert _hashes(data_dir) == before
    assert len(_all_snapshots(data_dir)) == n_before


def test_same_source_update_fetched_twice_is_one_observation(chunk_dir, data_dir):
    """Two fetches of the same 15-s file generation (identical `update`) collapse."""
    body = raw("gen_20260929T1024Z")
    make_chunk(chunk_dir, "20260929T102401Z", {"gen": body})
    make_chunk(chunk_dir, "20260929T102405Z", {"gen": body})
    consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    df = _all_snapshots(data_dir)
    unit = df[df.stream == "gen.unit"]
    assert len(unit) == 271 * 2  # 271 units x (value, color), once
    assert not unit.duplicated(store.SNAPSHOT_KEY).any()
    assert set(unit["chunk"]) == {"sitr_20260929T102401Z_snap_ok1of1.tar.gz"}  # first fetch wins


def test_trend_overlap_dedupes_and_keeps_revisions(chunk_dir, data_dir):
    """Two real files one minute apart at the same seconds offset overlap on
    1,440 minutes. Unchanged samples collapse; revised ones are kept as a
    second version, and the canonical view picks the most recent."""
    make_chunk(chunk_dir, "20260929T102728Z", {"sin": raw("sin_20260929T1027Z_a")})
    make_chunk(chunk_dir, "20260929T102839Z", {"sin": raw("sin_20260929T1028Z_b")})
    consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    t = store.load_trend(data_dir / "trend")
    instants = t.drop_duplicates(["entity", "timestamp_utc"])
    # 1441 + 1 new instant per series, not 2 x 1441
    assert len(instants) == 1442 * 3
    revised = t.groupby(["entity", "timestamp_utc"]).size()
    assert 0 < (revised > 1).sum() < 60  # a handful of revisions (forecast rounding + live point)
    canon = store.canonical_trend(t)
    assert not canon.duplicated(["entity", "timestamp_utc"]).any()
    # The live point at 05:27:23 was revised from 1409 to 1412 MW in the later file.
    gen = canon[(canon.entity == "Generación") & (canon.timestamp_local == pd.Timestamp("2026-09-29 05:27:23"))]
    assert gen["value"].tolist() == [1412.0]

    before = _hashes(data_dir)
    consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"), use_ledger=False)
    assert _hashes(data_dir) == before


def test_minute_series_is_long_format():
    df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime(["2026-09-29T10:00:10Z", "2026-09-29T10:00:40Z"]),
        "entity": ["Generación"] * 2, "value": [100.0, 110.0], "stream": "sin.trend", "field": "value",
        "unit": "MW", "minute_label_local": pd.to_datetime(["2026-09-29 05:00", "2026-09-29 05:00"]),
        "first_seen_update_utc": pd.to_datetime(["2026-09-29T10:00:10Z"] * 2),
        "last_seen_update_utc": pd.to_datetime(["2026-09-29T10:00:10Z"] * 2),
    })
    m = store.minute_trend(store._normalise(df, store.TREND_SCHEMA))
    assert list(m.columns[:7]) == ["timestamp_utc", "timestamp_local", "stream", "entity", "field", "value", "unit"]
    assert m["value"].tolist() == [105.0] and m["n_samples"].tolist() == [2]
    assert m["timestamp_utc"].iloc[0] == pd.Timestamp("2026-09-29T10:00:00Z")
