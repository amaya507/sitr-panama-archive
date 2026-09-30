"""Ingest raw chunks into the parsed store.

Idempotent by construction: the store dedupes on merge, and the ledger only
avoids repeated work. Deleting the ledger and re-running produces the same data.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import config, store
from .capture import read_chunk
from .chunks import CHUNK_RE, ChunkInfo, chunk_info  # noqa: F401 (re-exported)
from .parse import parse_endpoint
from .timeutil import now_utc, parse_compact

@dataclass
class Result:
    ingested: list[str] = field(default_factory=list)
    skipped_recent: int = 0
    changed_files: list[Path] = field(default_factory=list)
    new_drift: list[dict] = field(default_factory=list)
    live_probes: list[dict] = field(default_factory=list)


def parse_chunk(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], list[dict], list[dict]]:
    """Returns (snapshot_rows, trend_rows, drift, fetch_log_rows, probe_rows)."""
    meta, raw, probes = read_chunk(path)
    snaps, trends, drift, fetch_rows, probe_rows = [], [], [], [], []
    for ep, m in (meta.get("endpoints") or {}).items():
        fetch_rows.append({
            "chunk": path.name, "endpoint": ep, "status": m.get("status"), "ok": bool(m.get("ok")),
            "attempts": m.get("attempts"), "bytes": m.get("bytes"), "elapsed_s": m.get("elapsed_s"),
            "fetched_at_utc": m.get("fetched_at_utc"), "error": m.get("error"),
        })
    for ep, m in (meta.get("probes") or {}).items():
        probe_rows.append({"chunk": path.name, "endpoint": ep, "status": m.get("status"),
                           "fetched_at_utc": m.get("fetched_at_utc")})
    for ep, body in raw.items():
        fetched_at = _ts((meta.get("endpoints") or {}).get(ep, {}).get("fetched_at_utc")) or _chunk_time(path)
        rows, trend_rows, events, update_utc = parse_endpoint(ep, body, fetched_at_utc=fetched_at)
        for r in rows:
            r["source_update_utc"] = update_utc
            r["fetched_at_utc"] = fetched_at
            r["chunk"] = path.name
        seen = update_utc or fetched_at
        for r in trend_rows:
            r["first_seen_update_utc"] = seen
            r["last_seen_update_utc"] = seen
        snaps.extend(rows)
        trends.extend(trend_rows)
        drift.extend({**e.as_dict(), "chunk": path.name} for e in events)
    return pd.DataFrame(snaps), pd.DataFrame(trends), drift, fetch_rows, probe_rows


def _ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except ValueError:
        return None


def _chunk_time(path: Path) -> datetime:
    info = chunk_info(path)
    return info.started_utc if info else now_utc()


# --------------------------------------------------------------------------
# Manifests
# --------------------------------------------------------------------------
def _ledger_path(data_dir: Path) -> Path:
    return data_dir / "manifest" / "ingested_chunks.csv"


def read_ledger(data_dir: Path) -> pd.DataFrame:
    p = _ledger_path(data_dir)
    if p.exists():
        return pd.read_csv(p, dtype=str)
    return pd.DataFrame(columns=["chunk", "ingested_at_utc", "snapshot_rows", "trend_rows", "drift_events"])


def _append_csv(path: Path, rows: list[dict], key: list[str] | None = None, sort: list[str] | None = None) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame(rows).astype(str)
    if path.exists():
        old = pd.read_csv(path, dtype=str, keep_default_na=False)
        new = pd.concat([old, new], ignore_index=True)
    if key:
        new = new.drop_duplicates(key, keep="first")
    if sort:
        new = new.sort_values(sort, kind="stable")
    new.to_csv(path, index=False, lineterminator="\n")


def _drift_key(d: dict) -> str:
    return re.sub(r"\d+", "#", d["detail"])


def update_drift_log(data_dir: Path, drift: list[dict]) -> list[dict]:
    """Aggregate drift events; return the kinds never seen before (for alerting)."""
    if not drift:
        return []
    path = data_dir / "manifest" / "drift_log.csv"
    cols = ["endpoint", "path", "kind", "pattern", "severity", "example", "first_seen_chunk", "last_seen_chunk", "n_chunks"]
    log = pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else pd.DataFrame(columns=cols)
    index = {(r.endpoint, r.path, r.kind, r.pattern): i for i, r in log.iterrows()}
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for d in drift:
        grouped[(d["endpoint"], d["path"], d["kind"], _drift_key(d))].append(d)
    new_kinds = []
    for key, events in grouped.items():
        chunks = sorted({e["chunk"] for e in events})
        if key in index:
            i = index[key]
            log.at[i, "last_seen_chunk"] = max(log.at[i, "last_seen_chunk"], chunks[-1])
            log.at[i, "n_chunks"] = str(int(log.at[i, "n_chunks"]) + len(chunks))
        else:
            row = {"endpoint": key[0], "path": key[1], "kind": key[2], "pattern": key[3],
                   "severity": events[0]["severity"], "example": events[0]["detail"],
                   "first_seen_chunk": chunks[0], "last_seen_chunk": chunks[-1], "n_chunks": str(len(chunks))}
            log = pd.concat([log, pd.DataFrame([row])], ignore_index=True)
            new_kinds.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    log[cols].sort_values(["endpoint", "path", "kind", "pattern"]).to_csv(path, index=False, lineterminator="\n")
    return new_kinds


def update_fetch_log(data_dir: Path, rows: list[dict]) -> None:
    """Per-chunk, per-endpoint fetch outcome, monthly parquet (for the
    methodology chapter: success rates, retries, latency)."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["month"] = df["chunk"].str.slice(5, 11)
    for month, part in df.groupby("month"):
        path = data_dir / "manifest" / "fetch_log" / f"fetch_log_{month[:4]}-{month[4:]}.parquet"
        part = part.drop(columns="month")
        if path.exists():
            part = pd.concat([pd.read_parquet(path), part], ignore_index=True)
        part = part.astype({"status": "float64", "attempts": "float64", "bytes": "float64", "elapsed_s": "float64"})
        part = part.drop_duplicates(["chunk", "endpoint"]).sort_values(["chunk", "endpoint"]).reset_index(drop=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        part.to_parquet(path, index=False, compression="zstd")


# --------------------------------------------------------------------------
# Main entry
# --------------------------------------------------------------------------
def consolidate(
    chunk_dir: Path,
    data_dir: Path = config.DATA_DIR,
    *,
    cutoff_utc: datetime | None = None,
    use_ledger: bool = True,
) -> Result:
    """Ingest every chunk in chunk_dir started before cutoff_utc (default: the
    start of the current UTC day, so each daily partition is written once when
    it is complete rather than rewritten through the day)."""
    res = Result()
    if cutoff_utc is None:
        cutoff_utc = now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
    done = set(read_ledger(data_dir)["chunk"]) if use_ledger else set()

    by_day: dict[str, list[ChunkInfo]] = defaultdict(list)
    for p in sorted(chunk_dir.rglob("sitr_*.tar.gz")):
        info = chunk_info(p)
        if info is None or info.name in done:
            continue
        if info.started_utc >= cutoff_utc:
            res.skipped_recent += 1
            continue
        by_day[info.started_utc.strftime("%Y-%m-%d")].append(info)

    for day in sorted(by_day):
        snaps, trends, drift, fetch_rows, probe_rows, ledger_rows = [], [], [], [], [], []
        for info in by_day[day]:
            try:
                s, t, d, f, pr = parse_chunk(info.path)
            except Exception as e:  # noqa: BLE001 - corrupt tarball etc.: log, keep going
                d = [{"endpoint": "*", "path": info.name, "kind": "chunk_unreadable",
                      "detail": f"{type(e).__name__}: {e}", "severity": "ERROR", "chunk": info.name}]
                s, t, f, pr = pd.DataFrame(), pd.DataFrame(), [], []
            if not s.empty:
                snaps.append(s)
            if not t.empty:
                trends.append(store.dedupe_trend(store._normalise(t, store.TREND_SCHEMA)))
            drift += d
            fetch_rows += f
            probe_rows += pr
            ledger_rows.append({"chunk": info.name, "ingested_at_utc": now_utc().isoformat(timespec="seconds"),
                                "snapshot_rows": len(s), "trend_rows": len(t), "drift_events": len(d)})
        snap_root = data_dir / "snapshots"
        trend_root = data_dir / "trend"
        if snaps:
            res.changed_files += store.merge_snapshots(pd.concat(snaps, ignore_index=True), snap_root)
        if trends:
            res.changed_files += store.merge_trend(pd.concat(trends, ignore_index=True), trend_root)
        res.new_drift += update_drift_log(data_dir, drift)
        update_fetch_log(data_dir, fetch_rows)
        _append_csv(data_dir / "manifest" / "endpoint_probes.csv", probe_rows,
                    key=["chunk", "endpoint"], sort=["chunk", "endpoint"])
        res.live_probes += [p for p in probe_rows if str(p["status"]) == "200"]
        # Ledger last: a crash before this line just means re-ingesting (harmless).
        if use_ledger:
            _append_csv(_ledger_path(data_dir), ledger_rows, key=["chunk"], sort=["chunk"])
        res.ingested += [i.name for i in by_day[day]]
        print(f"[consolidate] {day}: {len(by_day[day])} chunks", flush=True)
    return res
