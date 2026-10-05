"""Pacing for the self-dispatching snapshot chain.

GitHub's cron scheduler proved unreliable for a 5-minute cadence (2 of ~120
expected runs fired on 2026-09-29), so each snapshot run dispatches the next.
This module enforces the minimum spacing between fetches IN CODE, so no
misconfiguration (missing environment wait timer, duplicate chains, a burst of
cron triggers) can make us poll CND faster than the configured interval.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, github
from .chunks import chunk_info


def last_snapshot_time(now: datetime | None = None) -> datetime | None:
    now = now or datetime.now(timezone.utc)
    latest = None
    for day in (now, now - timedelta(days=1)):
        tag = f"{config.RELEASE_PREFIX}{day:%Y-%m-%d}"
        try:
            rel = github.request("GET", f"/repos/{github.repo()}/releases/tags/{tag}")
        except Exception:  # noqa: BLE001 - 404 when the day's release doesn't exist yet
            continue
        for a in github.release_assets(rel):
            info = chunk_info(Path(a["name"]))
            if info and info.kind == "snap" and (latest is None or info.started_utc > latest):
                latest = info.started_utc
        if latest:
            return latest
    return latest


def previous_run_start(workflow: str = "snapshot.yml") -> datetime | None:
    """When the most recent earlier run of this workflow finished (success or not).
    Pacing on runs as well as chunks means a run that failed before uploading
    still counts, so failures can never turn the chain into a tight loop."""
    me = int(os.environ.get("GITHUB_RUN_ID") or 0) or None
    runs = github.request("GET", f"/repos/{github.repo()}/actions/workflows/{workflow}/runs?per_page=10")
    return previous_run_end(runs.get("workflow_runs", []), me)


def previous_run_end(runs: list[dict], me: int | None) -> datetime | None:
    """End time of the latest EARLIER run that has finished. Only earlier runs
    (lower id) count: the successor this run just queued must not (on
    2026-10-01..05 it did, doubling every run's duration). The end time, not
    the queue time, is used: a run can sit queued for minutes before fetching."""
    ends = []
    for r in runs:
        if me is not None and int(r.get("id", 0)) >= me:
            continue
        if r.get("status") != "completed" or not r.get("updated_at"):
            continue
        ends.append(datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")))
    return max(ends) if ends else None


def last_activity() -> datetime | None:
    times = [t for t in (last_snapshot_time(), previous_run_start()) if t is not None]
    return max(times) if times else None


def pace(min_interval_s: float, max_wait_s: float = 360, sleep=time.sleep) -> float:
    """Sleep until at least min_interval_s after the last snapshot chunk or the
    previous run's start, whichever is later. Returns seconds slept."""
    try:
        last = last_activity()
    except Exception as e:  # noqa: BLE001 - if GitHub is unreachable, be conservative
        print(f"[pace] could not read last chunk ({e}); waiting the full interval", flush=True)
        sleep(min_interval_s)
        return min_interval_s
    if last is None:
        print("[pace] no previous snapshot found; not waiting", flush=True)
        return 0.0
    wait = (last + timedelta(seconds=min_interval_s) - datetime.now(timezone.utc)).total_seconds()
    wait = max(0.0, min(wait, max_wait_s))
    print(f"[pace] last snapshot {last:%H:%M:%S}Z; waiting {wait:.0f}s", flush=True)
    if wait:
        sleep(wait)
    return wait
