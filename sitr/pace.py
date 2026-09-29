"""Pacing for the self-dispatching snapshot chain.

GitHub's cron scheduler proved unreliable for a 5-minute cadence (2 of ~120
expected runs fired on 2026-09-29), so each snapshot run dispatches the next.
This module enforces the minimum spacing between fetches IN CODE, so no
misconfiguration (missing environment wait timer, duplicate chains, a burst of
cron triggers) can make us poll CND faster than the configured interval.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, github
from .consolidate import chunk_info


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


def pace(min_interval_s: float, max_wait_s: float = 360, sleep=time.sleep) -> float:
    """Sleep until at least min_interval_s after the last snapshot. Returns seconds slept."""
    try:
        last = last_snapshot_time()
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
