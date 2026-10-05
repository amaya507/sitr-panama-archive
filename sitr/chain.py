"""Keep the snapshot chain alive: queue exactly one successor run.

Stdlib only (the snapshot job installs nothing).

History (why this is Python, not inline shell):
* 2026-10-01..05: the shell version counted pending runs without excluding the
  run doing the counting. Right after a run starts, GitHub's run list can still
  report it as "pending", so it concluded a successor already existed, queued
  nothing, and the chain died until a (very late) cron restart: 2-6 h gaps.
* A failed listing made the shell compare "" to "0" and also queue nothing.
Now: own run excluded, and if the listing fails we dispatch anyway. Duplicates
are harmless: the concurrency group keeps one pending run, and pace.py keeps
>= 285 s between fetches no matter how many runs exist.
"""
from __future__ import annotations

import os
import time

from . import github

NOT_STARTED = {"queued", "waiting", "pending", "requested"}


def pending_successors(runs: list[dict], me: int | None) -> int:
    return sum(1 for r in runs if r.get("id") != me and r.get("status") in NOT_STARTED)


def queue_next(workflow: str = "snapshot.yml", ref: str = "main", sleep=time.sleep) -> str:
    me = int(os.environ["GITHUB_RUN_ID"]) if os.environ.get("GITHUB_RUN_ID") else None
    try:
        runs = github.request("GET", f"/repos/{github.repo()}/actions/workflows/{workflow}/runs?per_page=20")
        n = pending_successors(runs.get("workflow_runs", []), me)
        if n:
            return f"already queued ({n} pending run(s) other than this one); not dispatching"
    except Exception as e:  # noqa: BLE001 - can't tell: dispatch anyway (safe, see docstring)
        print(f"[chain] could not list runs ({e}); dispatching anyway", flush=True)
    last = None
    for attempt in range(1, 6):
        try:
            github.request("POST", f"/repos/{github.repo()}/actions/workflows/{workflow}/dispatches", {"ref": ref})
            return "dispatched next run"
        except Exception as e:  # noqa: BLE001
            last = e
            sleep(5 * attempt)
    raise RuntimeError(f"could not dispatch next run after 5 attempts: {last}")
