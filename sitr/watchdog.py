"""Freshness checks that need no downloads: chunk names encode fetch time and
which endpoints failed, so listing release assets is enough."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, github
from .chunks import chunk_info

SNAPSHOT_STALE = timedelta(minutes=90)
SIN_STALE = timedelta(hours=6)          # trend window is 24 h; alert with 18 h to spare
FAILURE_RATE_ALERT = 0.2                # per endpoint, over the last 12 h
CADENCE_ALERT_FRACTION = 0.5            # alert below half the expected snapshots in 12 h


def recent_chunks(days: int = 2) -> list:
    names = []
    for rel in github.list_releases(max_pages=1):
        tag = rel.get("tag_name", "")
        if not tag.startswith(config.RELEASE_PREFIX):
            continue
        try:
            day = datetime.strptime(tag[len(config.RELEASE_PREFIX):], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if datetime.now(timezone.utc) - day > timedelta(days=days + 1):
            continue
        names += [a["name"] for a in github.release_assets(rel)]
    infos = [chunk_info(Path(n)) for n in names]
    return sorted([i for i in infos if i], key=lambda i: i.started_utc)


def check(now: datetime | None = None, chunks: list | None = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    chunks = recent_chunks() if chunks is None else chunks
    problems: list[str] = []

    ok_any = [c for c in chunks if c.n_ok > 0]
    last_any = ok_any[-1].started_utc if ok_any else None
    if last_any is None or now - last_any > SNAPSHOT_STALE:
        msg = f"No successful fetch since {last_any or 'ever (in the last 2 days)'} (now {now:%Y-%m-%d %H:%M}Z)."
        problems.append(msg)
        github.raise_alert("snapshot-stale", "Snapshot capture has stopped", msg +
                           "\n\nCheck the `snapshot` workflow: is it disabled, failing, or is the site down/blocking?")
    else:
        github.resolve_alert("snapshot-stale", f"fetches resumed; last at {last_any:%Y-%m-%d %H:%M}Z")

    sin_ok = [c for c in chunks if c.n_ok > 0 and "sin" not in c.failed]
    last_sin = sin_ok[-1].started_utc if sin_ok else None
    if last_sin is None or now - last_sin > SIN_STALE:
        msg = (f"No successful sin.json fetch since {last_sin or 'ever (2 days)'}. The trend window is 24 h: "
               f"data older than 24 h is lost permanently if this continues.")
        problems.append(msg)
        github.raise_alert("sin-stale", "sin.json not captured — trend data at risk", msg)
    else:
        github.resolve_alert("sin-stale", f"sin.json captured at {last_sin:%Y-%m-%d %H:%M}Z")

    window = [c for c in chunks if now - c.started_utc <= timedelta(hours=12) and c.kind == "snap"]
    expected = 12 * 60 // 5
    if len(window) < CADENCE_ALERT_FRACTION * expected:
        msg = (f"Only {len(window)} snapshot fetches in the last 12 h (expected ~{expected} at 5-min cadence). "
               "Snapshot streams have no history at the source, so this resolution is lost for good.\n\n"
               "Check the `snapshot` workflow: is the self-dispatch chain running (a new run every ~5 min in the "
               "Actions tab)? If not, start it with Actions -> snapshot -> Run workflow.")
        problems.append(msg)
        github.raise_alert("snapshot-cadence", f"Snapshot cadence degraded ({len(window)}/{expected} in 12 h)", msg)
    else:
        github.resolve_alert("snapshot-cadence", f"{len(window)}/{expected} snapshots in the last 12 h")
    if window:
        for ep in config.LIVE_ENDPOINTS:
            rate = sum(ep in c.failed for c in window) / len(window)
            key = f"endpoint-failing-{ep}"
            if rate > FAILURE_RATE_ALERT:
                msg = f"{ep}.json failed in {rate:.0%} of {len(window)} fetches over the last 12 h."
                problems.append(msg)
                github.raise_alert(key, f"{ep}.json failing ({rate:.0%})", msg)
            else:
                github.resolve_alert(key, f"failure rate back to {rate:.0%}")
    _resolve_workflow_alert_if_healthy()
    return problems


def _resolve_workflow_alert_if_healthy(workflow: str = "snapshot.yml", n: int = 20) -> None:
    """Close the snapshot-workflow alert once the last n finished runs all succeeded."""
    if not github.token():
        return
    try:
        runs = github.request("GET", f"/repos/{github.repo()}/actions/workflows/{workflow}/runs?status=completed&per_page={n}")
        concl = [r.get("conclusion") for r in runs.get("workflow_runs", [])]
    except Exception:  # noqa: BLE001
        return
    finished = [c for c in concl if c != "cancelled"]  # cancelled = collapsed duplicate triggers
    if len(finished) >= 10 and all(c == "success" for c in finished):
        github.resolve_alert("snapshot-workflow", f"the last {len(finished)} snapshot runs succeeded")
