"""Command line: python -m sitr <command>. See README for the full picture."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import config


def cmd_snapshot(a) -> int:
    from . import github
    from .capture import capture

    path, meta = capture("snap", config.LIVE_ENDPOINTS, Path(a.out))
    eps = meta["endpoints"]
    n_ok = sum(1 for m in eps.values() if m["ok"])
    blocked = [n for n, m in eps.items() if m["status"] in (401, 403) or (m["status"] == 503 and "cloudflare" in str(m["headers"]).lower())]
    if blocked:
        github.raise_alert(
            "blocked", f"Source refused requests ({', '.join(blocked)})",
            "SITR answered 401/403 (or a Cloudflare challenge). This pipeline must NOT try to evade it.\n"
            "Options: wait and see if it is transient; contact CND (see README 'If CND asks us to stop'); "
            "or move capture to a VPS if only GitHub's IP ranges are affected.\n\n"
            + "\n".join(f"- {n}: status {eps[n]['status']}, snippet: {eps[n]['body_snippet']!r}" for n in blocked),
        )
    if n_ok == 0:
        github.raise_alert(
            "fetch-failing", "All SITR endpoints failed in a snapshot run",
            "Every endpoint failed after retries.\n\n" + "\n".join(
                f"- {n}: status={m['status']} error={m['error']}" for n, m in eps.items()),
        )
    # The chunk (even if empty of data) is uploaded regardless: it records the failure.
    return 0


def cmd_trend_capture(a) -> int:
    from . import github
    from .capture import capture

    path, meta = capture("trend", ["sin"], Path(a.out), probe=[] if a.no_probe else config.DEAD_ENDPOINTS)
    if not meta["endpoints"]["sin"]["ok"]:
        github.raise_alert("trend-capture-failing", "Twice-daily sin.json backstop fetch failed",
                           f"status={meta['endpoints']['sin']['status']} error={meta['endpoints']['sin']['error']}")
    return 0


def cmd_download(a) -> int:
    from .releases import download_recent
    download_recent(Path(a.dest), days=a.days, data_dir=Path(a.data))
    return 0


def cmd_consolidate(a) -> int:
    from .consolidate import consolidate
    cutoff = datetime.max.replace(tzinfo=timezone.utc) if a.include_today else None
    res = consolidate(Path(a.chunks), Path(a.data), cutoff_utc=cutoff, use_ledger=not a.no_ledger)
    print(f"ingested {len(res.ingested)} chunks, {res.skipped_recent} left for tomorrow, "
          f"{len(res.changed_files)} files changed, {len(res.new_drift)} new drift kinds")
    return 0


def cmd_daily(a) -> int:
    """Orchestration for the twice-daily job, after chunks have been downloaded."""
    import pandas as pd

    from . import gaps, github, watchdog
    from .consolidate import consolidate

    data = Path(a.data)
    old_missing = _read_missing(data)
    res = consolidate(Path(a.chunks), data)
    print(f"[daily] ingested {len(res.ingested)} chunks; {len(res.changed_files)} files changed")

    if res.new_drift:
        errs = [d for d in res.new_drift if d["severity"] == "ERROR"]
        body = "New schema drift detected. Raw files are preserved; affected fields may be missing from parsed data " \
               "until the parser is updated, then history can be reparsed (README 'Reparsing').\n\n" + \
               "\n".join(f"- **{d['severity']}** `{d['endpoint']}` `{d['path']}` {d['kind']}: {d['example']} "
                         f"(first in {d['first_seen_chunk']})" for d in res.new_drift)
        key = "drift-" + datetime.now(timezone.utc).strftime("%Y%m%d")
        github.raise_alert(key, f"Schema drift: {len(res.new_drift)} new kind(s), {len(errs)} error(s)", body)
    if res.live_probes:
        github.raise_alert("dead-endpoint-alive", "A previously dead SITR endpoint now returns 200",
                           "\n".join(f"- {p['endpoint']}.json (chunk {p['chunk']})" for p in res.live_probes) +
                           "\n\nThe body is saved in the chunk under probe/. Consider adding it to LIVE_ENDPOINTS.")

    cov = gaps.write_manifests(data)
    new_missing = _read_missing(data)
    fresh = sorted(set(new_missing) - set(old_missing))
    # Ignore the open-ended tail: the last minutes before the newest ingested fetch.
    if fresh:
        github.raise_alert(
            "trend-gap", f"Trend series gap(s) detected ({len(fresh)} new interval(s))",
            "Minutes of the 1-minute trend with no sample for at least one series:\n\n" +
            "\n".join(f"- {s} -> {e} local" for s, e in fresh[:50]) +
            "\n\nThese minutes are older than the source's 24 h window and cannot be recovered. "
            "Close this issue once noted in the methodology log.", repeat_after_h=1e9)
    print(gaps.report(data))
    if not a.skip_watchdog:
        watchdog.check()
    return 0


def _read_missing(data: Path) -> list[tuple[str, str]]:
    import pandas as pd
    p = data / "manifest" / "trend_missing_intervals.csv"
    if not p.exists() or p.stat().st_size == 0:
        return []
    try:
        df = pd.read_csv(p, dtype=str)
    except Exception:  # noqa: BLE001
        return []
    return list(zip(df.get("start_local", []), df.get("end_local", [])))


def cmd_gaps(a) -> int:
    from . import gaps
    print(gaps.report(Path(a.data), a.since, a.until))
    return 0


def cmd_verify(a) -> int:
    from .verify import verify
    ok, text = verify(Path(a.data))
    print(text)
    return 0 if ok else 1


def cmd_watchdog(a) -> int:
    from . import watchdog
    problems = watchdog.check()
    print("\n".join(problems) or "watchdog: all fresh")
    return 0


def cmd_export(a) -> int:
    from .export import export_all
    export_all(Path(a.data), Path(a.out))
    return 0


def cmd_mirror(a) -> int:
    from .mirror import mirror
    return mirror(Path(a.data), Path(a.work), remote=a.remote, dry_run=a.dry_run)


def cmd_alert(a) -> int:
    from . import github
    github.raise_alert(a.key, a.title, a.body)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m sitr")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("snapshot", help="fetch all live endpoints once, write a chunk")
    s.add_argument("--out", default="chunks")
    s.set_defaults(func=cmd_snapshot)

    s = sub.add_parser("trend-capture", help="fetch sin.json (+probe dead endpoints), write a chunk")
    s.add_argument("--out", default="chunks")
    s.add_argument("--no-probe", action="store_true")
    s.set_defaults(func=cmd_trend_capture)

    s = sub.add_parser("download", help="download not-yet-ingested chunks from recent releases")
    s.add_argument("--dest", default="chunks")
    s.add_argument("--days", type=int, default=14)
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.set_defaults(func=cmd_download)

    s = sub.add_parser("consolidate", help="ingest chunks into data/")
    s.add_argument("--chunks", default="chunks")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.add_argument("--include-today", action="store_true", help="also ingest chunks from the current UTC day")
    s.add_argument("--no-ledger", action="store_true", help="ignore/skip the ingest ledger (for reparsing)")
    s.set_defaults(func=cmd_consolidate)

    s = sub.add_parser("daily", help="consolidate + manifests + alerts + watchdog")
    s.add_argument("--chunks", default="chunks")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.add_argument("--skip-watchdog", action="store_true")
    s.set_defaults(func=cmd_daily)

    s = sub.add_parser("gaps", help="print the completeness report")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.add_argument("--since", help="Panama-local start, e.g. '2026-10-01 00:00'")
    s.add_argument("--until", help="Panama-local end")
    s.set_defaults(func=cmd_gaps)

    s = sub.add_parser("verify", help="compare the store against a fresh sin.json fetch")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.set_defaults(func=cmd_verify)

    s = sub.add_parser("watchdog", help="check chunk freshness via release listings")
    s.set_defaults(func=cmd_watchdog)

    s = sub.add_parser("export", help="build consolidated exports")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.add_argument("--out", default="export")
    s.set_defaults(func=cmd_export)

    s = sub.add_parser("mirror", help="export + monthly raw bundles -> Google Drive via rclone")
    s.add_argument("--data", default=str(config.DATA_DIR))
    s.add_argument("--work", default="mirror_work")
    s.add_argument("--remote", default="sitr:")
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(func=cmd_mirror)

    s = sub.add_parser("alert", help="file/refresh an alert issue")
    s.add_argument("--key", required=True)
    s.add_argument("--title", required=True)
    s.add_argument("--body", default="")
    s.set_defaults(func=cmd_alert)

    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
