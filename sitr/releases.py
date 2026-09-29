"""Raw chunk storage in GitHub Releases: one release per UTC day (raw-YYYY-MM-DD),
one asset per fetch. Uploads happen in the workflow (gh CLI); this module
lists and downloads."""
from __future__ import annotations

import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, github
from .consolidate import read_ledger


def release_day(tag: str) -> datetime | None:
    if not tag.startswith(config.RELEASE_PREFIX):
        return None
    try:
        return datetime.strptime(tag[len(config.RELEASE_PREFIX):], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def raw_releases(since: datetime | None = None, until: datetime | None = None) -> list[dict]:
    out = []
    for rel in github.list_releases(max_pages=10):
        day = release_day(rel.get("tag_name", ""))
        if day is None:
            continue
        if since and day < since:
            continue
        if until and day >= until:
            continue
        out.append(rel)
    return sorted(out, key=lambda r: r["tag_name"])


def download_asset(asset: dict, dest: Path, attempts: int = 4) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / asset["name"]
    if target.exists() and target.stat().st_size == asset.get("size", -1):
        return target
    headers = {"Accept": "application/octet-stream", "User-Agent": "sitr-panama-archive"}
    if github.token():
        headers["Authorization"] = f"Bearer {github.token()}"
    for i in range(1, attempts + 1):
        try:
            req = urllib.request.Request(asset["url"], headers=headers)
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            if asset.get("size") and len(data) != asset["size"]:
                raise IOError(f"size mismatch {len(data)} != {asset['size']}")
            tmp = target.with_suffix(".part")
            tmp.write_bytes(data)
            tmp.replace(target)
            return target
        except Exception:  # noqa: BLE001
            if i == attempts:
                raise
            time.sleep(2 ** i)
    return target


def download_recent(dest: Path, *, days: int = 14, data_dir: Path = config.DATA_DIR) -> list[Path]:
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
    done = set(read_ledger(data_dir)["chunk"])
    got = []
    for rel in raw_releases(since=since):
        for asset in github.release_assets(rel):
            if asset["name"].startswith("sitr_") and asset["name"] not in done:
                got.append(download_asset(asset, dest))
    print(f"[download] {len(got)} chunks to ingest", flush=True)
    return got


def download_month(month: str, dest: Path) -> list[Path]:
    """All chunks for a calendar month (UTC), e.g. '2026-09'."""
    start = datetime.strptime(month + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = (start + timedelta(days=32)).replace(day=1)
    got = []
    for rel in raw_releases(since=start, until=end):
        for asset in github.release_assets(rel):
            if asset["name"].startswith("sitr_"):
                got.append(download_asset(asset, dest))
    return got
