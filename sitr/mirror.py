"""Weekly mirror to Google Drive via rclone.

The rclone remote (default "sitr:") is configured entirely from environment
variables set by the workflow from GitHub Secrets (see README "Google Drive").
Its root is the SITR/ folder, so every path below is relative to SITR/.

Failure here never touches capture: this runs in its own workflow, and the
primary copies (git + GitHub Releases) are never deleted by this code.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import config, github
from .export import export_all
from .releases import download_month, raw_releases, release_day


def rclone(*args: str, dry_run: bool = False) -> None:
    cmd = ["rclone", *args, "--retries", "5", "--low-level-retries", "10", "--stats-one-line", "-v"]
    if dry_run:
        cmd.append("--dry-run")
    print("[rclone]", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def bundle_month(month: str, work: Path) -> tuple[Path, int]:
    """Download every chunk of a month and tar them (uncompressed: members are
    already gzipped). Returns (tar path, number of chunks)."""
    chunk_dir = work / "chunks" / month
    chunks = download_month(month, chunk_dir)
    out_dir = work / "raw_archive" / month
    out_dir.mkdir(parents=True, exist_ok=True)
    tar_path = out_dir / f"sitr-raw-{month}.tar"
    with tarfile.open(tar_path, "w") as tar:
        for c in sorted(chunks):
            tar.add(c, arcname=f"sitr-raw-{month}/{c.name}")
    lines = [f"{sha256(c)}  sitr-raw-{month}/{c.name}" for c in sorted(chunks)]
    (out_dir / f"sitr-raw-{month}.chunks.sha256").write_text("\n".join(lines) + "\n")
    (out_dir / f"sitr-raw-{month}.tar.sha256").write_text(f"{sha256(tar_path)}  {tar_path.name}\n")
    shutil.rmtree(chunk_dir, ignore_errors=True)
    return tar_path, len(chunks)


def mirror(data_dir: Path, work: Path, *, remote: str = "sitr:", dry_run: bool = False) -> int:
    manifest = data_dir / "manifest" / "drive_raw_archive.csv"
    errors: list[str] = []
    try:
        export_dir = work / "export"
        shutil.rmtree(export_dir, ignore_errors=True)
        export_all(data_dir, export_dir)
        rclone("copy", str(export_dir), f"{remote}exports", "--checksum", dry_run=dry_run)
        if not dry_run:
            rclone("check", str(export_dir), f"{remote}exports", "--one-way", dry_run=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"exports: {type(e).__name__}: {e}")

    try:
        done = set(pd.read_csv(manifest, dtype=str)["month"]) if manifest.exists() else set()
        this_month = datetime.now(timezone.utc).strftime("%Y-%m")
        months = sorted({release_day(r["tag_name"]).strftime("%Y-%m") for r in raw_releases()})
        rows = []
        for month in months:
            if month >= this_month or month in done:
                continue
            tar_path, n = bundle_month(month, work)
            rclone("copy", str(tar_path.parent), f"{remote}raw_archive/{month}", "--checksum", dry_run=dry_run)
            if not dry_run:
                rclone("check", str(tar_path.parent), f"{remote}raw_archive/{month}", "--one-way", dry_run=False)
                rows.append({"month": month, "file": tar_path.name, "chunks": n, "bytes": tar_path.stat().st_size,
                             "sha256": sha256(tar_path), "verified_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")})
            shutil.rmtree(tar_path.parent, ignore_errors=True)
        if rows:
            old = pd.read_csv(manifest, dtype=str) if manifest.exists() else pd.DataFrame()
            manifest.parent.mkdir(parents=True, exist_ok=True)
            pd.concat([old, pd.DataFrame(rows).astype(str)]).to_csv(manifest, index=False, lineterminator="\n")
    except Exception as e:  # noqa: BLE001
        errors.append(f"raw archive: {type(e).__name__}: {e}")

    if errors:
        github.raise_alert("drive-mirror", "Weekly Google Drive mirror failed",
                           "Capture is unaffected (git + GitHub Releases are the record). Details:\n\n" +
                           "\n".join(f"- {e}" for e in errors) + "\n\nSee README 'Google Drive' for credential setup.")
        return 1
    github.resolve_alert("drive-mirror", "mirror succeeded")
    return 0
