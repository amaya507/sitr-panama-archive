"""Fetch endpoints and write one immutable "chunk": a .tar.gz holding the raw
response bodies byte-for-byte plus a meta.json describing the fetch.

Chunk name: sitr_<startUTC>_<kind>_ok<k>of<n>[-x<failed>...].tar.gz
The name alone tells the watchdog when the fetch ran and what failed, without
downloading anything.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
import time
from pathlib import Path

from . import config
from .fetch import FetchResult, fetch
from .timeutil import fmt_compact, now_utc

CHUNK_FORMAT_VERSION = 1


def code_version() -> str:
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=config.REPO_ROOT, check=True
        ).stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def chunk_name(started, kind: str, results: list[FetchResult]) -> str:
    ok = [r for r in results if r.ok]
    failed = [r.name for r in results if not r.ok]
    suffix = "".join(f"-x{n}" for n in failed)
    return f"sitr_{fmt_compact(started)}_{kind}_ok{len(ok)}of{len(results)}{suffix}.tar.gz"


def capture(
    kind: str,
    endpoints: list[str],
    out_dir: Path,
    *,
    probe: list[str] | None = None,
    fetcher=fetch,
    sleep=time.sleep,
) -> tuple[Path, dict]:
    out_dir.mkdir(parents=True, exist_ok=True)
    started = now_utc()
    results: list[FetchResult] = []
    for i, name in enumerate(endpoints):
        if i:
            sleep(config.PAUSE_BETWEEN_REQUESTS_S)
        r = fetcher(name)
        results.append(r)
        print(f"[fetch] {name}: status={r.status} ok={r.ok} bytes={len(r.body or b'')} "
              f"attempts={r.attempts} {r.error or ''}", flush=True)

    probes: list[FetchResult] = []
    for name in probe or []:
        sleep(config.PAUSE_BETWEEN_REQUESTS_S)
        r = fetcher(name, attempts=1)
        probes.append(r)
        print(f"[probe] {name}: status={r.status}", flush=True)

    meta = {
        "chunk_format": CHUNK_FORMAT_VERSION,
        "kind": kind,
        "started_utc": started.isoformat(),
        "finished_utc": now_utc().isoformat(),
        "user_agent": config.USER_AGENT,
        "code_version": code_version(),
        "runner": os.environ.get("GITHUB_RUN_ID", "local"),
        "endpoints": {r.name: r.meta() for r in results},
        "probes": {r.name: r.meta() for r in probes},
    }
    name = chunk_name(started, kind, results)
    path = out_dir / name
    with tarfile.open(path, "w:gz", compresslevel=9) as tar:
        _add(tar, "meta.json", json.dumps(meta, indent=2, ensure_ascii=False).encode("utf-8"))
        for r in results:
            if r.ok:
                _add(tar, f"raw/{r.name}.json", r.body)
        for r in probes:
            if r.status == 200 and r.body is not None:
                _add(tar, f"probe/{r.name}.json", r.body)
    print(f"[chunk] {path.name} ({path.stat().st_size} bytes)", flush=True)
    return path, meta


def _add(tar: tarfile.TarFile, arcname: str, data: bytes) -> None:
    info = tarfile.TarInfo(arcname)
    info.size = len(data)
    info.mtime = int(time.time())
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(data))


def read_chunk(path: Path) -> tuple[dict, dict[str, bytes], dict[str, bytes]]:
    """Return (meta, raw bodies by endpoint, probe bodies by endpoint)."""
    raw: dict[str, bytes] = {}
    probes: dict[str, bytes] = {}
    meta: dict = {}
    with tarfile.open(path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            data = tar.extractfile(member).read()
            if member.name == "meta.json":
                meta = json.loads(data.decode("utf-8"))
            elif member.name.startswith("raw/") and member.name.endswith(".json"):
                raw[member.name[4:-5]] = data
            elif member.name.startswith("probe/") and member.name.endswith(".json"):
                probes[member.name[6:-5]] = data
    return meta, raw, probes
