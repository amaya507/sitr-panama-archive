"""Fixtures are real SITR responses captured 2026-09-29 10:24-10:28 UTC."""
from __future__ import annotations

import gzip
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sitr.capture import _add

FIX = Path(__file__).parent / "fixtures"


def raw(name: str) -> bytes:
    return gzip.decompress((FIX / f"{name}.json.gz").read_bytes())


def obj(name: str) -> dict:
    return json.loads(raw(name).decode("utf-8"))


ALL_1024 = {ep: f"{ep}_20260929T1024Z" for ep in ["sin", "gen", "vert", "int", "diagram", "flow"]}


def make_chunk(dir_: Path, ts: str, bodies: dict[str, bytes], kind: str = "snap") -> Path:
    """Build a chunk exactly like capture.capture() does, from given bodies."""
    start = datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    eps = {n: {"url": n, "status": 200, "ok": True, "error": None, "attempts": 1,
               "fetched_at_utc": start.isoformat(), "elapsed_s": 0.1, "bytes": len(b),
               "headers": {}, "body_snippet": None} for n, b in bodies.items()}
    meta = {"chunk_format": 1, "kind": kind, "started_utc": start.isoformat(), "endpoints": eps, "probes": {}}
    path = dir_ / f"sitr_{ts}_{kind}_ok{len(bodies)}of{len(bodies)}.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        _add(tar, "meta.json", json.dumps(meta).encode())
        for n, b in bodies.items():
            _add(tar, f"raw/{n}.json", b)
    return path


@pytest.fixture
def chunk_dir(tmp_path):
    d = tmp_path / "chunks"
    d.mkdir()
    return d


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "data"
