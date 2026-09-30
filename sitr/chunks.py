"""Chunk file names: sitr_<startUTC>_<kind>_ok<k>of<n>[-x<failed>...].tar.gz

Dependency-free (stdlib only) so the 5-minute snapshot job, which installs
nothing, can use it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .timeutil import parse_compact

CHUNK_RE = re.compile(r"^sitr_(\d{8}T\d{6}Z)_([a-z]+)_ok(\d+)of(\d+)((?:-x[A-Za-z0-9]+)*)\.tar\.gz$")


@dataclass
class ChunkInfo:
    path: Path
    name: str
    started_utc: datetime
    kind: str
    n_ok: int
    n: int
    failed: list[str]


def chunk_info(path: Path) -> ChunkInfo | None:
    m = CHUNK_RE.match(path.name)
    if not m:
        return None
    ts, kind, ok, n, fails = m.groups()
    failed = [f for f in fails.split("-x") if f] if fails else []
    return ChunkInfo(path, path.name, parse_compact(ts), kind, int(ok), int(n), failed)
