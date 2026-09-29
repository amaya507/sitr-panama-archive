"""HTTP retry policy, chunk naming, and gap detection."""
from __future__ import annotations

import io
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from conftest import make_chunk, raw
from sitr import config, gaps, watchdog
from sitr.capture import chunk_name
from sitr.consolidate import chunk_info, consolidate
from sitr.fetch import FetchResult, fetch


class FakeResp(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body)
        self.status = status
        self.headers = {"Last-Modified": "x"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _opener(seq, seen):
    def opener(req, timeout):
        seen.append(dict(req.header_items()))
        item = seq.pop(0)
        if isinstance(item, Exception):
            raise item
        return item
    return opener


def _http_error(code):
    return urllib.error.HTTPError("u", code, "err", {}, io.BytesIO(b"<html>"))


def test_retries_transient_then_succeeds():
    seen = []
    r = fetch("sin", opener=_opener([_http_error(503), urllib.error.URLError("reset"), FakeResp(b'{"a":1}')], seen),
              sleep=lambda s: None)
    assert r.ok and r.attempts == 3
    assert seen[0]["User-agent"] == config.USER_AGENT and config.CONTACT_EMAIL in config.USER_AGENT


def test_no_retry_on_404_or_403():
    for code in (404, 403):
        r = fetch("carga", opener=_opener([_http_error(code)], []), sleep=lambda s: None)
        assert not r.ok and r.status == code and r.attempts == 1


def test_truncated_json_is_retried():
    r = fetch("sin", opener=_opener([FakeResp(b'{"a":'), FakeResp(b'{"a":1}')], []), sleep=lambda s: None)
    assert r.ok and r.attempts == 2


def test_gives_up_after_max_attempts():
    r = fetch("sin", attempts=3, opener=_opener([_http_error(502)] * 3, []), sleep=lambda s: None)
    assert not r.ok and r.attempts == 3


def test_chunk_name_roundtrip():
    rs = [FetchResult("sin", "u", status=200, body=b"{}"), FetchResult("gen", "u", status=500, error="HTTP 500")]
    name = chunk_name(datetime(2026, 9, 29, 10, 24, 1, tzinfo=timezone.utc), "snap", rs)
    assert name == "sitr_20260929T102401Z_snap_ok1of2-xgen.tar.gz"
    info = chunk_info(Path(name))
    assert info.n_ok == 1 and info.failed == ["gen"] and info.kind == "snap"


def test_gap_report_finds_hole(chunk_dir, data_dir):
    # Two files 24h+ apart would leave a hole; simulate by dropping minutes from one file.
    import json
    o = json.loads(raw("sin_20260929T1024Z"))
    del o["trend"]["series"][600:660]  # remove one hour of points
    make_chunk(chunk_dir, "20260929T102401Z", {"sin": json.dumps(o).encode()})
    consolidate(chunk_dir, data_dir, cutoff_utc=pd.Timestamp.max.tz_localize("UTC"))
    cov = gaps.trend_coverage(data_dir)
    assert cov["expected_minutes"] - cov["held_minutes"] == 60
    assert len(cov["missing"]) == 1 and cov["missing"]["minutes"].iloc[0] == 60
    text = gaps.report(data_dir)
    assert "COMPLETENESS: 95.8" in text and "(60 min)" in text


def test_watchdog_flags_stale(monkeypatch):
    raised = []
    monkeypatch.setattr("sitr.github.raise_alert", lambda key, *a, **k: raised.append(key))
    monkeypatch.setattr("sitr.github.resolve_alert", lambda *a, **k: None)
    now = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
    old = [chunk_info(Path("sitr_20260929T090000Z_snap_ok6of6.tar.gz"))]
    watchdog.check(now=now, chunks=old)
    assert "snapshot-stale" in raised and "sin-stale" not in raised
    raised.clear()
    fresh = [chunk_info(Path(f"sitr_20260929T11{m:02d}00Z_snap_ok5of6-xsin.tar.gz")) for m in range(0, 60, 5)]
    watchdog.check(now=now, chunks=fresh)
    assert "endpoint-failing-sin" in raised and "snapshot-stale" not in raised
