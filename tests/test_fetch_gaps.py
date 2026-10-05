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


def test_pace_enforces_minimum_spacing(monkeypatch):
    from datetime import timedelta
    from sitr import pace
    now = datetime.now(timezone.utc)
    slept = []
    monkeypatch.setattr(pace, "last_activity", lambda: now - timedelta(seconds=100))
    pace.pace(285, sleep=slept.append)
    assert 180 <= slept[0] <= 186                      # waits out the remaining ~185 s
    slept.clear()
    monkeypatch.setattr(pace, "last_activity", lambda: now - timedelta(seconds=600))
    assert pace.pace(285, sleep=slept.append) == 0 and slept == []
    def boom():
        raise OSError("api down")
    monkeypatch.setattr(pace, "last_activity", boom)
    pace.pace(285, sleep=slept.append)
    assert slept == [285]                              # unknown -> conservative full wait


def test_watchdog_flags_low_cadence(monkeypatch):
    raised = []
    monkeypatch.setattr("sitr.github.raise_alert", lambda key, *a, **k: raised.append(key))
    monkeypatch.setattr("sitr.github.resolve_alert", lambda *a, **k: None)
    now = datetime(2026, 9, 29, 22, 0, tzinfo=timezone.utc)
    # what actually happened on 2026-09-29: two scheduled runs in 10 hours
    few = [chunk_info(Path(n)) for n in ("sitr_20260929T170023Z_snap_ok6of6.tar.gz",
                                          "sitr_20260929T212250Z_snap_ok6of6.tar.gz")]
    watchdog.check(now=now, chunks=few)
    assert "snapshot-cadence" in raised and "snapshot-stale" not in raised
    raised.clear()
    full = [chunk_info(Path(f"sitr_20260929T{h:02d}{m:02d}00Z_snap_ok6of6.tar.gz"))
            for h in range(10, 22) for m in range(0, 60, 5)]
    watchdog.check(now=now, chunks=full)
    assert raised == []


def test_snapshot_job_modules_import_without_pandas():
    """The 5-minute job installs nothing: everything it runs must be stdlib-only.
    (A pandas import in pace.py caused a runaway failure loop on 2026-09-30.)"""
    import subprocess
    import sys
    code = ("import sys; sys.modules['pandas'] = None; sys.modules['pyarrow'] = None; sys.modules['numpy'] = None\n"
            "import sitr.__main__, sitr.chain, sitr.pace, sitr.capture, sitr.fetch, sitr.github, sitr.chunks, sitr.watchdog\n"
            "from sitr.__main__ import main\n"
            "main(['pace', '--help'])")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       cwd=Path(__file__).resolve().parent.parent)
    assert "ModuleNotFoundError" not in r.stderr and "ImportError" not in r.stderr, r.stderr


def test_hung_request_hits_hard_deadline():
    import time as _t
    def hang(req, timeout):
        _t.sleep(5)  # e.g. a stalled DNS lookup that urllib's timeout does not cover
    t0 = _t.monotonic()
    r = fetch("sin", attempts=2, deadline=0.2, opener=hang, sleep=lambda s: None)
    assert not r.ok and "deadline exceeded" in r.error and r.attempts == 2
    assert _t.monotonic() - t0 < 2


def test_capture_budget_skips_rather_than_hangs(tmp_path, monkeypatch):
    from sitr import capture
    monkeypatch.setattr(capture, "BUDGET_RETRY_S", -1)   # already past both budgets
    monkeypatch.setattr(capture, "BUDGET_TOTAL_S", -1)
    calls = []
    def fake(name, **kw):
        calls.append(name)
        return FetchResult(name, "u", status=200, body=b"{}")
    path, meta = capture.capture("snap", ["sin", "gen"], tmp_path, fetcher=fake, sleep=lambda s: None)
    assert calls == []                                    # nothing fetched once over budget
    assert all("budget" in m["error"] for m in meta["endpoints"].values())
    assert path.name.endswith("_snap_ok0of2-xsin-xgen.tar.gz")


def test_chain_ignores_own_pending_run_and_dispatches(monkeypatch):
    """Regression for the 2026-10-01..05 chain deaths."""
    from sitr import chain
    calls = []
    runs = {"workflow_runs": [
        {"id": 500, "status": "pending"},       # this run, still listed as pending
        {"id": 499, "status": "completed"},
    ]}
    def req(method, path, data=None, **kw):
        calls.append(method)
        return runs if method == "GET" else None
    monkeypatch.setenv("GITHUB_RUN_ID", "500")
    monkeypatch.setattr(chain.github, "request", req)
    assert chain.queue_next(sleep=lambda s: None) == "dispatched next run"
    runs["workflow_runs"].append({"id": 501, "status": "queued"})   # a real successor exists
    calls.clear()
    assert "already queued" in chain.queue_next(sleep=lambda s: None) and calls == ["GET"]


def test_chain_dispatches_when_listing_fails(monkeypatch):
    from sitr import chain
    def req(method, path, data=None, **kw):
        if method == "GET":
            raise OSError("502")
        return None
    monkeypatch.setattr(chain.github, "request", req)
    assert chain.queue_next(sleep=lambda s: None) == "dispatched next run"


def test_pace_ignores_successor_and_uses_end_time():
    from sitr.pace import previous_run_end
    runs = [
        {"id": 501, "status": "pending", "updated_at": "2026-10-05T12:55:23Z"},    # successor: ignore
        {"id": 500, "status": "in_progress", "updated_at": "2026-10-05T12:55:21Z"},  # me
        {"id": 499, "status": "completed", "updated_at": "2026-10-05T12:55:18Z"},
        {"id": 498, "status": "completed", "updated_at": "2026-10-05T12:50:16Z"},
    ]
    assert previous_run_end(runs, 500) == datetime(2026, 10, 5, 12, 55, 18, tzinfo=timezone.utc)
