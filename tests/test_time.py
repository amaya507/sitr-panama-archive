"""The month-indexing trap and timezone handling."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from conftest import obj, raw
from sitr.parse import parse_endpoint
from sitr.timeutil import local_to_utc, parse_spanish_ts, trend_label, utc_to_local

FETCHED = datetime(2026, 9, 29, 10, 24, 1, tzinfo=timezone.utc)


def test_trend_month_is_zero_indexed_against_real_file():
    o = obj("sin_20260929T1024Z")
    last = o["trend"]["series"][-1]["time"]
    assert last["m"] == "8" and last["d"] == "29"            # raw says month 8 ...
    assert o["update"] == "29-septiembre-2026 5:23:53"        # ... and update says September
    assert trend_label(last) == datetime(2026, 9, 29, 5, 23)  # decoded as September


def test_parsed_trend_matches_update_field():
    rows, trend, drift, update_utc = parse_endpoint("sin", raw("sin_20260929T1024Z"), fetched_at_utc=FETCHED)
    assert not [d for d in drift if d.severity == "ERROR"]
    assert update_utc == datetime(2026, 9, 29, 10, 23, 53, tzinfo=timezone.utc)
    last = max(trend, key=lambda r: r["timestamp_utc"])
    first = min(trend, key=lambda r: r["timestamp_utc"])
    # last sample instant == update instant; label + seconds of update
    assert last["timestamp_local"] == datetime(2026, 9, 29, 5, 23, 53)
    assert last["minute_label_local"] == datetime(2026, 9, 29, 5, 23)
    assert last["timestamp_utc"] == update_utc
    assert first["timestamp_local"] == datetime(2026, 9, 28, 5, 23, 53)  # 24 h earlier, still September
    assert len(trend) == 1441 * 3
    # Values are mapped to names in order: first point is 1339/1351/1438 in the raw file.
    firsts = {r["entity"]: r["value"] for r in trend if r["timestamp_local"] == first["timestamp_local"]}
    assert firsts == {"Generación": 1339.0, "Demanda Real": 1351.0, "Demanda Pronosticada": 1438.0}


def test_one_based_month_is_detected_and_corrected():
    o = obj("sin_20260929T1024Z")
    for p in o["trend"]["series"]:
        p["time"]["m"] = str(int(p["time"]["m"]) + 1)  # simulate CND switching to 1-based months
    body = json.dumps(o, ensure_ascii=False).encode("utf-8")
    _, trend, drift, _ = parse_endpoint("sin", body, fetched_at_utc=FETCHED)
    assert any(d.kind == "month_indexing_changed" and d.severity == "ERROR" for d in drift)
    assert max(r["timestamp_local"] for r in trend) == datetime(2026, 9, 29, 5, 23, 53)


def test_unrecognisable_trend_time_is_loud():
    o = obj("sin_20260929T1024Z")
    for p in o["trend"]["series"]:
        p["time"]["y"] = "2025"
    _, _, drift, _ = parse_endpoint("sin", json.dumps(o).encode(), fetched_at_utc=FETCHED)
    assert any(d.kind == "label_mismatch" and d.severity == "ERROR" for d in drift)


def test_spanish_timestamps():
    assert parse_spanish_ts("29-septiembre-2026 5:17:08") == datetime(2026, 9, 29, 5, 17, 8)
    assert parse_spanish_ts("1-setiembre-2026 23:59:59") == datetime(2026, 9, 1, 23, 59, 59)
    assert parse_spanish_ts("30-agosto-2023 14:41") == datetime(2023, 8, 30, 14, 41)
    assert parse_spanish_ts("29-Septiembre-2026 05:17:08") == datetime(2026, 9, 29, 5, 17, 8)
    assert parse_spanish_ts("2026-09-29 05:17:08") is None
    assert parse_spanish_ts(None) is None


def test_fixed_utc_minus_5_regardless_of_date():
    # No DST in Panama: both January and July are UTC-5.
    for d in (datetime(2027, 1, 15, 12, 0), datetime(2027, 7, 15, 12, 0)):
        assert local_to_utc(d) == d.replace(tzinfo=timezone.utc).replace(hour=17)
        assert utc_to_local(local_to_utc(d)) == d


def test_runner_timezone_does_not_leak(monkeypatch):
    monkeypatch.setenv("TZ", "Asia/Kolkata")
    rows, trend, _, _ = parse_endpoint("gen", raw("gen_20260929T1024Z"), fetched_at_utc=FETCHED)
    r = rows[0]
    assert r["timestamp_utc"] == datetime(2026, 9, 29, 10, 24, 8, tzinfo=timezone.utc)
    assert r["timestamp_local"] == datetime(2026, 9, 29, 5, 24, 8)
